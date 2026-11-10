//! `wa-mcp service` : un seul process tient la session WhatsApp (bridge,
//! verrous, base, limite d'envoi) et la sert à plusieurs hôtes MCP à la fois,
//! en Streamable HTTP, le transport standard de MCP.
//!
//! - écoute sur `127.0.0.1` uniquement, chemin `/mcp` ;
//! - deux jetons aléatoires, stockés en `0600` dans `service/` du répertoire de
//!   données : `token` ouvre tous les outils, `token-read-only` seulement ceux
//!   de lecture. Chaque jeton a son propre gestionnaire de sessions : une
//!   session ouverte en lecture seule ne se rejoint pas avec l'autre jeton ;
//! - `Host` limité aux noms de boucle locale, et toute requête portant un
//!   `Origin` (un navigateur) refusée : pas de DNS rebinding ;
//! - un `WaServer` par session MCP, au-dessus de la même [`mcp::Instance`].
//!
//! `wa-mcp serve` reste ce que lancent les hôtes stdio : s'il trouve le service,
//! il lui relaie stdio (voir `relay`), sinon il tient la session lui-même.

use std::convert::Infallible;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::mcp::{Instance, WaServer};

/// Port d'écoute par défaut (`--port`, `WA_SERVICE_PORT`).
pub const DEFAULT_PORT: u16 = 47_311;
/// Chemin MCP servi.
pub const MCP_PATH: &str = "/mcp";
/// Contrôle de vie, jeton exigé : sert à `serve` et à `doctor`.
pub const HEALTH_PATH: &str = "/health";
/// Nom du service pour launchd et systemd.
#[cfg(target_os = "macos")]
const LAUNCHD_LABEL: &str = "io.github.edouard-claude.wa-mcp";
#[cfg(target_os = "linux")]
const SYSTEMD_UNIT: &str = "wa-mcp.service";

fn dir(data_dir: &Path) -> PathBuf {
    data_dir.join("service")
}

/// Port de la dernière écoute du service, à défaut le port par défaut.
pub fn port(data_dir: &Path) -> u16 {
    std::fs::read_to_string(dir(data_dir).join("port"))
        .ok()
        .and_then(|p| p.trim().parse().ok())
        .unwrap_or(DEFAULT_PORT)
}

pub fn url(port: u16) -> String {
    format!("http://127.0.0.1:{port}{MCP_PATH}")
}

/// Les deux jetons du service.
#[derive(Clone)]
pub struct Tokens {
    pub read_write: String,
    pub read_only: String,
}

impl Tokens {
    /// Jetons existants, ou créés au premier appel. Ils restent stables d'un
    /// démarrage à l'autre, pour que la configuration collée dans un hôte HTTP
    /// tienne ; pour en changer, supprimer les fichiers et redémarrer.
    pub fn load_or_create(data_dir: &Path) -> Result<Tokens> {
        let d = dir(data_dir);
        create_private_dir(&d)?;
        Ok(Tokens {
            read_write: token_file(&d.join("token"))?,
            read_only: token_file(&d.join("token-read-only"))?,
        })
    }

    /// Jetons existants, sans rien créer.
    pub fn load(data_dir: &Path) -> Option<Tokens> {
        let read = |name: &str| {
            std::fs::read_to_string(dir(data_dir).join(name))
                .ok()
                .map(|t| t.trim().to_owned())
                .filter(|t| !t.is_empty())
        };
        Some(Tokens {
            read_write: read("token")?,
            read_only: read("token-read-only")?,
        })
    }

    pub fn for_access(&self, read_only: bool) -> &str {
        if read_only {
            &self.read_only
        } else {
            &self.read_write
        }
    }

    /// Accès ouvert par l'en-tête `Authorization`, s'il porte un des jetons.
    fn access(&self, authorization: Option<&[u8]>) -> Option<Access> {
        let token = authorization?.strip_prefix(b"Bearer ")?;
        // Comparaison en temps constant : un jeton ne se devine pas octet par octet.
        if eq_constant_time(token, self.read_write.as_bytes()) {
            Some(Access::ReadWrite)
        } else if eq_constant_time(token, self.read_only.as_bytes()) {
            Some(Access::ReadOnly)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Access {
    ReadWrite,
    ReadOnly,
}

fn eq_constant_time(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn create_private_dir(d: &Path) -> Result<()> {
    std::fs::create_dir_all(d).with_context(|| format!("création de {}", d.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(d, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Lit un jeton, ou en écrit un nouveau (32 octets aléatoires en hexadécimal,
/// fichier `0600` créé d'emblée avec ces droits).
fn token_file(path: &Path) -> Result<String> {
    if let Ok(t) = std::fs::read_to_string(path) {
        let t = t.trim();
        if t.len() >= 32 {
            return Ok(t.to_owned());
        }
    }
    let mut raw = [0u8; 32];
    getrandom::fill(&mut raw).map_err(|e| anyhow::anyhow!("aléa indisponible : {e}"))?;
    let token: String = raw.iter().map(|b| format!("{b:02x}")).collect();
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.mode(0o600);
    }
    let mut f = opts
        .open(path)
        .with_context(|| format!("écriture de {}", path.display()))?;
    #[cfg(unix)]
    {
        // Le mode de `open` ne s'applique qu'à la création.
        use std::os::unix::fs::PermissionsExt as _;
        f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    writeln!(f, "{token}")?;
    Ok(token)
}

/// Écrit le port d'écoute, pour que `serve` et `doctor` trouvent le service.
pub fn write_port(data_dir: &Path, port: u16) -> Result<()> {
    let d = dir(data_dir);
    create_private_dir(&d)?;
    std::fs::write(d.join("port"), format!("{port}\n"))?;
    Ok(())
}

pub fn remove_port(data_dir: &Path) {
    let _ = std::fs::remove_file(dir(data_dir).join("port"));
}

type Body = BoxBody<Bytes, Infallible>;

fn plain(status: StatusCode, text: &str) -> Response<Body> {
    let mut r = Response::new(Full::new(Bytes::from(format!("{text}\n"))).boxed());
    *r.status_mut() = status;
    r.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    r
}

/// Sert MCP en Streamable HTTP sur `listener` jusqu'à `stop`.
pub async fn serve_http(
    instance: Instance,
    listener: TcpListener,
    tokens: Tokens,
    stop: CancellationToken,
) -> Result<()> {
    let config = StreamableHttpServerConfig::default()
        .with_allowed_hosts(["127.0.0.1", "localhost"])
        // Liste vide et validation imposée : tout `Origin` est refusé. Les hôtes
        // MCP n'en envoient pas ; une page web, toujours.
        .enforce_origin_validation()
        .with_cancellation_token(stop.child_token());
    let service = |read_only: bool| {
        let instance = instance.clone();
        StreamableHttpService::new(
            move || Ok(WaServer::new(&instance, read_only)),
            Arc::new(LocalSessionManager::default()),
            config.clone(),
        )
    };
    let (read_write, read_only) = (service(false), service(true));
    let mut connections = tokio::task::JoinSet::new();
    loop {
        let (stream, peer) = tokio::select! {
            () = stop.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok(a) => a,
                Err(e) => {
                    tracing::warn!(error = %e, "connexion refusée");
                    continue;
                }
            },
            // Les connexions terminées ne s'accumulent pas.
            Some(_) = connections.join_next() => continue,
        };
        let (rw, ro, tokens) = (read_write.clone(), read_only.clone(), tokens.clone());
        let handler = hyper::service::service_fn(move |req: Request<Incoming>| {
            let (rw, ro) = (rw.clone(), ro.clone());
            let access = tokens.access(
                req.headers()
                    .get(hyper::header::AUTHORIZATION)
                    .map(hyper::header::HeaderValue::as_bytes),
            );
            async move { Ok::<_, Infallible>(route(req, access, rw, ro).await) }
        });
        connections.spawn(async move {
            let io = hyper_util::rt::TokioIo::new(stream);
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, handler)
                .await
            {
                tracing::debug!(%peer, error = %e, "connexion HTTP");
            }
        });
    }
    // Les flux SSE ne se ferment pas d'eux-mêmes : on coupe les connexions.
    connections.shutdown().await;
    Ok(())
}

type Mcp = StreamableHttpService<WaServer, LocalSessionManager>;

async fn route(req: Request<Incoming>, access: Option<Access>, rw: Mcp, ro: Mcp) -> Response<Body> {
    use tower_service::Service as _;
    let path = req.uri().path();
    if path != MCP_PATH && path != HEALTH_PATH {
        return plain(StatusCode::NOT_FOUND, "introuvable : le point MCP est /mcp");
    }
    let Some(access) = access else {
        let mut r = plain(
            StatusCode::UNAUTHORIZED,
            "jeton manquant ou invalide : en-tête `Authorization: Bearer <jeton>`, voir `wa-mcp doctor`",
        );
        r.headers_mut().insert(
            hyper::header::WWW_AUTHENTICATE,
            hyper::header::HeaderValue::from_static("Bearer"),
        );
        return r;
    };
    if path == HEALTH_PATH {
        return plain(
            StatusCode::OK,
            &format!("wa-mcp {}", env!("CARGO_PKG_VERSION")),
        );
    }
    let mut mcp = match access {
        Access::ReadWrite => rw,
        Access::ReadOnly => ro,
    };
    match mcp.call(req).await {
        Ok(r) => r,
        Err(never) => match never {},
    }
}

/// Le service répond-il, avec ce jeton ? Requête HTTP minimale, sans client
/// complet : `serve` décide en moins d'une seconde entre relais et process.
pub async fn probe(port: u16, token: &str) -> bool {
    let attempt = async {
        let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
        let req = format!(
            "GET {HEALTH_PATH} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAuthorization: Bearer {token}\r\nConnection: close\r\n\r\n"
        );
        s.write_all(req.as_bytes()).await?;
        let mut head = [0u8; 64];
        let n = s.read(&mut head).await?;
        std::io::Result::Ok(head[..n].starts_with(b"HTTP/1.1 200"))
    };
    matches!(
        tokio::time::timeout(Duration::from_secs(1), attempt).await,
        Ok(Ok(true))
    )
}

/// Arrêt demandé : Ctrl-C, ou SIGTERM (launchd, systemd).
pub async fn terminated() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

// ------------------------------------------------------------- service système

/// Ce que lance le gestionnaire de services : chemins absolus, pour ne dépendre
/// ni du `PATH` ni du répertoire courant.
pub struct Launch {
    pub exe: PathBuf,
    pub data_dir: PathBuf,
    pub bridge: PathBuf,
    pub port: u16,
}

impl Launch {
    fn args(&self) -> Vec<String> {
        vec![
            self.exe.display().to_string(),
            "--data-dir".into(),
            self.data_dir.display().to_string(),
            "--bridge".into(),
            self.bridge.display().to_string(),
            "service".into(),
            "--port".into(),
            self.port.to_string(),
        ]
    }
}

fn home() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME absent")
}

/// Exécute une commande du gestionnaire de services, erreur lisible sinon.
fn run(cmd: &str, args: &[&str]) -> Result<std::process::Output> {
    std::process::Command::new(cmd)
        .args(args)
        .output()
        .with_context(|| format!("lancement de {cmd}"))
}

fn run_ok(cmd: &str, args: &[&str]) -> Result<()> {
    let out = run(cmd, args)?;
    if !out.status.success() {
        bail!(
            "`{cmd} {}` : {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(())
}

#[cfg(target_os = "macos")]
mod system {
    use super::*;

    fn uid() -> Result<String> {
        let out = run("id", &["-u"])?;
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
    }

    fn domain() -> Result<String> {
        Ok(format!("gui/{}", uid()?))
    }

    pub fn definition() -> Result<PathBuf> {
        Ok(home()?
            .join("Library/LaunchAgents")
            .join(format!("{LAUNCHD_LABEL}.plist")))
    }

    pub fn log_file() -> Option<PathBuf> {
        home()
            .ok()
            .map(|h| h.join("Library/Logs/wa-mcp/service.log"))
    }

    pub fn log_hint() -> String {
        log_file().map_or_else(
            || "~/Library/Logs/wa-mcp/service.log".into(),
            |p| p.display().to_string(),
        )
    }

    pub fn manager() -> &'static str {
        "launchd"
    }

    pub fn render(launch: &Launch, log: Option<&Path>) -> String {
        let log = log.map_or_else(String::new, |l| l.display().to_string());
        let args: String = launch
            .args()
            .iter()
            .map(|a| format!("\n    <string>{}</string>", xml_escape(a)))
            .collect();
        let log = xml_escape(&log);
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{LAUNCHD_LABEL}</string>
  <key>ProgramArguments</key>
  <array>{args}
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>10</integer>
  <key>StandardOutPath</key>
  <string>{log}</string>
  <key>StandardErrorPath</key>
  <string>{log}</string>
</dict>
</plist>
"#
        )
    }

    fn xml_escape(s: &str) -> String {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
    }

    pub fn start(def: &Path) -> Result<()> {
        let domain = domain()?;
        // Déjà chargé (réinstallation) : le décharger d'abord.
        let _ = run(
            "launchctl",
            &["bootout", &format!("{domain}/{LAUNCHD_LABEL}")],
        );
        run_ok(
            "launchctl",
            &["bootstrap", &domain, &def.display().to_string()],
        )
    }

    pub fn stop() -> Result<()> {
        let target = format!("{}/{LAUNCHD_LABEL}", domain()?);
        if loaded()? {
            run_ok("launchctl", &["bootout", &target])?;
        }
        Ok(())
    }

    pub fn loaded() -> Result<bool> {
        let target = format!("{}/{LAUNCHD_LABEL}", domain()?);
        Ok(run("launchctl", &["print", &target])?.status.success())
    }
}

#[cfg(target_os = "linux")]
mod system {
    use super::*;

    fn unit_dir() -> Result<PathBuf> {
        Ok(
            match std::env::var_os("XDG_CONFIG_HOME").filter(|x| !x.is_empty()) {
                Some(x) => PathBuf::from(x),
                None => home()?.join(".config"),
            }
            .join("systemd/user"),
        )
    }

    pub fn definition() -> Result<PathBuf> {
        Ok(unit_dir()?.join(SYSTEMD_UNIT))
    }

    /// Journal de systemd : pas de fichier à nous.
    pub fn log_file() -> Option<PathBuf> {
        None
    }

    pub fn log_hint() -> String {
        format!("journalctl --user -u {SYSTEMD_UNIT}")
    }

    pub fn manager() -> &'static str {
        "systemd --user"
    }

    pub fn render(launch: &Launch, _log: Option<&Path>) -> String {
        let exec: Vec<String> = launch.args().iter().map(|a| systemd_quote(a)).collect();
        format!(
            "[Unit]\n\
             Description=wa-mcp : session WhatsApp partagée entre hôtes MCP\n\
             \n\
             [Service]\n\
             ExecStart={}\n\
             Restart=on-failure\n\
             RestartSec=10\n\
             \n\
             [Install]\n\
             WantedBy=default.target\n",
            exec.join(" ")
        )
    }

    /// Argument d'`ExecStart` : entre guillemets, `\`, `"` échappés, `%` doublé.
    fn systemd_quote(s: &str) -> String {
        let escaped = s
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('%', "%%");
        format!("\"{escaped}\"")
    }

    pub fn start(_def: &Path) -> Result<()> {
        run_ok("systemctl", &["--user", "daemon-reload"])?;
        run_ok("systemctl", &["--user", "enable", SYSTEMD_UNIT])?;
        // `restart` plutôt que `start` : une réinstallation prend la nouvelle unité.
        run_ok("systemctl", &["--user", "restart", SYSTEMD_UNIT])
    }

    pub fn stop() -> Result<()> {
        if loaded()? {
            run_ok("systemctl", &["--user", "disable", "--now", SYSTEMD_UNIT])?;
        }
        Ok(())
    }

    pub fn loaded() -> Result<bool> {
        Ok(run("systemctl", &["--user", "is-enabled", SYSTEMD_UNIT])?
            .status
            .success())
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
mod system {
    use super::*;

    const UNSUPPORTED: &str = "service système pris en charge sur macOS (launchd) et Linux (systemd --user) : ailleurs, lancer `wa-mcp service` à la main";

    pub fn definition() -> Result<PathBuf> {
        bail!(UNSUPPORTED)
    }
    pub fn log_file() -> Option<PathBuf> {
        None
    }
    pub fn log_hint() -> String {
        "stderr de `wa-mcp service`".into()
    }
    pub fn manager() -> &'static str {
        "aucun"
    }
    pub fn render(_launch: &Launch, _log: &Path) -> String {
        String::new()
    }
    pub fn start(_def: &Path) -> Result<()> {
        bail!(UNSUPPORTED)
    }
    pub fn stop() -> Result<()> {
        bail!(UNSUPPORTED)
    }
    pub fn loaded() -> Result<bool> {
        Ok(false)
    }
}

/// `wa-mcp service install` : écrit la définition du service et le démarre.
pub fn install(launch: &Launch) -> Result<()> {
    let def = system::definition()?;
    let log = system::log_file();
    for parent in [Some(def.as_path()), log.as_deref()]
        .into_iter()
        .flatten()
        .filter_map(Path::parent)
    {
        std::fs::create_dir_all(parent)?;
    }
    // Jetons créés ici plutôt qu'au premier démarrage : `doctor` peut afficher
    // la configuration aussitôt.
    Tokens::load_or_create(&launch.data_dir)?;
    std::fs::write(&def, system::render(launch, log.as_deref()))
        .with_context(|| format!("écriture de {}", def.display()))?;
    system::start(&def)?;
    println!(
        "service   installé ({}) : {}",
        system::manager(),
        def.display()
    );
    println!("journal   {}", system::log_hint());
    println!("écoute    {}", url(launch.port));
    println!(
        "suite     redémarrer les hôtes MCP : leur `wa-mcp serve` passera par le service. `wa-mcp doctor` affiche la configuration des hôtes HTTP."
    );
    Ok(())
}

/// `wa-mcp service uninstall` : arrête le service et retire sa définition.
pub fn uninstall() -> Result<()> {
    let def = system::definition()?;
    system::stop()?;
    match std::fs::remove_file(&def) {
        Ok(()) => println!("service   désinstallé : {} retiré", def.display()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            println!("service   pas installé");
        }
        Err(e) => return Err(e).with_context(|| format!("suppression de {}", def.display())),
    }
    #[cfg(target_os = "linux")]
    let _ = run("systemctl", &["--user", "daemon-reload"]);
    println!("suite     les hôtes MCP reprennent la session en process à leur prochain démarrage");
    Ok(())
}

/// État du service : installé, en marche, port, journal.
pub struct Status {
    pub installed: bool,
    pub manager: &'static str,
    pub port: u16,
    pub running: bool,
    pub log: String,
}

pub async fn status(data_dir: &Path) -> Status {
    let port = port(data_dir);
    let running = match Tokens::load(data_dir) {
        Some(t) => probe(port, &t.read_write).await,
        None => false,
    };
    Status {
        installed: system::loaded().unwrap_or(false)
            || system::definition().is_ok_and(|d| d.is_file()),
        manager: system::manager(),
        port,
        running,
        log: system::log_hint(),
    }
}

impl Status {
    /// Une ligne pour `doctor` et `service status`.
    pub fn line(&self) -> String {
        let installed = if self.installed {
            format!("installé ({})", self.manager)
        } else {
            "pas installé".to_owned()
        };
        let running = if self.running {
            format!("en marche sur {}", url(self.port))
        } else {
            "arrêté : `serve` tient la session en process".to_owned()
        };
        format!("{running}, {installed}")
    }
}

/// Configuration à coller dans un hôte MCP qui parle HTTP.
pub fn host_config(port: u16, tokens: &Tokens) -> String {
    let url = url(port);
    format!(
        "URL       {url}\n\
         en-tête   Authorization: Bearer {rw}\n\
         lecture   Authorization: Bearer {ro}   (lecture seule)\n\
         exemple   claude mcp add --transport http whatsapp {url} --header \"Authorization: Bearer {rw}\"",
        rw = tokens.read_write,
        ro = tokens.read_only,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jetons() {
        let t = Tokens {
            read_write: "a".repeat(64),
            read_only: "b".repeat(64),
        };
        let bearer = |s: &str| format!("Bearer {s}");
        assert_eq!(
            t.access(Some(bearer(&"a".repeat(64)).as_bytes())),
            Some(Access::ReadWrite)
        );
        assert_eq!(
            t.access(Some(bearer(&"b".repeat(64)).as_bytes())),
            Some(Access::ReadOnly)
        );
        assert_eq!(t.access(Some(bearer(&"a".repeat(63)).as_bytes())), None);
        assert_eq!(t.access(Some("a".repeat(64).as_bytes())), None);
        assert_eq!(t.access(None), None);
    }

    #[test]
    fn jetons_stables_et_prives() {
        let dir = std::env::temp_dir().join(format!("wa-tokens-{}", std::process::id()));
        let first = Tokens::load_or_create(&dir).unwrap_or_else(|e| panic!("jetons : {e}"));
        let again = Tokens::load_or_create(&dir).unwrap_or_else(|e| panic!("jetons : {e}"));
        assert_eq!(first.read_write, again.read_write);
        assert_ne!(first.read_write, first.read_only);
        assert_eq!(first.read_write.len(), 64);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = |p: PathBuf| {
                std::fs::metadata(p)
                    .map(|m| m.permissions().mode() & 0o777)
                    .unwrap_or(0)
            };
            assert_eq!(mode(dir.join("service/token")), 0o600);
            assert_eq!(mode(dir.join("service/token-read-only")), 0o600);
            assert_eq!(mode(dir.join("service")), 0o700);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn definition_du_service() {
        let launch = Launch {
            exe: "/opt/wa mcp/wa-mcp".into(),
            data_dir: "/data/50%".into(),
            bridge: "/opt/wa mcp/wa-bridge".into(),
            port: 47_311,
        };
        let def = system::render(&launch, Some(Path::new("/tmp/service.log")));
        #[cfg(target_os = "macos")]
        assert!(
            def.contains("<string>/opt/wa mcp/wa-mcp</string>")
                && def.contains("<string>47311</string>")
        );
        #[cfg(target_os = "linux")]
        assert!(def.contains(r#"ExecStart="/opt/wa mcp/wa-mcp" "--data-dir" "/data/50%%""#));
        let _ = def;
    }
}

//! wa-mcp : serveur MCP WhatsApp adossé à whatsmeow.
//!
//! `serve` est le mode normal, lancé par un hôte MCP sur stdio : il démarre tous
//! les comptes, ingère en continu et répond aux requêtes MCP. Si `service`
//! tourne (session partagée entre plusieurs hôtes, en Streamable HTTP), `serve`
//! ne fait que lui relayer stdio. `pair`, `run` et `send` sont des commandes de
//! terminal (appairage, observation, essai d'envoi).

#![forbid(unsafe_code)]

mod bridge;
mod compose;
mod dates;
mod domain;
mod ingest;
mod lock;
mod mcp;
mod parse;
mod query;
mod relay;
mod service;
mod store;
#[cfg(test)]
mod tests_service;

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use clap::{Parser, Subcommand};
use tokio::task::JoinSet;
use wa_proto::wabridge::v1 as pb;

use crate::bridge::{BridgeEvent, BridgeHandle};
use crate::domain::AccountAlias;
use crate::ingest::Ingest;
use crate::store::Store;
use wa_proto::wabridge::v1::connection_state::State;

#[derive(Parser)]
#[command(version, about = "Serveur MCP WhatsApp")]
struct Cli {
    /// Racine des données : sessions, bases, médias. Par défaut
    /// `~/Library/Application Support/wa-mcp` (macOS) ou `~/.local/share/wa-mcp`.
    #[arg(long, env = "WA_DATA_DIR")]
    data_dir: Option<PathBuf>,
    /// Binaire wa-bridge. Par défaut, à côté de wa-mcp.
    #[arg(long, env = "WA_BRIDGE")]
    bridge: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Vérifie l'installation : bridge, répertoire de données, comptes, verrous.
    Doctor,
    /// Serveur MCP sur stdio (mode utilisé par l'hôte MCP). stdout appartient au protocole.
    /// Relaie vers `wa-mcp service` s'il tourne, sinon tient la session lui-même.
    Serve {
        /// N'expose que les outils de lecture : aucun envoi possible.
        #[arg(long, env = "WA_READ_ONLY")]
        read_only: bool,
    },
    /// Session WhatsApp partagée entre plusieurs hôtes MCP, servie en
    /// Streamable HTTP sur 127.0.0.1. Sans sous-commande, tourne au premier plan.
    Service {
        #[command(subcommand)]
        action: Option<ServiceAction>,
        /// Port d'écoute, sur 127.0.0.1 seulement.
        #[arg(long, env = "WA_SERVICE_PORT", default_value_t = service::DEFAULT_PORT, global = true)]
        port: u16,
    },
    /// Appaire un compte, par QR ou par code (`--phone`).
    Pair {
        account: AccountAlias,
        /// Numéro international sans `+` (ex. 33612345678) : code à 8 caractères au lieu du QR.
        #[arg(long)]
        phone: Option<String>,
    },
    /// Démarre les comptes et affiche les messages reçus jusqu'à Ctrl-C.
    Run {
        /// Comptes à démarrer. Par défaut, tous ceux présents dans le répertoire de données.
        accounts: Vec<AccountAlias>,
    },
    /// Envoie un texte, puis s'arrête.
    Send {
        account: AccountAlias,
        /// JID complet ou numéro international sans `+`.
        to: String,
        text: String,
    },
}

#[derive(Subcommand, Clone, Copy)]
enum ServiceAction {
    /// Installe et démarre le service (launchd sur macOS, systemd --user sur Linux).
    Install,
    /// Arrête le service et retire sa définition.
    Uninstall,
    /// Dit si le service est installé et s'il répond.
    Status,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        // Pas de codes couleur quand stderr part dans le journal d'un hôte MCP.
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .init();

    let cli = Cli::parse();
    let data_dir = match cli.data_dir {
        Some(d) => d,
        None => default_data_dir()?,
    };
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("création de {}", data_dir.display()))?;
    let data_dir = data_dir.canonicalize()?;
    let bin = match cli.bridge {
        Some(b) => b,
        None => std::env::current_exe()?
            .parent()
            .context("répertoire de wa-mcp introuvable")?
            .join("wa-bridge"),
    };
    match cli.cmd {
        Cmd::Doctor => return doctor(&data_dir, &bin).await,
        Cmd::Serve { read_only } => {
            let result = serve(&data_dir, &bin, read_only).await;
            // La lecture de stdin occupe un thread bloquant que l'arrêt du runtime
            // attendrait jusqu'à la prochaine ligne de l'hôte : sortir tout de suite.
            std::process::exit(match result {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("Error: {e:#}");
                    1
                }
            });
        }
        Cmd::Service { action, port } => {
            return match action {
                None => run_service(&data_dir, &bin, port).await,
                Some(ServiceAction::Install) => service::install(&service::Launch {
                    exe: std::env::current_exe()?,
                    data_dir: data_dir.clone(),
                    bridge: bin.canonicalize().with_context(|| {
                        format!("bridge {} introuvable : passer --bridge", bin.display())
                    })?,
                    port,
                }),
                Some(ServiceAction::Uninstall) => service::uninstall(),
                Some(ServiceAction::Status) => {
                    let st = service::status(&data_dir).await;
                    println!("service   {}", st.line());
                    println!("journal   {}", st.log);
                    Ok(())
                }
            };
        }
        _ => {}
    }
    // Une seule instance par compte : deux clients sur la même session se
    // déconnectent mutuellement (StreamReplaced) et l'un abandonne pour de bon.
    let wanted = match &cli.cmd {
        Cmd::Run { accounts } if accounts.is_empty() => discover(&data_dir)?,
        Cmd::Run { accounts } => accounts.clone(),
        Cmd::Pair { account, .. } | Cmd::Send { account, .. } => vec![account.clone()],
        Cmd::Serve { .. } | Cmd::Service { .. } | Cmd::Doctor => unreachable!("traités plus haut"),
    };
    let _locks = lock_accounts(&data_dir, &wanted)?;

    let bridge::Bridge {
        handle,
        mut events,
        task,
    } = bridge::spawn(&bin, &data_dir)?;
    let (store, store_thread) = Store::open(data_dir.clone());
    let mut tasks = JoinSet::new();

    let (target, mode) = match cli.cmd {
        Cmd::Pair { account, phone } => {
            let h = handle.clone();
            let a = account.clone();
            tasks.spawn(async move {
                h.start_account(&a, phone.as_deref())
                    .await
                    .map_err(anyhow::Error::from)
                    .map(|()| None)
            });
            (Some(account), Mode::Pair)
        }
        Cmd::Run { accounts } => {
            let accounts = if accounts.is_empty() {
                discover(&data_dir)?
            } else {
                accounts
            };
            if accounts.is_empty() {
                bail!(
                    "aucun compte dans {} : commencer par `wa-mcp pair <alias>`",
                    data_dir.display()
                );
            }
            for a in accounts {
                let h = handle.clone();
                tasks.spawn(async move {
                    h.start_account(&a, None)
                        .await
                        .map_err(anyhow::Error::from)
                        .map(|()| None)
                });
            }
            (None, Mode::Run)
        }
        Cmd::Serve { .. } | Cmd::Service { .. } | Cmd::Doctor => {
            unreachable!("traités plus haut")
        }
        Cmd::Send { account, to, text } => {
            let h = handle.clone();
            let a = account.clone();
            tasks.spawn(async move {
                h.start_account(&a, None)
                    .await
                    .map_err(anyhow::Error::from)
                    .map(|()| None)
            });
            (Some(account.clone()), Mode::Send { account, to, text })
        }
    };

    let (ingest, refresher) = Ingest::new(
        handle.clone(),
        store.clone(),
        tokio::sync::broadcast::channel(1).0,
    );
    let result = event_loop(
        &handle,
        &ingest,
        &mut events,
        &mut tasks,
        target.as_ref(),
        mode,
        true,
    )
    .await;

    // Arrêt : plus de poignée, stdin du bridge se ferme, il déconnecte et sort.
    // Les Acks en attente tombent : WhatsApp redélivrera, rien n'est perdu.
    tasks.shutdown().await;
    refresher.abort();
    drop(ingest);
    shutdown(handle, store, task, store_thread, result).await
}

/// `serve` : relais vers le service s'il répond, session en process sinon.
async fn serve(data_dir: &Path, bin: &Path, read_only: bool) -> Result<()> {
    if let Some((url, token)) = reachable_service(data_dir, read_only).await {
        return relay_stdio(&url, &token, read_only).await;
    }
    let locks = match lock_accounts(data_dir, &discover(data_dir)?) {
        Ok(l) => l,
        // Le service démarre peut-être (redémarrage, ouverture de session) :
        // il tient déjà les verrous mais n'écoute pas encore.
        Err(busy) if service::Tokens::load(data_dir).is_some() => {
            match wait_for_service(data_dir, read_only, Duration::from_secs(10)).await {
                Some((url, token)) => return relay_stdio(&url, &token, read_only).await,
                None => return Err(busy),
            }
        }
        Err(e) => return Err(e),
    };
    let running = Running::start(data_dir, bin, locks).await?;
    let server = mcp::WaServer::new(&running.instance, read_only);
    use rmcp::ServiceExt as _;
    let mut running = running;
    let session = server
        .serve(rmcp::transport::stdio())
        .await
        .context("transport stdio")?;
    let result = tokio::select! {
        quit = session.waiting() => {
            tracing::info!(?quit, "hôte MCP parti, arrêt");
            Ok(())
        }
        pumped = &mut running.pump => pumped_result(pumped),
    };
    running.stop(result).await
}

async fn relay_stdio(url: &str, token: &str, read_only: bool) -> Result<()> {
    use rmcp::transport::IntoTransport;
    tracing::info!(%url, read_only, "service wa-mcp en marche : relais stdio");
    let down = IntoTransport::<rmcp::RoleServer, _, _>::into_transport(rmcp::transport::stdio());
    relay::run(down, relay::upstream(url, token)).await
}

/// URL et jeton du service s'il répond, pour l'accès demandé.
async fn reachable_service(data_dir: &Path, read_only: bool) -> Option<(String, String)> {
    let tokens = service::Tokens::load(data_dir)?;
    let port = service::port(data_dir);
    // Le contrôle se fait toujours avec le jeton complet : il prouve que le
    // service est le nôtre, l'accès réel dépend du jeton relayé.
    service::probe(port, &tokens.read_write)
        .await
        .then(|| (service::url(port), tokens.for_access(read_only).to_owned()))
}

async fn wait_for_service(
    data_dir: &Path,
    read_only: bool,
    within: Duration,
) -> Option<(String, String)> {
    let deadline = tokio::time::Instant::now() + within;
    while tokio::time::Instant::now() < deadline {
        if let Some(found) = reachable_service(data_dir, read_only).await {
            return Some(found);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    None
}

/// `service` au premier plan : verrous, bridge, base, puis MCP en HTTP jusqu'à
/// SIGTERM ou Ctrl-C.
async fn run_service(data_dir: &Path, bin: &Path, port: u16) -> Result<()> {
    let tokens = service::Tokens::load_or_create(data_dir)?;
    // Ne prend que la boucle locale : jamais d'écoute au-delà de la machine.
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .with_context(|| format!("écoute sur 127.0.0.1:{port} (`--port` pour en changer)"))?;
    let port = listener.local_addr()?.port();
    let locks = lock_until_free(data_dir).await?;
    let mut running = Running::start(data_dir, bin, locks).await?;
    service::write_port(data_dir, port)?;
    tracing::info!(url = %service::url(port), "service wa-mcp en marche");
    let stop = tokio_util::sync::CancellationToken::new();
    let mut http = tokio::spawn(service::serve_http(
        running.instance.clone(),
        listener,
        tokens,
        stop.clone(),
    ));
    let result = tokio::select! {
        () = service::terminated() => {
            tracing::info!("arrêt demandé");
            Ok(())
        }
        pumped = &mut running.pump => pumped_result(pumped),
        served = &mut http => match served {
            Ok(r) => r,
            Err(e) => Err(anyhow::anyhow!("serveur HTTP : {e}")),
        },
    };
    service::remove_port(data_dir);
    stop.cancel();
    // Les sessions fermées lâchent leurs poignées vers le bridge et la base.
    let _ = tokio::time::timeout(Duration::from_secs(5), &mut http).await;
    http.abort();
    running.stop(result).await
}

/// Verrous de tous les comptes. Un hôte qui tient encore la session en process
/// (installé avant le service) la garde jusqu'à son redémarrage : le service
/// attend son tour plutôt que de sortir en boucle sous launchd ou systemd.
async fn lock_until_free(data_dir: &Path) -> Result<Vec<std::fs::File>> {
    let mut warned = false;
    loop {
        match lock_accounts(data_dir, &discover(data_dir)?) {
            Ok(locks) => return Ok(locks),
            Err(e)
                if e.downcast_ref::<lock::LockError>()
                    .is_some_and(|e| matches!(e, lock::LockError::Busy(_))) =>
            {
                if !warned {
                    tracing::warn!(
                        "{e} ; en attente (redémarrer l'hôte MCP concerné pour qu'il passe par le service)"
                    );
                    warned = true;
                }
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_secs(5)) => {}
                    () = service::terminated() => bail!("arrêt demandé avant la prise des verrous"),
                }
            }
            Err(e) => return Err(e),
        }
    }
}

fn pumped_result(pumped: std::result::Result<Result<()>, tokio::task::JoinError>) -> Result<()> {
    match pumped {
        Ok(r) => r,
        Err(e) => Err(anyhow::anyhow!("boucle d'événements : {e}")),
    }
}

/// Une instance en marche : verrous, bridge, base, ingestion. Servie par une
/// session stdio (`serve`) ou par autant de sessions HTTP que d'hôtes (`service`).
struct Running {
    instance: mcp::Instance,
    pump: tokio::task::JoinHandle<Result<()>>,
    refresher: tokio::task::JoinHandle<()>,
    handle: BridgeHandle,
    store: Store,
    task: tokio::task::JoinHandle<Result<std::process::ExitStatus, bridge::BridgeError>>,
    store_thread: std::thread::JoinHandle<()>,
    _locks: Vec<std::fs::File>,
}

impl Running {
    async fn start(data_dir: &Path, bin: &Path, locks: Vec<std::fs::File>) -> Result<Running> {
        let bridge::Bridge {
            handle,
            mut events,
            task,
        } = bridge::spawn(bin, data_dir)?;
        let (store, store_thread) = Store::open(data_dir.to_path_buf());
        let accounts = discover(data_dir)?;
        // Nouvelles de chaque discussion, pour les abonnements MCP.
        let (updates, _) = tokio::sync::broadcast::channel(1024);
        let instance = mcp::Instance::new(
            data_dir.to_path_buf(),
            handle.clone(),
            store.clone(),
            accounts.clone(),
            updates.clone(),
        );
        instance.open_databases().await?;
        let mut tasks = JoinSet::new();
        for a in accounts {
            let h = handle.clone();
            tasks.spawn(async move {
                h.start_account(&a, None)
                    .await
                    .map_err(anyhow::Error::from)
                    .map(|()| None)
            });
        }
        let (ingest, refresher) = Ingest::new(handle.clone(), store.clone(), updates);
        let pump_handle = handle.clone();
        let pump = tokio::spawn(async move {
            event_loop(
                &pump_handle,
                &ingest,
                &mut events,
                &mut tasks,
                None,
                Mode::Run,
                false,
            )
            .await
        });
        Ok(Running {
            instance,
            pump,
            refresher,
            handle,
            store,
            task,
            store_thread,
            _locks: locks,
        })
    }

    async fn stop(self, result: Result<()>) -> Result<()> {
        self.pump.abort();
        self.refresher.abort();
        drop(self.instance);
        let _ = self.pump.await;
        let result = shutdown(
            self.handle,
            self.store,
            self.task,
            self.store_thread,
            result,
        )
        .await;
        drop(self._locks);
        result
    }
}

/// Arrêt : plus de poignée, stdin du bridge se ferme, il déconnecte et sort.
/// Les Acks en attente tombent : WhatsApp redélivrera, rien n'est perdu.
async fn shutdown(
    handle: BridgeHandle,
    store: Store,
    task: tokio::task::JoinHandle<Result<std::process::ExitStatus, bridge::BridgeError>>,
    store_thread: std::thread::JoinHandle<()>,
    result: Result<()>,
) -> Result<()> {
    drop(handle);
    drop(store);
    match tokio::time::timeout(Duration::from_secs(10), task).await {
        Ok(Ok(Ok(_))) => {}
        Ok(Ok(Err(e))) => tracing::warn!(error = %e, "arrêt du bridge"),
        Ok(Err(e)) => tracing::warn!(error = %e, "tâche du bridge"),
        Err(_) => tracing::warn!("le bridge ne s'est pas arrêté à temps, process tué"),
    }
    let _ = store_thread.join();
    result
}

enum Mode {
    Pair,
    Run,
    Send {
        account: AccountAlias,
        to: String,
        text: String,
    },
}

/// Boucle unique de consommation des événements. Elle n'attend jamais de réponse
/// du bridge elle-même : les requêtes vivent dans `tasks` ou dans le
/// rafraîchisseur (voir `bridge.rs`).
async fn event_loop(
    handle: &BridgeHandle,
    ingest: &Ingest,
    events: &mut tokio::sync::mpsc::Receiver<BridgeEvent>,
    tasks: &mut JoinSet<Result<Option<pb::SendResult>>>,
    target: Option<&AccountAlias>,
    mut mode: Mode,
    // Faux en mode `serve` : stdout appartient au protocole MCP.
    verbose: bool,
) -> Result<()> {
    let is_target = |acc: &str| target.is_some_and(|t| t.as_str() == acc);
    loop {
        tokio::select! {
            evt = events.recv() => {
                let Some(evt) = evt else { bail!("le bridge s'est arrêté") };
                match evt {
                    BridgeEvent::Message(m) => {
                        if let Some(l) = ingest.message(m).await? {
                            let arrow = if l.from_me { "moi -> " } else { "" };
                            if verbose {
                                println!("[{}] {} {arrow}{} {}", l.account, l.chat, l.who, l.what);
                            } else {
                                tracing::debug!(account = %l.account, chat = %l.chat, "message");
                            }
                        }
                    }
                    BridgeEvent::History(h) => {
                        let (account, kind, progress) = (h.account.clone(), h.sync_type.clone(), h.progress);
                        let n = ingest.history(h).await?;
                        eprintln!("[{account}] historique {kind} : {n} messages ({progress} %)");
                    }
                    BridgeEvent::Receipt(r) => ingest.receipt(r).await?,
                    BridgeEvent::GroupChanged(g) => ingest.group_changed(g).await?,
                    BridgeEvent::ChatSettings(c) => ingest.chat_settings(c).await?,
                    BridgeEvent::Qr(q) => print_qr(&q.account, &q.code),
                    BridgeEvent::PairCode(p) => {
                        eprintln!("[{}] code d'appairage : {}", p.account, p.code);
                        eprintln!("  WhatsApp > Appareils connectés > Connecter un appareil > Connecter avec le numéro de téléphone");
                    }
                    BridgeEvent::PairSuccess(p) => eprintln!("[{}] appairé : {}", p.account, p.jid),
                    BridgeEvent::Connection(c) => {
                        let (account, state, detail) = (c.account.clone(), c.state(), c.detail.clone());
                        let jid = c.jid.clone();
                        ingest.connection(c).await?;
                        match state {
                            State::Connected => {
                                eprintln!("[{account}] connecté ({jid})");
                                if is_target(&account) {
                                    match std::mem::replace(&mut mode, Mode::Run) {
                                        // On reste connecté : l'historique arrive juste après l'appairage.
                                        Mode::Pair => eprintln!("[{account}] historique en cours de réception, Ctrl-C pour arrêter"),
                                        Mode::Send { account, to, text } => {
                                            let h = handle.clone();
                                            let msg = compose::text(&text, None, Vec::new());
                                            tasks.spawn(async move { Ok(Some(h.send_message(&account, &to, &msg, None).await?)) });
                                        }
                                        Mode::Run => {}
                                    }
                                }
                            }
                            State::LoggedOut | State::StreamReplaced | State::ClientOutdated => {
                                eprintln!("[{account}] {} : {detail}", state.as_str_name());
                                if is_target(&account) {
                                    bail!("compte {account} arrêté : {} {detail}", state.as_str_name());
                                }
                            }
                            _ => eprintln!("[{account}] {} {detail}", state.as_str_name()),
                        }
                    }
                    BridgeEvent::Reply(_) => {}
                }
            }
            Some(done) = tasks.join_next() => {
                if let Some(sent) = done.context("tâche interrompue")?? {
                    println!("envoyé : id={} ts={}", sent.message_id, sent.timestamp_ms);
                    return Ok(());
                }
            }
            _ = tokio::signal::ctrl_c() => return Ok(()),
        }
    }
}

fn print_qr(account: &str, code: &str) {
    use qrcode::render::unicode::Dense1x2;
    match qrcode::QrCode::new(code) {
        Ok(qr) => {
            let img = qr.render::<Dense1x2>().quiet_zone(true).build();
            eprintln!("[{account}] scanner ce QR (WhatsApp > Appareils connectés) :\n{img}");
        }
        Err(e) => eprintln!("[{account}] QR non affichable ({e}) : {code}"),
    }
}

/// Répertoire de données par défaut, selon les conventions de la plateforme.
fn default_data_dir() -> Result<PathBuf> {
    let home = std::env::var_os("HOME").context("HOME absent : préciser --data-dir")?;
    let home = PathBuf::from(home);
    Ok(if cfg!(target_os = "macos") {
        home.join("Library/Application Support/wa-mcp")
    } else if let Some(xdg) = std::env::var_os("XDG_DATA_HOME").filter(|x| !x.is_empty()) {
        PathBuf::from(xdg).join("wa-mcp")
    } else {
        home.join(".local/share/wa-mcp")
    })
}

/// Diagnostic lisible par une personne comme par un agent : chaque ligne dit ce
/// qui va, ou quoi faire.
async fn doctor(data_dir: &Path, bin: &Path) -> Result<()> {
    println!("wa-mcp {}", env!("CARGO_PKG_VERSION"));
    println!("données   {}", data_dir.display());
    let bridge_ok = bin.is_file();
    println!(
        "bridge    {} {}",
        bin.display(),
        if bridge_ok {
            "ok"
        } else {
            "ABSENT : le placer à côté de wa-mcp ou passer --bridge"
        }
    );
    let st = service::status(data_dir).await;
    println!("service   {}", st.line());
    if st.running
        && let Some(tokens) = service::Tokens::load(data_dir)
    {
        println!("hôtes HTTP (Streamable HTTP) :");
        for line in service::host_config(st.port, &tokens).lines() {
            println!("  {line}");
        }
    } else if !st.installed {
        println!(
            "          plusieurs hôtes sur le même compte ? `wa-mcp service install`, puis les redémarrer"
        );
    }
    let accounts = discover(data_dir)?;
    if accounts.is_empty() {
        println!("comptes   aucun : `wa-mcp pair <alias> --phone <numéro international>`");
    }
    for a in &accounts {
        let lock = match lock::lock(data_dir, a) {
            Ok(_) => "libre",
            Err(lock::LockError::Busy(_)) if st.running => "tenu par le service",
            Err(lock::LockError::Busy(_)) => "utilisé par une instance en cours",
            Err(lock::LockError::Io(_)) => "verrou illisible",
        };
        let db = data_dir.join("accounts").join(a.as_str()).join("store.db");
        let summary =
            rusqlite::Connection::open_with_flags(&db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .and_then(|c| {
                    c.query_row(
                        "SELECT (SELECT count(*) FROM messages), (SELECT count(*) FROM chats),
                            (SELECT value FROM meta WHERE key = 'connection.state')",
                        [],
                        |r| {
                            Ok((
                                r.get::<_, i64>(0)?,
                                r.get::<_, i64>(1)?,
                                r.get::<_, Option<String>>(2)?,
                            ))
                        },
                    )
                });
        match summary {
            Ok((m, c, state)) => println!(
                "compte    {a} : {m} messages, {c} discussions, dernier état {}, {lock}",
                state.as_deref().unwrap_or("inconnu")
            ),
            Err(_) => println!("compte    {a} : base pas encore créée, {lock}"),
        }
    }
    if !bridge_ok {
        bail!("installation incomplète");
    }
    Ok(())
}

fn lock_accounts(data_dir: &Path, accounts: &[AccountAlias]) -> Result<Vec<std::fs::File>> {
    accounts
        .iter()
        .map(|a| lock::lock(data_dir, a).map_err(anyhow::Error::from))
        .collect()
}

/// Comptes présents : `accounts/<alias>/session.db`.
fn discover(data_dir: &Path) -> Result<Vec<AccountAlias>> {
    let dir = data_dir.join("accounts");
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for e in entries {
        let e = e?;
        if e.path().join("session.db").is_file()
            && let Some(Ok(alias)) = e.file_name().to_str().map(str::parse::<AccountAlias>)
        {
            out.push(alias);
        }
    }
    out.sort();
    Ok(out)
}

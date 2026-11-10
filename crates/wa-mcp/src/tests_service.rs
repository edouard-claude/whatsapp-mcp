//! Bout en bout de `wa-mcp service` sur un compte factice : pas de bridge (les
//! requêtes vers WhatsApp échouent), une vraie base, un vrai serveur HTTP.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;

use rmcp::model::CallToolRequestParams;
use rmcp::service::{RoleClient, RunningService};
use rmcp::transport::IntoTransport;
use rmcp::{RoleServer, ServiceExt as _};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio_util::sync::CancellationToken;

use crate::bridge::BridgeHandle;
use crate::domain::AccountAlias;
use crate::mcp::Instance;
use crate::service::{self, Tokens};
use crate::store::{Op, Store};

struct Fixture {
    dir: PathBuf,
    port: u16,
    tokens: Tokens,
    stop: CancellationToken,
    http: tokio::task::JoinHandle<anyhow::Result<()>>,
    _store_thread: std::thread::JoinHandle<()>,
}

impl Fixture {
    async fn start(name: &str) -> Fixture {
        let dir = std::env::temp_dir().join(format!("wa-service-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let account: AccountAlias = "test".parse().unwrap();
        let (store, store_thread) = Store::open(dir.clone());
        store.apply(account.clone(), Op::Open).await.unwrap();
        let instance = Instance::new(
            dir.clone(),
            BridgeHandle::detached(),
            store,
            vec![account],
            tokio::sync::broadcast::channel(16).0,
        );
        let tokens = Tokens::load_or_create(&dir).unwrap();
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        service::write_port(&dir, port).unwrap();
        let stop = CancellationToken::new();
        let http = tokio::spawn(service::serve_http(
            instance,
            listener,
            tokens.clone(),
            stop.clone(),
        ));
        Fixture {
            dir,
            port,
            tokens,
            stop,
            http,
            _store_thread: store_thread,
        }
    }

    async fn client(&self, token: &str) -> RunningService<RoleClient, ()> {
        ().serve(crate::relay::upstream(&service::url(self.port), token))
            .await
            .unwrap()
    }

    async fn stop(self) {
        self.stop.cancel();
        let _ = self.http.await;
        service::remove_port(&self.dir);
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn tool_names(c: &RunningService<RoleClient, ()>) -> Vec<String> {
    c.list_all_tools()
        .await
        .unwrap()
        .into_iter()
        .map(|t| t.name.to_string())
        .collect()
}

/// Statut HTTP d'un `initialize` envoyé à la main, avec ces en-têtes en plus.
async fn initialize_status(port: u16, extra: &str) -> u16 {
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}"#;
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let req = format!(
        "POST /mcp HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n{extra}\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut head = [0u8; 12];
    s.read_exact(&mut head).await.unwrap();
    std::str::from_utf8(&head[9..12]).unwrap().parse().unwrap()
}

#[tokio::test]
async fn deux_sessions_simultanees_sur_le_meme_compte() {
    let f = Fixture::start("deux").await;
    let (a, b) = tokio::join!(
        f.client(&f.tokens.read_write),
        f.client(&f.tokens.read_write)
    );
    let (ta, tb) = tokio::join!(tool_names(&a), tool_names(&b));
    assert_eq!(ta, tb);
    assert!(ta.iter().any(|t| t == "send_message"));
    // Les deux sessions lisent la même base, en même temps.
    let call = || CallToolRequestParams::new("list_accounts");
    let (ra, rb) = tokio::join!(a.call_tool(call()), b.call_tool(call()));
    for r in [ra.unwrap(), rb.unwrap()] {
        assert_ne!(r.is_error, Some(true), "{r:?}");
        let text = serde_json::to_string(&r.structured_content).unwrap();
        assert!(text.contains("\"test\""), "{text}");
    }
    // Le départ d'une session laisse l'autre servie.
    a.cancel().await.unwrap();
    assert!(b.call_tool(call()).await.is_ok());
    b.cancel().await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn jeton_et_origine_controles() {
    let f = Fixture::start("refus").await;
    let rw = format!("Authorization: Bearer {}\r\n", f.tokens.read_write);
    assert_eq!(initialize_status(f.port, &rw).await, 200);
    assert_eq!(initialize_status(f.port, "").await, 401);
    assert_eq!(
        initialize_status(f.port, "Authorization: Bearer 0000\r\n").await,
        401
    );
    // Une page web (DNS rebinding ou non) porte toujours un Origin.
    let from_browser = format!("{rw}Origin: http://evil.example\r\n");
    assert_eq!(initialize_status(f.port, &from_browser).await, 403);
    let localhost_page = format!("{rw}Origin: http://127.0.0.1:{}\r\n", f.port);
    assert_eq!(initialize_status(f.port, &localhost_page).await, 403);
    f.stop().await;
}

#[tokio::test]
async fn host_etranger_refuse() {
    let f = Fixture::start("host").await;
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}"#;
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", f.port))
        .await
        .unwrap();
    let req = format!(
        "POST /mcp HTTP/1.1\r\nHost: evil.example\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        f.tokens.read_write,
        body.len()
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut head = [0u8; 12];
    s.read_exact(&mut head).await.unwrap();
    assert_ne!(&head[9..12], b"200");
    f.stop().await;
}

#[tokio::test]
async fn jeton_lecture_seule_limite_aux_outils_de_lecture() {
    let f = Fixture::start("lecture").await;
    let ro = f.client(&f.tokens.read_only).await;
    let rw = f.client(&f.tokens.read_write).await;
    let (tro, trw) = (tool_names(&ro).await, tool_names(&rw).await);
    assert!(tro.len() < trw.len());
    for write in [
        "send_message",
        "delete_message",
        "session_pair",
        "session_logout",
    ] {
        assert!(
            !tro.iter().any(|t| t == write),
            "{write} exposé en lecture seule"
        );
        assert!(trw.iter().any(|t| t == write), "{write} absent en écriture");
    }
    let refused = ro
        .call_tool(CallToolRequestParams::new("send_message"))
        .await;
    assert!(
        refused.is_err() || refused.as_ref().is_ok_and(|r| r.is_error == Some(true)),
        "{refused:?}"
    );
    ro.cancel().await.unwrap();
    rw.cancel().await.unwrap();
    f.stop().await;
}

/// `serve` relaie vers le service quand il répond, et repasse en process
/// quand il est arrêté.
#[tokio::test]
async fn relais_serve_puis_repli() {
    let f = Fixture::start("relais").await;
    let (url, token) = crate::reachable_service(&f.dir, true)
        .await
        .expect("service joignable");
    assert_eq!(token, f.tokens.read_only);

    // Ce que fait `serve` : stdio de l'hôte <-> service. Ici, stdio est un tuyau.
    let (host_end, relay_end) = tokio::io::duplex(1 << 16);
    let down = IntoTransport::<RoleServer, _, _>::into_transport(tokio::io::split(relay_end));
    let relay = tokio::spawn(crate::relay::run(
        down,
        crate::relay::upstream(&url, &token),
    ));
    let host = ().serve(tokio::io::split(host_end)).await.unwrap();
    let tools = tool_names(&host).await;
    assert!(
        !tools.iter().any(|t| t == "send_message"),
        "relais en lecture seule"
    );
    let r = host
        .call_tool(CallToolRequestParams::new("list_accounts"))
        .await
        .unwrap();
    assert_ne!(r.is_error, Some(true), "{r:?}");
    // L'hôte part : le relais se termine proprement.
    host.cancel().await.unwrap();
    let ended = tokio::time::timeout(std::time::Duration::from_secs(5), relay)
        .await
        .expect("relais terminé")
        .unwrap();
    assert!(ended.is_ok(), "{ended:?}");

    let dir = f.dir.clone();
    let port = f.port;
    f.stop.cancel();
    let _ = f.http.await;
    // Service arrêté (port retiré ou non) : `serve` tiendra la session lui-même.
    assert!(!service::probe(port, &f.tokens.read_write).await);
    assert!(crate::reachable_service(&dir, false).await.is_none());
    let _ = std::fs::remove_dir_all(&dir);
}

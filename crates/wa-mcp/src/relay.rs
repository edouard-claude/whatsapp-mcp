//! Relais stdio ↔ Streamable HTTP : ce que fait `wa-mcp serve` quand
//! `wa-mcp service` tourne. L'hôte parle MCP sur stdio comme avant ; chaque
//! message passe tel quel au service, qui ouvre une session MCP par relais.

use anyhow::{Result, anyhow};
use futures::future::BoxFuture;
use rmcp::service::{RoleClient, RoleServer};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::Transport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;

/// Transport vers le service, authentifié par `token`.
pub fn upstream(url: &str, token: &str) -> impl Transport<RoleClient> + 'static {
    StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(url.to_owned()).auth_header(token.to_owned()),
    )
}

/// Relaie `down` (l'hôte) vers `up` (le service) jusqu'à ce que l'un parte.
/// Fin normale quand l'hôte ferme stdio ; erreur quand le service disparaît,
/// pour que l'hôte relance `serve`, qui reprendra alors la session en process.
pub async fn run<D, U>(mut down: D, mut up: U) -> Result<()>
where
    D: Transport<RoleServer>,
    U: Transport<RoleClient>,
{
    // Les envois vers le service partent dans l'ordre (initialize avant
    // initialized, une requête avant son annulation) sans bloquer le sens
    // retour : une tâche les attend un par un.
    let (queue, mut pending) =
        tokio::sync::mpsc::unbounded_channel::<BoxFuture<'static, Result<(), U::Error>>>();
    let mut sender = tokio::spawn(async move {
        while let Some(send) = pending.recv().await {
            send.await?;
        }
        Ok::<_, U::Error>(())
    });
    let result = loop {
        tokio::select! {
            from_host = down.receive() => match from_host {
                Some(m) => {
                    if queue.send(Box::pin(up.send(m))).is_err() {
                        break Err(anyhow!("relais vers le service interrompu"));
                    }
                }
                None => break Ok(()),
            },
            from_service = up.receive() => match from_service {
                Some(m) => down.send(m).await.map_err(|e| anyhow!("stdio : {e}"))?,
                None => break Err(anyhow!("le service wa-mcp a fermé la session")),
            },
            sent = &mut sender => break match sent {
                Ok(Ok(())) => Err(anyhow!("relais vers le service interrompu")),
                Ok(Err(e)) => Err(anyhow!("envoi au service wa-mcp : {e}")),
                Err(e) => Err(anyhow!("relais : {e}")),
            },
        }
    };
    drop(queue);
    // Hôte parti : ce qui était en route part quand même, brièvement.
    if result.is_ok() {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), &mut sender).await;
    }
    sender.abort();
    // Ferme la session côté service (DELETE) : ses abonnements tombent tout de suite.
    let _ = up.close().await;
    let _ = down.close().await;
    result
}

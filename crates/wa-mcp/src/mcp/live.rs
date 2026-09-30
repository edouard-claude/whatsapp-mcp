//! Temps réel (P5) : notifications de ressources quand une discussion change, et
//! historique plus ancien demandé au téléphone.
//!
//! Deux protocoles coexistent :
//! - `resources/subscribe` (2025-11-25, encore celui de la plupart des hôtes) :
//!   une tâche de notification unique, démarrée au premier abonnement ;
//! - `subscriptions/listen` (2026-07-28) : un flux par requête, servi par `listen`.
//!
//! Dans les deux cas, un URI abonné est d'abord ramené à sa forme canonique (JID
//! plutôt que nom ou numéro) pour être comparé aux changements diffusés par
//! l'ingestion.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::ResourceUpdatedNotificationParam;
use rmcp::service::{Peer, SubscriptionContext};
use rmcp::{ErrorData as McpError, Json, RoleServer, tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use wa_proto::wabridge::v1 as pb;

use super::WaServer;
use crate::ingest::ChatUpdate;
use crate::query;

/// Abonnements du protocole `resources/subscribe`.
#[derive(Default)]
pub struct LegacySubs {
    peer: Option<Peer<RoleServer>>,
    /// URI canonique -> URI tel que l'hôte l'a demandé.
    uris: HashMap<String, String>,
    notifier_started: bool,
}

pub type SharedSubs = Arc<parking_lot::Mutex<LegacySubs>>;

/// URI touchés par un changement de discussion.
fn touched(u: &ChatUpdate) -> [String; 3] {
    [
        format!("whatsapp://{}/chat/{}/messages", u.account, u.chat),
        format!("whatsapp://{}/chat/{}", u.account, u.chat),
        format!("whatsapp://{}/chats", u.account),
    ]
}

impl WaServer {
    /// Forme canonique d'un URI : le segment de discussion résolu en JID.
    pub(super) async fn canonical_uri(&self, uri: &str) -> String {
        let Some(rest) = uri.strip_prefix("whatsapp://") else {
            return uri.to_owned();
        };
        let parts: Vec<&str> = rest.split('/').collect();
        match parts.as_slice() {
            [account, "chat", chat, tail @ ..] => {
                let Ok(alias) = self.account(Some(account)) else {
                    return uri.to_owned();
                };
                let chat = super::resources::decode(chat);
                match self.resolve(&alias, chat).await {
                    Ok(jid) => {
                        let mut out = format!("whatsapp://{alias}/chat/{jid}");
                        for t in tail {
                            out.push('/');
                            out.push_str(t);
                        }
                        out
                    }
                    Err(_) => uri.to_owned(),
                }
            }
            _ => uri.to_owned(),
        }
    }

    pub(super) async fn legacy_subscribe(&self, uri: String, peer: Peer<RoleServer>) {
        let canonical = self.canonical_uri(&uri).await;
        let start = {
            let mut s = self.subs.lock();
            s.uris.insert(canonical, uri);
            s.peer = Some(peer);
            !std::mem::replace(&mut s.notifier_started, true)
        };
        if start {
            tokio::spawn(legacy_notifier(self.subs.clone(), self.updates.subscribe()));
        }
    }

    pub(super) async fn legacy_unsubscribe(&self, uri: &str) {
        let canonical = self.canonical_uri(uri).await;
        self.subs.lock().uris.remove(&canonical);
    }

    /// Un flux `subscriptions/listen` : notifie les URI acceptés jusqu'à annulation.
    pub(super) async fn serve_listen(&self, cx: SubscriptionContext) -> Result<(), McpError> {
        let mut wanted = HashMap::new();
        for uri in cx
            .accepted()
            .resource_subscriptions
            .clone()
            .unwrap_or_default()
        {
            wanted.insert(self.canonical_uri(&uri).await, uri);
        }
        let mut rx = self.updates.subscribe();
        loop {
            tokio::select! {
                () = cx.cancelled() => return Ok(()),
                update = rx.recv() => match update {
                    Ok(u) => {
                        for uri in touched(&u) {
                            if let Some(original) = wanted.get(&uri)
                                && cx.sink().notify_resource_updated(original.clone()).await.is_err()
                            {
                                return Ok(());
                            }
                        }
                    }
                    // En retard : des notifications sont perdues, pas le flux.
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(missed = n, "notifications de ressources perdues");
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                },
            }
        }
    }
}

async fn legacy_notifier(subs: SharedSubs, mut rx: broadcast::Receiver<ChatUpdate>) {
    loop {
        let update = match rx.recv().await {
            Ok(u) => u,
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(missed = n, "notifications de ressources perdues");
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => return,
        };
        // Le verrou n'est tenu que pour copier : jamais pendant un envoi.
        let (peer, targets) = {
            let s = subs.lock();
            let targets: Vec<String> = touched(&update)
                .iter()
                .filter_map(|u| s.uris.get(u).cloned())
                .collect();
            (s.peer.clone(), targets)
        };
        let Some(peer) = peer else { continue };
        for uri in targets {
            if let Err(e) = peer
                .notify_resource_updated(ResourceUpdatedNotificationParam::new(uri))
                .await
            {
                tracing::debug!(error = %e, "hôte MCP parti, fin des notifications");
                return;
            }
        }
    }
}

// ------------------------------------------------------------------ historique

#[derive(Debug, Deserialize, JsonSchema)]
struct HistoryParams {
    #[serde(default)]
    account: Option<String>,
    chat: String,
    /// Nombre de messages plus anciens à demander (1 à 500, défaut 50).
    #[serde(default)]
    count: Option<u32>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct HistoryOut {
    /// Messages en base pour cette discussion, avant et après.
    before: i64,
    after: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    oldest_message_at: Option<String>,
    detail: String,
}

/// Délai d'attente de la réponse du téléphone.
const HISTORY_WAIT: Duration = Duration::from_secs(45);

#[tool_router(router = live_router, vis = "pub(super)")]
impl WaServer {
    #[tool(
        description = "Demande au téléphone des messages plus anciens que le plus ancien connu d'une discussion (le téléphone doit être en ligne). Attend jusqu'à 45 s leur arrivée, puis `list_messages` avec `before` les montre.",
        annotations(
            title = "Historique plus ancien",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn request_history(
        &self,
        Parameters(p): Parameters<HistoryParams>,
    ) -> Result<Json<HistoryOut>, String> {
        let account = self.account(p.account.as_deref())?;
        let (chat, count_before, oldest) = self
            .read(&account, move |cx| {
                let jid = query::resolve_chat(cx, &p.chat)?;
                let (n, oldest) = query::oldest_message(cx, &jid)?;
                Ok((jid, n, oldest))
            })
            .await?;
        let Some(oldest) = oldest else {
            return Err(
                "aucun message connu dans cette discussion : rien à partir de quoi remonter".into(),
            );
        };
        let mut rx = self.updates.subscribe();
        self.bridge
            .request_history(pb::RequestHistory {
                account: account.to_string(),
                chat: chat.clone(),
                oldest_id: oldest.id,
                oldest_from_me: oldest.from_me,
                oldest_sender: oldest.sender,
                oldest_ts_ms: oldest.timestamp_ms,
                count: p.count.unwrap_or(50).clamp(1, 500),
            })
            .await
            .map_err(|e| format!("demande impossible : {e}"))?;
        let arrived = tokio::time::timeout(HISTORY_WAIT, async {
            loop {
                match rx.recv().await {
                    Ok(u) if u.history && u.account == account && u.chat == chat => return true,
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => return false,
                }
            }
        })
        .await
        .unwrap_or(false);
        let chat2 = chat.clone();
        let (count_after, oldest_after) = self
            .read(&account, move |cx| query::oldest_message(cx, &chat2))
            .await?;
        let oldest_message_at = oldest_after.map(|o| {
            let t = o.timestamp_ms;
            jiff::Timestamp::from_millisecond(t)
                .map(|t| {
                    t.to_zoned(self.tz.clone())
                        .strftime("%Y-%m-%d %H:%M")
                        .to_string()
                })
                .unwrap_or_default()
        });
        Ok(Json(HistoryOut {
            before: count_before,
            after: count_after,
            oldest_message_at,
            detail: if arrived {
                format!("{} messages reçus", count_after - count_before)
            } else {
                "pas de réponse du téléphone dans le délai : il est peut-être hors ligne, ou il n'a rien de plus ancien".into()
            },
        }))
    }
}

//! Ressources MCP : la même matière que les outils de lecture, adressable par URI.
//!
//! ```text
//! whatsapp://accounts
//! whatsapp://{account}/status
//! whatsapp://{account}/chats
//! whatsapp://{account}/chat/{chat}
//! whatsapp://{account}/chat/{chat}/messages
//! whatsapp://{account}/contact/{contact}
//! whatsapp://{account}/media/{chat}/{message_id}
//! ```
//!
//! `{chat}` et `{contact}` acceptent un JID, un numéro ou un nom, encodés en URL.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use rmcp::ErrorData as McpError;
use rmcp::model::{CacheScope, ReadResourceResult, Resource, ResourceContents, ResourceTemplate};
use serde::Serialize;

use super::WaServer;
use crate::domain::AccountAlias;
use crate::query::{self, ChatFilter, MessageFilter};

const SCHEME: &str = "whatsapp://";
/// Fraîcheur annoncée : les messages arrivent en continu.
const TTL_MS: u64 = 10_000;

pub fn catalogue(accounts: &[AccountAlias]) -> Vec<Resource> {
    let mut out = vec![
        Resource::new("whatsapp://accounts", "accounts")
            .with_title("Comptes WhatsApp")
            .with_description("Les comptes servis et leur état de connexion.")
            .with_mime_type("application/json"),
    ];
    for a in accounts {
        out.push(
            Resource::new(format!("{SCHEME}{a}/status"), format!("{a}-status"))
                .with_title(format!("État du compte {a}"))
                .with_mime_type("application/json"),
        );
        out.push(
            Resource::new(format!("{SCHEME}{a}/chats"), format!("{a}-chats"))
                .with_title(format!("Discussions du compte {a}"))
                .with_description("Les 100 discussions les plus récentes.")
                .with_mime_type("application/json"),
        );
    }
    out
}

pub fn templates() -> Vec<ResourceTemplate> {
    let t = |uri: &str, name: &str, title: &str, desc: &str, mime: &str| {
        ResourceTemplate::new(uri, name)
            .with_title(title)
            .with_description(desc)
            .with_mime_type(mime)
    };
    vec![
        t(
            "whatsapp://{account}/chat/{chat}",
            "chat",
            "Discussion",
            "Détail d'une discussion : participants d'un groupe, fiche d'une personne.",
            "application/json",
        ),
        t(
            "whatsapp://{account}/chat/{chat}/messages",
            "chat-messages",
            "Messages d'une discussion",
            "Les 100 derniers messages, en ordre chronologique.",
            "application/json",
        ),
        t(
            "whatsapp://{account}/contact/{contact}",
            "contact",
            "Contact",
            "Fiche d'un contact et groupes en commun.",
            "application/json",
        ),
        t(
            "whatsapp://{account}/media/{chat}/{message_id}",
            "media",
            "Média d'un message",
            "Le fichier du média, déjà téléchargé par `get_media`.",
            "application/octet-stream",
        ),
    ]
}

fn not_found(uri: &str, why: impl std::fmt::Display) -> McpError {
    McpError::resource_not_found(format!("{uri} : {why}"), None)
}

/// Décode un segment d'URI (`%20`, `%40`...).
pub(super) fn decode(seg: &str) -> String {
    let bytes = seg.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |b: u8| (b as char).to_digit(16);
        match (
            bytes[i],
            bytes.get(i + 1).copied().and_then(hex),
            bytes.get(i + 2).copied().and_then(hex),
        ) {
            (b'%', Some(h), Some(l)) => {
                // h et l sont des chiffres hexadécimaux : h * 16 + l tient dans un octet.
                out.push(u8::try_from(h * 16 + l).unwrap_or(b'?'));
                i += 3;
            }
            (b, _, _) => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn json<T: Serialize>(uri: &str, v: &T) -> Result<ReadResourceResult, McpError> {
    let text = serde_json::to_string_pretty(v)
        .map_err(|e| McpError::internal_error(e.to_string(), None))?;
    Ok(ReadResourceResult::new(vec![
        ResourceContents::text(text, uri).with_mime_type("application/json"),
    ])
    .with_ttl_ms(TTL_MS)
    .with_cache_scope(CacheScope::Private))
}

pub async fn read(s: &WaServer, uri: &str) -> Result<ReadResourceResult, McpError> {
    let rest = uri
        .strip_prefix(SCHEME)
        .ok_or_else(|| not_found(uri, "schéma inconnu"))?;
    if rest == "accounts" {
        let mut all = Vec::new();
        for a in &s.accounts() {
            let alias = a.clone();
            all.push(
                s.read(a, move |cx| query::status(cx, &alias))
                    .await
                    .map_err(|e| not_found(uri, e))?,
            );
        }
        return json(uri, &all);
    }
    let (account, path) = rest
        .split_once('/')
        .ok_or_else(|| not_found(uri, "chemin incomplet"))?;
    let account = s.account(Some(account)).map_err(|e| not_found(uri, e))?;
    let segs: Vec<String> = path.split('/').map(decode).collect();
    let segs: Vec<&str> = segs.iter().map(String::as_str).collect();
    let r = |e: String| not_found(uri, e);
    match segs.as_slice() {
        ["status"] => {
            let alias = account.clone();
            json(
                uri,
                &s.read(&account, move |cx| query::status(cx, &alias))
                    .await
                    .map_err(r)?,
            )
        }
        ["chats"] => {
            let filter = ChatFilter {
                query: None,
                kind: None,
                include_archived: false,
                limit: 100,
                offset: 0,
            };
            json(
                uri,
                &s.read(&account, move |cx| {
                    query::list_chats(cx, &filter).map(|(c, _)| c)
                })
                .await
                .map_err(r)?,
            )
        }
        ["chat", chat] => {
            let chat = (*chat).to_owned();
            json(
                uri,
                &s.read(&account, move |cx| {
                    let jid = query::resolve_chat(cx, &chat)?;
                    query::get_chat(cx, &jid)
                })
                .await
                .map_err(r)?,
            )
        }
        ["chat", chat, "messages"] => {
            let chat = (*chat).to_owned();
            json(
                uri,
                &s.read(&account, move |cx| {
                    let jid = query::resolve_chat(cx, &chat)?;
                    let filter = MessageFilter {
                        before: None,
                        since_ms: None,
                        until_ms: None,
                        sender: None,
                        kind: None,
                        limit: 100,
                    };
                    query::list_messages(cx, &jid, &filter).map(|(m, _)| m)
                })
                .await
                .map_err(r)?,
            )
        }
        ["contact", contact] => {
            let contact = (*contact).to_owned();
            json(
                uri,
                &s.read(&account, move |cx| {
                    let jid = query::resolve_chat(cx, &contact)?;
                    query::get_contact(cx, &jid)
                })
                .await
                .map_err(r)?,
            )
        }
        ["media", chat, id] => {
            let (chat, id) = ((*chat).to_owned(), (*id).to_owned());
            let file = s
                .read(&account, move |cx| {
                    let jid = query::resolve_chat(cx, &chat)?;
                    query::cached_media(cx, &jid, &id)
                })
                .await
                .map_err(r)?
                .ok_or_else(|| not_found(uri, "média pas encore téléchargé : appeler get_media"))?;
            let bytes = tokio::fs::read(&file.path)
                .await
                .map_err(|e| McpError::internal_error(e.to_string(), None))?;
            let mut contents = ResourceContents::blob(BASE64.encode(bytes), uri);
            if let Some(m) = file.mime {
                contents = contents.with_mime_type(m);
            }
            Ok(ReadResourceResult::new(vec![contents]).with_cache_scope(CacheScope::Private))
        }
        _ => Err(not_found(uri, "ressource inconnue")),
    }
}

#[cfg(test)]
mod tests {
    use super::decode;

    #[test]
    fn uri_segments_decode() {
        assert_eq!(decode("Famille%20Dupont"), "Famille Dupont");
        assert_eq!(decode("120363%40g.us"), "120363@g.us");
        assert_eq!(decode("%E2%9C%93"), "✓");
        assert_eq!(decode("100%"), "100%");
    }
}

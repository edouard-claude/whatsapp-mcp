//! Outils MCP en lecture (P2) : comptes, discussions, messages, contacts, médias.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock};
use rmcp::{Json, tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::WaServer;
use crate::parse;
use crate::query::{
    self, ChatDetail, ChatFilter, ChatSummary, Contact, Cursor, Message, MessageFilter,
    SearchFilter, Status,
};
use crate::store::Op;
use wa_proto::wabridge::v1 as pb;

// Sorties : `Result<Json<T>, String>` écrit en toutes lettres (un alias cacherait
// `Json` à la macro, qui ne générerait plus d'`outputSchema`). L'erreur est un
// message lisible, rendu avec `isError: true`.

/// Au-delà, un média n'est pas rendu dans la réponse, seulement son chemin.
const INLINE_MAX: u64 = 8 * 1024 * 1024;

fn limit(n: Option<usize>, default: usize, max: usize) -> usize {
    n.unwrap_or(default).clamp(1, max)
}

// ------------------------------------------------------------------ paramètres

#[derive(Debug, Deserialize, JsonSchema)]
struct AccountParam {
    /// Alias du compte. Facultatif s'il n'y en a qu'un.
    #[serde(default)]
    account: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ListChatsParams {
    #[serde(default)]
    account: Option<String>,
    /// Filtre sur le nom ou le numéro (sans accents ni casse).
    #[serde(default)]
    query: Option<String>,
    /// `dm`, `group`, `community`, `broadcast` ou `newsletter`.
    #[serde(default)]
    kind: Option<String>,
    /// Inclure les discussions archivées (non par défaut).
    #[serde(default)]
    include_archived: bool,
    /// 1 à 200, défaut 30.
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    offset: usize,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ChatParam {
    #[serde(default)]
    account: Option<String>,
    /// JID, numéro international ou nom de la discussion.
    chat: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ListMessagesParams {
    #[serde(default)]
    account: Option<String>,
    /// JID, numéro international ou nom de la discussion.
    chat: String,
    /// 1 à 200, défaut 50.
    #[serde(default)]
    limit: Option<usize>,
    /// Curseur `next_before` d'une page précédente : messages plus anciens.
    #[serde(default)]
    before: Option<String>,
    /// Borne basse (date ou durée écoulée).
    #[serde(default)]
    since: Option<String>,
    /// Borne haute (date).
    #[serde(default)]
    until: Option<String>,
    /// Seulement les messages de cet auteur (JID, numéro ou nom).
    #[serde(default)]
    sender: Option<String>,
    /// Seulement ce type : `text`, `image`, `voice`, `document`...
    #[serde(default)]
    kind: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ContextParams {
    #[serde(default)]
    account: Option<String>,
    chat: String,
    message_id: String,
    /// Messages de part et d'autre, 1 à 50, défaut 5.
    #[serde(default)]
    around: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SearchParams {
    #[serde(default)]
    account: Option<String>,
    /// Mots à trouver, tous requis, sans tenir compte des accents ni de la casse.
    query: String,
    /// Limiter à une discussion (JID, numéro ou nom).
    #[serde(default)]
    chat: Option<String>,
    /// Limiter à un auteur (JID, numéro ou nom).
    #[serde(default)]
    sender: Option<String>,
    #[serde(default)]
    since: Option<String>,
    #[serde(default)]
    until: Option<String>,
    /// 1 à 100, défaut 30.
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    offset: usize,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct RecentParams {
    #[serde(default)]
    account: Option<String>,
    /// Depuis quand : durée écoulée (24h, 7d) ou date. Défaut 24h.
    #[serde(default)]
    since: Option<String>,
    /// Inclure mes propres messages (non par défaut).
    #[serde(default)]
    include_mine: bool,
    /// `dm` ou `group` pour ne voir qu'un type de discussion.
    #[serde(default)]
    chat_kind: Option<String>,
    /// 1 à 500, défaut 200.
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SearchContactsParams {
    #[serde(default)]
    account: Option<String>,
    /// Nom ou partie du numéro (au moins 4 chiffres).
    query: String,
    /// 1 à 100, défaut 20.
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ContactParam {
    #[serde(default)]
    account: Option<String>,
    /// JID, numéro international ou nom.
    contact: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MediaParams {
    #[serde(default)]
    account: Option<String>,
    chat: String,
    message_id: String,
}

// ------------------------------------------------------------------ sorties

#[derive(Debug, Serialize, JsonSchema)]
struct AccountsOut {
    accounts: Vec<Status>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct ChatsOut {
    chats: Vec<ChatSummary>,
    /// `offset` de la page suivante, absent s'il n'y en a pas.
    #[serde(skip_serializing_if = "Option::is_none")]
    next_offset: Option<usize>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct MessagesOut {
    chat: String,
    chat_jid: String,
    /// Ordre chronologique.
    messages: Vec<Message>,
    /// À passer dans `before` pour la page plus ancienne, absent s'il n'y en a pas.
    #[serde(skip_serializing_if = "Option::is_none")]
    next_before: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct ContextOut {
    chat_jid: String,
    messages: Vec<Message>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct SearchOut {
    /// Les plus récents d'abord.
    messages: Vec<Message>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_offset: Option<usize>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct RecentOut {
    since: String,
    /// Ordre chronologique, toutes discussions confondues.
    messages: Vec<Message>,
    /// Vrai si la limite a coupé la liste : relancer avec un `since` plus récent.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    truncated: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
struct ContactsOut {
    contacts: Vec<Contact>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct MediaOut {
    kind: String,
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    mime: Option<String>,
    size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_name: Option<String>,
    /// Le fichier est aussi joint à cette réponse (image ou audio de taille raisonnable).
    inline: bool,
}

// ------------------------------------------------------------------ outils

#[tool_router(router = read_router, vis = "pub(super)")]
impl WaServer {
    #[tool(
        description = "Liste les comptes WhatsApp servis, avec leur état de connexion et le volume de données.",
        annotations(title = "Comptes", read_only_hint = true, open_world_hint = false)
    )]
    async fn list_accounts(&self) -> Result<Json<AccountsOut>, String> {
        let mut accounts = Vec::new();
        for a in &self.accounts() {
            let alias = a.clone();
            accounts.push(self.read(a, move |cx| query::status(cx, &alias)).await?);
        }
        Ok(Json(AccountsOut { accounts }))
    }

    #[tool(
        description = "État d'un compte : connexion, numéro, dernière connexion, volume de messages et période couverte.",
        annotations(
            title = "État du compte",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn session_status(
        &self,
        Parameters(p): Parameters<AccountParam>,
    ) -> Result<Json<Status>, String> {
        let account = self.account(p.account.as_deref())?;
        let alias = account.clone();
        Ok(Json(
            self.read(&account, move |cx| query::status(cx, &alias))
                .await?,
        ))
    }

    #[tool(
        description = "Liste les discussions (individuelles et groupes), épinglées puis les plus récentes, avec le dernier message.",
        annotations(title = "Discussions", read_only_hint = true, open_world_hint = false)
    )]
    async fn list_chats(
        &self,
        Parameters(p): Parameters<ListChatsParams>,
    ) -> Result<Json<ChatsOut>, String> {
        let account = self.account(p.account.as_deref())?;
        let filter = ChatFilter {
            query: p.query.filter(|q| !q.trim().is_empty()),
            kind: p.kind,
            include_archived: p.include_archived,
            limit: limit(p.limit, 30, 200),
            offset: p.offset,
        };
        let (chats, more) = self
            .read(&account, move |cx| query::list_chats(cx, &filter))
            .await?;
        let next_offset = more.then(|| p.offset + chats.len());
        Ok(Json(ChatsOut { chats, next_offset }))
    }

    #[tool(
        description = "Détail d'une discussion : pour un groupe, sujet, réglages et participants ; pour une personne, ses noms, son numéro et les groupes en commun.",
        annotations(title = "Discussion", read_only_hint = true, open_world_hint = false)
    )]
    async fn get_chat(
        &self,
        Parameters(p): Parameters<ChatParam>,
    ) -> Result<Json<ChatDetail>, String> {
        let account = self.account(p.account.as_deref())?;
        Ok(Json(
            self.read(&account, move |cx| {
                let jid = query::resolve_chat(cx, &p.chat)?;
                query::get_chat(cx, &jid)
            })
            .await?,
        ))
    }

    #[tool(
        description = "Messages d'une discussion en ordre chronologique, la page la plus récente d'abord ; `next_before` donne la page plus ancienne. Les réactions, réponses citées, éditions et suppressions sont intégrées à chaque message.",
        annotations(title = "Messages", read_only_hint = true, open_world_hint = false)
    )]
    async fn list_messages(
        &self,
        Parameters(p): Parameters<ListMessagesParams>,
    ) -> Result<Json<MessagesOut>, String> {
        let account = self.account(p.account.as_deref())?;
        let before = p
            .before
            .as_deref()
            .map(Cursor::decode)
            .transpose()
            .map_err(|e| e.to_string())?;
        let since_ms = self.date(p.since.as_deref(), false)?;
        let until_ms = self.date(p.until.as_deref(), true)?;
        let lim = limit(p.limit, 50, 200);
        Ok(Json(
            self.read(&account, move |cx| {
                let jid = query::resolve_chat(cx, &p.chat)?;
                let sender = p
                    .sender
                    .as_deref()
                    .map(|s| query::resolve_chat(cx, s))
                    .transpose()?;
                let filter = MessageFilter {
                    before,
                    since_ms,
                    until_ms,
                    sender,
                    kind: p.kind,
                    limit: lim,
                };
                let (messages, next) = query::list_messages(cx, &jid, &filter)?;
                let chat = query::get_chat(cx, &jid)
                    .map(|c| c.summary.name)
                    .unwrap_or_else(|_| jid.clone());
                Ok(MessagesOut {
                    chat,
                    chat_jid: jid,
                    messages,
                    next_before: next.map(Cursor::encode),
                })
            })
            .await?,
        ))
    }

    #[tool(
        description = "Un message et ceux qui l'entourent dans sa discussion, par exemple après une recherche.",
        annotations(
            title = "Contexte d'un message",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn get_message_context(
        &self,
        Parameters(p): Parameters<ContextParams>,
    ) -> Result<Json<ContextOut>, String> {
        let account = self.account(p.account.as_deref())?;
        let around = limit(p.around, 5, 50);
        Ok(Json(
            self.read(&account, move |cx| {
                let jid = query::resolve_chat(cx, &p.chat)?;
                let messages = query::message_context(cx, &jid, &p.message_id, around)?;
                Ok(ContextOut {
                    chat_jid: jid,
                    messages,
                })
            })
            .await?,
        ))
    }

    #[tool(
        description = "Recherche plein texte dans les messages (texte, légendes, noms de fichiers), les plus récents d'abord. Tous les mots sont requis ; accents et casse ignorés.",
        annotations(title = "Rechercher", read_only_hint = true, open_world_hint = false)
    )]
    async fn search_messages(
        &self,
        Parameters(p): Parameters<SearchParams>,
    ) -> Result<Json<SearchOut>, String> {
        let account = self.account(p.account.as_deref())?;
        let since_ms = self.date(p.since.as_deref(), false)?;
        let until_ms = self.date(p.until.as_deref(), true)?;
        let lim = limit(p.limit, 30, 100);
        let offset = p.offset;
        let (messages, more) = self
            .read(&account, move |cx| {
                let chat = p
                    .chat
                    .as_deref()
                    .map(|c| query::resolve_chat(cx, c))
                    .transpose()?;
                let sender = p
                    .sender
                    .as_deref()
                    .map(|s| query::resolve_chat(cx, s))
                    .transpose()?;
                query::search_messages(
                    cx,
                    &SearchFilter {
                        query: p.query,
                        chat,
                        sender,
                        since_ms,
                        until_ms,
                        limit: lim,
                        offset,
                    },
                )
            })
            .await?;
        let next_offset = more.then(|| offset + messages.len());
        Ok(Json(SearchOut {
            messages,
            next_offset,
        }))
    }

    #[tool(
        description = "Tout ce qui est arrivé depuis un moment donné, toutes discussions confondues, en ordre chronologique : le point de départ pour « quoi de neuf ».",
        annotations(
            title = "Messages récents",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn list_recent(
        &self,
        Parameters(p): Parameters<RecentParams>,
    ) -> Result<Json<RecentOut>, String> {
        let account = self.account(p.account.as_deref())?;
        let since_input = p.since.clone().unwrap_or_else(|| "24h".into());
        let since_ms = self.date(Some(&since_input), false)?.unwrap_or(0);
        let lim = limit(p.limit, 200, 500);
        let (messages, truncated) = self
            .read(&account, move |cx| {
                query::recent_messages(cx, since_ms, p.include_mine, p.chat_kind.as_deref(), lim)
            })
            .await?;
        Ok(Json(RecentOut {
            since: since_input,
            messages,
            truncated,
        }))
    }

    #[tool(
        description = "Cherche des contacts par nom (carnet, nom WhatsApp, nom d'entreprise) ou par partie de numéro.",
        annotations(
            title = "Chercher un contact",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn search_contacts(
        &self,
        Parameters(p): Parameters<SearchContactsParams>,
    ) -> Result<Json<ContactsOut>, String> {
        let account = self.account(p.account.as_deref())?;
        let lim = limit(p.limit, 20, 100);
        let contacts = self
            .read(&account, move |cx| {
                query::search_contacts(cx, &p.query, lim)
            })
            .await?;
        Ok(Json(ContactsOut { contacts }))
    }

    #[tool(
        description = "Fiche d'un contact : noms, numéro, dernière discussion et groupes en commun.",
        annotations(title = "Contact", read_only_hint = true, open_world_hint = false)
    )]
    async fn get_contact(
        &self,
        Parameters(p): Parameters<ContactParam>,
    ) -> Result<Json<Contact>, String> {
        let account = self.account(p.account.as_deref())?;
        Ok(Json(
            self.read(&account, move |cx| {
                let jid = query::resolve_chat(cx, &p.contact)?;
                query::get_contact(cx, &jid)
            })
            .await?,
        ))
    }
}

#[tool_router(router = media_router, vis = "pub(super)")]
impl WaServer {
    #[tool(
        description = "Télécharge le média d'un message (vocal, image, vidéo, document, sticker) et le rend tel quel : chemin local, et le fichier lui-même joint pour une image ou un audio de taille raisonnable. Aucune transcription ni description : c'est au modèle de les faire.",
        annotations(
            title = "Média d'un message",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn get_media(
        &self,
        Parameters(p): Parameters<MediaParams>,
    ) -> Result<CallToolResult, String> {
        let account = self.account(p.account.as_deref())?;
        let id = p.message_id.clone();
        let (chat, r, cached) = self
            .read(&account, move |cx| {
                let chat = query::resolve_chat(cx, &p.chat)?;
                let r = query::message_ref(cx, &chat, &p.message_id)?;
                let cached = query::cached_media(cx, &chat, &p.message_id)?;
                Ok((chat, r, cached))
            })
            .await?;
        if r.deleted || r.raw.is_empty() {
            return Err(format!("le message {id} a été supprimé"));
        }
        let mut msg = parse::decode(&r.raw).map_err(|e| format!("message illisible : {e}"))?;
        // Copie : `msg` doit rester modifiable si le téléphone renvoie le média.
        let inner = parse::unwrap(&msg).clone();
        let inner = &inner;
        let media =
            parse::media(inner).ok_or_else(|| format!("le message {id} ne porte pas de média"))?;

        let file = match cached {
            Some(f) => f,
            None => {
                let path = self
                    .reader
                    .account_dir(&account)
                    .join("media")
                    .join(safe(&chat))
                    .join(format!(
                        "{}.{}",
                        safe(&id),
                        extension(media.mime.as_deref(), media.file_name.as_deref())
                    ));
                let first = self
                    .bridge
                    .download_media(&account, prost::Message::encode_to_vec(inner), &path)
                    .await;
                let res = match first {
                    Ok(res) => res,
                    // Expiré des serveurs : le téléphone peut le renvoyer.
                    Err(e) if e.to_string().contains("expiré") => {
                        let key = media
                            .media_key
                            .clone()
                            .ok_or_else(|| format!("média expiré et sans clé : {e}"))?;
                        let direct_path = self
                            .bridge
                            .media_retry(pb::MediaRetry {
                                account: account.to_string(),
                                chat: chat.clone(),
                                sender: r.sender.clone(),
                                from_me: r.from_me,
                                id: id.clone(),
                                media_key: key,
                            })
                            .await
                            .map_err(|e| format!("média expiré, renvoi impossible : {e}"))?;
                        if !parse::set_direct_path(&mut msg, &direct_path) {
                            return Err("média renvoyé mais message sans média".into());
                        }
                        // Le nouveau chemin est gardé : un prochain appel n'aura pas à redemander.
                        let _ = self
                            .store
                            .apply(
                                account.clone(),
                                Op::UpdateRaw {
                                    chat: chat.clone(),
                                    id: id.clone(),
                                    raw: prost::Message::encode_to_vec(&msg),
                                },
                            )
                            .await;
                        self.bridge
                            .download_media(
                                &account,
                                prost::Message::encode_to_vec(parse::unwrap(&msg)),
                                &path,
                            )
                            .await
                            .map_err(|e| format!("téléchargement impossible après renvoi : {e}"))?
                    }
                    Err(e) => return Err(format!("téléchargement impossible : {e}")),
                };
                let file = query::MediaFile {
                    path: res.path,
                    mime: media.mime.clone(),
                    size: res.size,
                };
                self.store
                    .apply(
                        account.clone(),
                        Op::Media {
                            chat: chat.clone(),
                            id: id.clone(),
                            path: file.path.clone(),
                            mime: file.mime.clone(),
                            size: file.size,
                        },
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                file
            }
        };

        let mime = file
            .mime
            .clone()
            .unwrap_or_else(|| "application/octet-stream".into());
        let base = mime.split(';').next().unwrap_or(&mime).trim().to_owned();
        let mut blocks = Vec::new();
        if file.size <= INLINE_MAX && (base.starts_with("image/") || base.starts_with("audio/")) {
            let bytes = tokio::fs::read(&file.path)
                .await
                .map_err(|e| e.to_string())?;
            let data = BASE64.encode(bytes);
            blocks.push(if base.starts_with("image/") {
                ContentBlock::image(data, base.clone())
            } else {
                ContentBlock::audio(data, base.clone())
            });
        }
        let out = MediaOut {
            kind: media.kind.as_str().into(),
            path: file.path,
            mime: file.mime,
            size: file.size,
            file_name: media.file_name,
            inline: !blocks.is_empty(),
        };
        let value = serde_json::to_value(&out).map_err(|e| e.to_string())?;
        let mut result = CallToolResult::structured(value);
        result.content.extend(blocks);
        Ok(result)
    }
}

/// Composant de chemin sûr : alphanumérique, `-`, `_` et `.` seulement.
fn safe(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_start_matches('.')
        .to_owned()
}

fn extension(mime: Option<&str>, file_name: Option<&str>) -> String {
    if let Some(ext) = file_name
        .and_then(|f| f.rsplit_once('.'))
        .map(|(_, e)| safe(e))
        .filter(|e| !e.is_empty() && e.len() <= 8)
    {
        return ext;
    }
    let base = mime.unwrap_or("").split(';').next().unwrap_or("").trim();
    match base {
        "image/jpeg" => "jpg",
        "image/png" => "png",
        "image/webp" => "webp",
        "image/gif" => "gif",
        "video/mp4" => "mp4",
        "audio/ogg" => "ogg",
        "audio/mpeg" => "mp3",
        "audio/mp4" | "audio/aac" => "m4a",
        "application/pdf" => "pdf",
        _ => "bin",
    }
    .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_paths_stay_flat() {
        assert_eq!(safe("../../etc/passwd"), "_.._etc_passwd");
        assert_eq!(safe("120363@g.us"), "120363_g.us");
        assert_eq!(extension(Some("audio/ogg; codecs=opus"), None), "ogg");
        assert_eq!(extension(None, Some("devis final.PDF")), "PDF");
        assert_eq!(extension(None, Some("x./../y")), "_y");
    }
}

//! Outils d'écriture (P3) : envoyer, répondre, réagir, modifier, supprimer,
//! marquer comme lu, signaler la frappe, vérifier des numéros.
//!
//! La confirmation d'un envoi appartient à l'hôte MCP (politique d'approbation
//! par outil) : les annotations disent exactement ce qui est irréversible.
//! Chaque message envoyé est aussi rangé en base, comme s'il avait été reçu.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use rmcp::handler::server::wrapper::Parameters;
use rmcp::{Json, tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use wa_proto::WaMessage;
use wa_proto::wabridge::v1 as pb;

use super::WaServer;
use crate::compose::{self, Media, MediaKind, Quoted};
use crate::domain::AccountAlias;
use crate::parse;
use crate::query;
use crate::store::{MessageIn, Op, Source};

/// Envois tolérés par compte et par minute : au-delà, WhatsApp risque de voir un
/// robot et de bannir le numéro.
const SENDS_PER_MINUTE: usize = 20;
/// WhatsApp n'accepte plus de modification après ce délai.
const EDIT_WINDOW: Duration = Duration::from_secs(15 * 60);
/// Ni de suppression pour tous (environ deux jours et demi).
const REVOKE_WINDOW: Duration = Duration::from_secs(60 * 60 * 60);

/// Fenêtre glissante des envois récents, par compte.
#[derive(Default)]
pub struct SendLimiter {
    // Mutex std : section critique de quelques instructions, jamais tenue pendant
    // un `.await`. Un canal vers une tâche propriétaire serait plus lourd pour rien.
    recent: parking_lot::Mutex<HashMap<AccountAlias, VecDeque<Instant>>>,
}

impl SendLimiter {
    fn check(&self, account: &AccountAlias) -> Result<(), String> {
        let now = Instant::now();
        let mut all = self.recent.lock();
        let q = all.entry(account.clone()).or_default();
        while q
            .front()
            .is_some_and(|t| now.duration_since(*t) > Duration::from_secs(60))
        {
            q.pop_front();
        }
        if q.len() >= SENDS_PER_MINUTE {
            return Err(format!(
                "limite de {SENDS_PER_MINUTE} envois par minute atteinte : attendre avant de continuer (protection contre le bannissement)"
            ));
        }
        q.push_back(now);
        Ok(())
    }
}

fn now_ms() -> i64 {
    jiff::Timestamp::now().as_millisecond()
}

// ------------------------------------------------------------------ paramètres

#[derive(Debug, Deserialize, JsonSchema)]
struct SendMessageParams {
    #[serde(default)]
    account: Option<String>,
    /// Destinataire : JID, numéro international ou nom (contact ou groupe).
    chat: String,
    text: String,
    /// Identifiant du message auquel répondre (il sera cité).
    #[serde(default)]
    reply_to: Option<String>,
    /// Personnes mentionnées (JID, numéro ou nom). Le texte doit contenir `@<numéro>`
    /// pour chacune, par exemple `@33612345678`.
    #[serde(default)]
    mentions: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SendMediaParams {
    #[serde(default)]
    account: Option<String>,
    chat: String,
    /// Chemin absolu du fichier à envoyer. Le type (image, vidéo, audio, document)
    /// est déduit de l'extension.
    path: String,
    #[serde(default)]
    caption: Option<String>,
    /// Envoyer un fichier OGG Opus comme message vocal plutôt que comme fichier audio.
    #[serde(default)]
    as_voice_note: bool,
    #[serde(default)]
    reply_to: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SendLocationParams {
    #[serde(default)]
    account: Option<String>,
    chat: String,
    latitude: f64,
    longitude: f64,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    address: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SendPollParams {
    #[serde(default)]
    account: Option<String>,
    chat: String,
    question: String,
    /// 2 à 12 choix.
    options: Vec<String>,
    /// Plusieurs choix possibles par personne (non par défaut).
    #[serde(default)]
    multiple: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ReactParams {
    #[serde(default)]
    account: Option<String>,
    chat: String,
    message_id: String,
    /// Un emoji ; chaîne vide pour retirer sa réaction.
    emoji: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct EditParams {
    #[serde(default)]
    account: Option<String>,
    chat: String,
    message_id: String,
    /// Nouveau texte.
    text: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MessageParam {
    #[serde(default)]
    account: Option<String>,
    chat: String,
    message_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ChatOnly {
    #[serde(default)]
    account: Option<String>,
    chat: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct PresenceParams {
    #[serde(default)]
    account: Option<String>,
    chat: String,
    /// `composing` (en train d'écrire), `recording` (enregistre un vocal) ou `paused`.
    state: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct CheckParams {
    #[serde(default)]
    account: Option<String>,
    /// Numéros internationaux (`+33612345678` ou `33612345678`), 50 au plus.
    phones: Vec<String>,
}

// ------------------------------------------------------------------ sorties

#[derive(Debug, Serialize, JsonSchema)]
struct Sent {
    chat_jid: String,
    message_id: String,
    at: String,
}

#[derive(Debug, Serialize, JsonSchema)]
struct Done {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct Checks {
    results: Vec<Check>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct Check {
    phone: String,
    on_whatsapp: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    jid: Option<String>,
}

// ------------------------------------------------------------------ mécanique commune

impl WaServer {
    /// Cite un message existant de la discussion.
    async fn quoted(
        &self,
        account: &AccountAlias,
        chat: &str,
        id: Option<String>,
    ) -> Result<Option<Quoted>, String> {
        let Some(id) = id.filter(|i| !i.trim().is_empty()) else {
            return Ok(None);
        };
        let chat = chat.to_owned();
        let r = self
            .read(account, move |cx| query::message_ref(cx, &chat, &id))
            .await?;
        let message = parse::decode(&r.raw)
            .ok()
            .map(|m| parse::unwrap(&m).clone());
        Ok(Some(Quoted {
            id: r.id,
            sender: r.sender,
            message,
        }))
    }

    /// Envoie, puis range le message envoyé en base.
    async fn deliver(
        &self,
        account: &AccountAlias,
        chat: &str,
        msg: &WaMessage,
    ) -> Result<Json<Sent>, String> {
        self.limiter.check(account)?;
        let sent = self
            .bridge
            .send_message(account, chat, msg, None)
            .await
            .map_err(|e| format!("envoi impossible : {e}"))?;
        self.remember(account, chat, msg, &sent).await;
        Ok(Json(Sent {
            chat_jid: chat.to_owned(),
            at: jiff::Timestamp::from_millisecond(sent.timestamp_ms)
                .map(|t| {
                    t.to_zoned(self.tz.clone())
                        .strftime("%Y-%m-%d %H:%M:%S%:z")
                        .to_string()
                })
                .unwrap_or_default(),
            message_id: sent.message_id,
        }))
    }

    /// Range un message envoyé : WhatsApp ne le renvoie pas à l'appareil qui l'a émis.
    /// Un échec ici n'annule pas l'envoi, déjà parti : il est seulement journalisé.
    async fn remember(
        &self,
        account: &AccountAlias,
        chat: &str,
        msg: &WaMessage,
        sent: &pb::SendResult,
    ) {
        let own = match self.read(account, query::own_jid).await {
            Ok(j) => j,
            Err(e) => {
                tracing::warn!(error = %e, "message envoyé non rangé");
                return;
            }
        };
        let op = Op::Message(MessageIn {
            info: pb::MessageInfo {
                id: sent.message_id.clone(),
                chat: chat.to_owned(),
                sender: own,
                from_me: true,
                is_group: chat.ends_with("@g.us"),
                timestamp_ms: sent.timestamp_ms,
                ..Default::default()
            },
            parsed: parse::parse(msg),
            raw: prost::Message::encode_to_vec(msg),
            source: Source::Live,
        });
        match self.store.apply(account.clone(), op).await {
            // Les abonnés à la discussion voient aussi mes propres envois.
            Ok(()) => {
                let _ = self.updates.send(crate::ingest::ChatUpdate {
                    account: account.clone(),
                    chat: chat.to_owned(),
                    history: false,
                });
            }
            Err(e) => tracing::warn!(error = %e, "message envoyé non rangé"),
        }
    }

    /// Cible d'une action sur un message : discussion résolue et message connu.
    async fn target(
        &self,
        account: &AccountAlias,
        chat: String,
        id: String,
    ) -> Result<(String, query::MessageRef), String> {
        self.read(account, move |cx| {
            let jid = query::resolve_chat(cx, &chat)?;
            let r = query::message_ref(cx, &jid, &id)?;
            Ok((jid, r))
        })
        .await
    }
}

fn elapsed_since(ms: i64) -> Duration {
    let age = now_ms().saturating_sub(ms);
    Duration::from_millis(u64::try_from(age).unwrap_or(0))
}

// ------------------------------------------------------------------ outils

#[tool_router(router = write_router, vis = "pub(super)")]
impl WaServer {
    #[tool(
        description = "Envoie un message texte, éventuellement en réponse à un message (cité) et avec des mentions. Irréversible une fois parti (voir delete_message).",
        annotations(
            title = "Envoyer un message",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn send_message(
        &self,
        Parameters(p): Parameters<SendMessageParams>,
    ) -> Result<Json<Sent>, String> {
        let account = self.account(p.account.as_deref())?;
        if p.text.trim().is_empty() {
            return Err("texte vide".into());
        }
        let chat = self.resolve(&account, p.chat).await?;
        let mut mentions = Vec::new();
        for m in p.mentions {
            let jid = self.resolve(&account, m).await?;
            let digits = jid.split('@').next().unwrap_or_default().to_owned();
            if !p.text.contains(&format!("@{digits}")) {
                return Err(format!(
                    "le texte doit contenir @{digits} pour mentionner {jid}"
                ));
            }
            mentions.push(jid);
        }
        let quoted = self.quoted(&account, &chat, p.reply_to).await?;
        let msg = compose::text(&p.text, quoted, mentions);
        self.deliver(&account, &chat, &msg).await
    }

    #[tool(
        description = "Envoie un fichier local : image, vidéo, audio, document, ou message vocal (OGG Opus avec as_voice_note). Le type est déduit de l'extension.",
        annotations(
            title = "Envoyer un fichier",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn send_media(
        &self,
        Parameters(p): Parameters<SendMediaParams>,
    ) -> Result<Json<Sent>, String> {
        let account = self.account(p.account.as_deref())?;
        let path = PathBuf::from(&p.path);
        if !path.is_absolute() {
            return Err("chemin absolu attendu".into());
        }
        let meta = tokio::fs::metadata(&path)
            .await
            .map_err(|e| format!("{} : {e}", path.display()))?;
        if !meta.is_file() {
            return Err(format!("{} n'est pas un fichier", path.display()));
        }
        let (kind, mime) = compose::guess(&path, p.as_voice_note);
        if p.as_voice_note && kind != MediaKind::Voice {
            return Err("un message vocal doit être un fichier OGG Opus (.ogg, .opus)".into());
        }
        let seconds = if matches!(kind, MediaKind::Voice | MediaKind::Audio)
            && mime.starts_with("audio/ogg")
        {
            let data = tokio::fs::read(&path).await.map_err(|e| e.to_string())?;
            compose::ogg_opus_seconds(&data)
        } else {
            None
        };
        let chat = self.resolve(&account, p.chat).await?;
        let quoted = self.quoted(&account, &chat, p.reply_to).await?;
        let up = self
            .bridge
            .upload_media(&account, &path, kind.upload_type())
            .await
            .map_err(|e| format!("téléversement impossible : {e}"))?;
        let media = Media {
            kind,
            mime,
            caption: p.caption.filter(|c| !c.trim().is_empty()),
            file_name: path.file_name().and_then(|n| n.to_str()).map(str::to_owned),
            seconds,
        };
        let msg = compose::media(media, up, quoted);
        self.deliver(&account, &chat, &msg).await
    }

    #[tool(
        description = "Envoie une position (latitude, longitude), avec un nom et une adresse facultatifs.",
        annotations(
            title = "Envoyer une position",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn send_location(
        &self,
        Parameters(p): Parameters<SendLocationParams>,
    ) -> Result<Json<Sent>, String> {
        let account = self.account(p.account.as_deref())?;
        if !(-90.0..=90.0).contains(&p.latitude) || !(-180.0..=180.0).contains(&p.longitude) {
            return Err("coordonnées hors limites".into());
        }
        let chat = self.resolve(&account, p.chat).await?;
        let msg = compose::location(p.latitude, p.longitude, p.name, p.address);
        self.deliver(&account, &chat, &msg).await
    }

    #[tool(
        description = "Crée un sondage (2 à 12 choix, un seul ou plusieurs par personne).",
        annotations(
            title = "Créer un sondage",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn send_poll(
        &self,
        Parameters(p): Parameters<SendPollParams>,
    ) -> Result<Json<Sent>, String> {
        let account = self.account(p.account.as_deref())?;
        let options: Vec<String> = p
            .options
            .into_iter()
            .map(|o| o.trim().to_owned())
            .filter(|o| !o.is_empty())
            .collect();
        if !(2..=12).contains(&options.len()) {
            return Err("un sondage demande de 2 à 12 choix non vides".into());
        }
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret).map_err(|e| format!("aléa indisponible : {e}"))?;
        let chat = self.resolve(&account, p.chat).await?;
        let msg = compose::poll(&p.question, &options, p.multiple, secret);
        self.deliver(&account, &chat, &msg).await
    }

    #[tool(
        description = "Réagit à un message avec un emoji, ou retire sa réaction (emoji vide). Remplace une réaction précédente.",
        annotations(
            title = "Réagir",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn react(&self, Parameters(p): Parameters<ReactParams>) -> Result<Json<Sent>, String> {
        let account = self.account(p.account.as_deref())?;
        let (chat, r) = self.target(&account, p.chat, p.message_id).await?;
        let key = compose::key(&chat, &r.id, r.from_me, &r.sender);
        let msg = compose::reaction(key, p.emoji.trim(), now_ms());
        self.deliver(&account, &chat, &msg).await
    }

    #[tool(
        description = "Modifie le texte d'un de mes messages, dans les 15 minutes qui suivent son envoi. L'ancien texte est remplacé chez tous les destinataires.",
        annotations(
            title = "Modifier un message",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn edit_message(
        &self,
        Parameters(p): Parameters<EditParams>,
    ) -> Result<Json<Sent>, String> {
        let account = self.account(p.account.as_deref())?;
        if p.text.trim().is_empty() {
            return Err("texte vide : pour retirer le message, utiliser delete_message".into());
        }
        let (chat, r) = self.target(&account, p.chat, p.message_id).await?;
        if !r.from_me {
            return Err("seuls mes propres messages peuvent être modifiés".into());
        }
        if r.deleted {
            return Err("message déjà supprimé".into());
        }
        if elapsed_since(r.timestamp_ms) > EDIT_WINDOW {
            return Err(
                "plus de 15 minutes se sont écoulées : WhatsApp refuse la modification".into(),
            );
        }
        let msg = compose::edit(&chat, &r.id, &p.text, now_ms());
        self.deliver(&account, &chat, &msg).await
    }

    #[tool(
        description = "Supprime un message pour tout le monde : les miens pendant environ deux jours et demi, ceux des autres seulement si je suis administrateur du groupe. Irréversible.",
        annotations(
            title = "Supprimer pour tous",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn delete_message(
        &self,
        Parameters(p): Parameters<MessageParam>,
    ) -> Result<Json<Sent>, String> {
        let account = self.account(p.account.as_deref())?;
        let (chat, r) = self.target(&account, p.chat, p.message_id).await?;
        if r.deleted {
            return Err("message déjà supprimé".into());
        }
        if !r.from_me && !chat.ends_with("@g.us") {
            return Err("dans une discussion individuelle, seuls mes propres messages peuvent être supprimés pour tous".into());
        }
        if r.from_me && elapsed_since(r.timestamp_ms) > REVOKE_WINDOW {
            return Err("message trop ancien : WhatsApp refuse la suppression pour tous".into());
        }
        let key = compose::key(&chat, &r.id, r.from_me, &r.sender);
        let msg = compose::revoke(key);
        self.deliver(&account, &chat, &msg).await
    }

    #[tool(
        description = "Marque une discussion comme lue : envoie les accusés de lecture (coches bleues) des derniers messages reçus.",
        annotations(
            title = "Marquer comme lu",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn mark_read(&self, Parameters(p): Parameters<ChatOnly>) -> Result<Json<Done>, String> {
        let account = self.account(p.account.as_deref())?;
        let (chat, incoming) = self
            .read(&account, move |cx| {
                let jid = query::resolve_chat(cx, &p.chat)?;
                let incoming = query::last_incoming(cx, &jid, 50)?;
                Ok((jid, incoming))
            })
            .await?;
        if incoming.is_empty() {
            return Ok(Json(Done {
                ok: true,
                detail: Some("aucun message reçu dans cette discussion".into()),
            }));
        }
        // Un accusé par auteur dans un groupe ; l'auteur est omis en individuel.
        let mut by_sender: HashMap<String, Vec<String>> = HashMap::new();
        for (id, sender) in incoming {
            let key = if chat.ends_with("@g.us") {
                sender
            } else {
                String::new()
            };
            by_sender.entry(key).or_default().push(id);
        }
        let n: usize = by_sender.values().map(Vec::len).sum();
        for (sender, ids) in by_sender {
            self.bridge
                .mark_read(&account, &chat, &sender, ids)
                .await
                .map_err(|e| format!("accusé de lecture impossible : {e}"))?;
        }
        Ok(Json(Done {
            ok: true,
            detail: Some(format!("{n} messages marqués comme lus")),
        }))
    }

    #[tool(
        description = "Affiche « en train d'écrire » ou « enregistre un vocal » dans une discussion, ou l'arrête (paused). À utiliser juste avant un envoi.",
        annotations(
            title = "Présence dans une discussion",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn send_presence(
        &self,
        Parameters(p): Parameters<PresenceParams>,
    ) -> Result<Json<Done>, String> {
        let account = self.account(p.account.as_deref())?;
        let chat = self.resolve(&account, p.chat).await?;
        self.bridge
            .chat_presence(&account, &chat, &p.state)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Json(Done {
            ok: true,
            detail: None,
        }))
    }

    #[tool(
        description = "Vérifie si des numéros ont un compte WhatsApp, sans rien leur envoyer.",
        annotations(
            title = "Numéros sur WhatsApp ?",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn check_whatsapp(
        &self,
        Parameters(p): Parameters<CheckParams>,
    ) -> Result<Json<Checks>, String> {
        let account = self.account(p.account.as_deref())?;
        if p.phones.is_empty() || p.phones.len() > 50 {
            return Err("de 1 à 50 numéros".into());
        }
        let phones: Vec<String> = p
            .phones
            .iter()
            .map(|n| n.chars().filter(char::is_ascii_digit).collect())
            .collect();
        let res = self
            .bridge
            .check_phones(&account, phones)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Json(Checks {
            results: res
                .results
                .into_iter()
                .map(|r| Check {
                    phone: format!("+{}", r.phone),
                    on_whatsapp: r.on_whatsapp,
                    jid: Some(r.jid).filter(|j| !j.is_empty()),
                })
                .collect(),
        }))
    }
}

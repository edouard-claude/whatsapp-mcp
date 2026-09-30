//! Outils de gestion (P4) : groupes et communautés, réglages de discussion,
//! profil, blocage, confidentialité, appairage et déconnexion de comptes.

use rmcp::handler::server::wrapper::Parameters;
use rmcp::{Json, tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use wa_proto::wabridge::v1 as pb;
use wa_proto::wabridge::v1::account_command::Op as AccOp;
use wa_proto::wabridge::v1::group_command::Op as GroupOp;
use wa_proto::wabridge::v1::reply::Payload;

use super::WaServer;
use crate::domain::AccountAlias;
use crate::query;
use crate::store::Op;

// ------------------------------------------------------------------ paramètres

#[derive(Debug, Deserialize, JsonSchema)]
struct CreateGroupParams {
    #[serde(default)]
    account: Option<String>,
    name: String,
    /// Membres (JID, numéro ou nom). Vide pour une communauté.
    #[serde(default)]
    participants: Vec<String>,
    /// Créer une communauté plutôt qu'un groupe.
    #[serde(default)]
    community: bool,
    /// Communauté à laquelle rattacher le nouveau groupe.
    #[serde(default)]
    parent_community: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct UpdateGroupParams {
    #[serde(default)]
    account: Option<String>,
    /// Groupe : JID ou nom.
    chat: String,
    #[serde(default)]
    name: Option<String>,
    /// Description du groupe.
    #[serde(default)]
    topic: Option<String>,
    /// Seuls les administrateurs peuvent écrire.
    #[serde(default)]
    announce: Option<bool>,
    /// Seuls les administrateurs peuvent modifier nom, description et photo.
    #[serde(default)]
    locked: Option<bool>,
    /// Les nouveaux membres doivent être approuvés par un administrateur.
    #[serde(default)]
    join_approval: Option<bool>,
    /// Messages éphémères : `off`, `24h`, `7d` ou `90d`.
    #[serde(default)]
    ephemeral: Option<String>,
    /// Chemin absolu d'une image JPEG pour la photo du groupe.
    #[serde(default)]
    photo_path: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum ParticipantAction {
    Add,
    Remove,
    Promote,
    Demote,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ParticipantsParams {
    #[serde(default)]
    account: Option<String>,
    chat: String,
    /// `add`, `remove`, `promote` (administrateur) ou `demote`.
    action: ParticipantAction,
    /// Personnes visées : JID, numéro ou nom.
    participants: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct GroupRef {
    #[serde(default)]
    account: Option<String>,
    chat: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct InviteParams {
    #[serde(default)]
    account: Option<String>,
    chat: String,
    /// Invalider l'ancien lien et en créer un nouveau.
    #[serde(default)]
    reset: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct LinkParams {
    #[serde(default)]
    account: Option<String>,
    /// Lien `https://chat.whatsapp.com/…` ou code seul.
    link: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct RequestsParams {
    #[serde(default)]
    account: Option<String>,
    chat: String,
    approve: bool,
    /// Demandeurs : JID ou numéro (voir `list_join_requests`).
    participants: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct CommunityParams {
    #[serde(default)]
    account: Option<String>,
    community: String,
    group: String,
    /// Détacher au lieu de rattacher.
    #[serde(default)]
    unlink: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct UpdateChatParams {
    #[serde(default)]
    account: Option<String>,
    chat: String,
    #[serde(default)]
    archive: Option<bool>,
    #[serde(default)]
    pin: Option<bool>,
    /// `8h`, `1w`, `always` ou `off`.
    #[serde(default)]
    mute: Option<String>,
    /// Marquer comme non lu (`true`) ou lu (`false`), sans accusé de lecture.
    #[serde(default)]
    unread: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ContactRef {
    #[serde(default)]
    account: Option<String>,
    /// JID, numéro ou nom.
    contact: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct AboutParams {
    #[serde(default)]
    account: Option<String>,
    /// Nouveau texte « Infos » du profil.
    about: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum BlockAction {
    List,
    Block,
    Unblock,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct BlockParams {
    #[serde(default)]
    account: Option<String>,
    action: BlockAction,
    /// Obligatoire pour `block` et `unblock`.
    #[serde(default)]
    contact: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct PrivacyParams {
    #[serde(default)]
    account: Option<String>,
    /// Réglage à modifier : `last`, `online`, `profile`, `status`, `readreceipts`,
    /// `groupadd`, `calladd`, `messages`. Absent : lecture seule.
    #[serde(default)]
    setting: Option<String>,
    /// `all`, `contacts`, `contact_blacklist`, `none`, `match_last_seen`, `known`.
    #[serde(default)]
    value: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct PairParams {
    /// Alias du nouveau compte : minuscules, chiffres, `-` et `_`.
    account: String,
    /// Numéro international du téléphone à lier (`33612345678`).
    phone: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct LogoutParams {
    account: String,
}

// ------------------------------------------------------------------ sorties

#[derive(Debug, Serialize, JsonSchema)]
struct Done {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

fn done(detail: impl Into<String>) -> Result<Json<Done>, String> {
    Ok(Json(Done {
        ok: true,
        detail: Some(detail.into()),
    }))
}

#[derive(Debug, Serialize, JsonSchema)]
struct GroupOut {
    jid: String,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic: Option<String>,
    participants: usize,
    is_community: bool,
}

impl From<pb::Group> for GroupOut {
    fn from(g: pb::Group) -> Self {
        GroupOut {
            jid: g.jid,
            name: g.name,
            topic: Some(g.topic).filter(|t| !t.is_empty()),
            participants: g.participants.len(),
            is_community: g.is_community,
        }
    }
}

#[derive(Debug, Serialize, JsonSchema)]
struct ParticipantOut {
    jid: String,
    ok: bool,
    /// Explication d'un échec (confidentialité, déjà membre…).
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    requested_at: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct ParticipantsOut {
    results: Vec<ParticipantOut>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct TextOut {
    value: String,
}

#[derive(Debug, Serialize, JsonSchema)]
struct Profile {
    jid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    about: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    picture_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    verified_name: Option<String>,
    /// Appareils liés au compte.
    devices: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    business: Option<std::collections::BTreeMap<String, String>>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct Blocked {
    blocked: Vec<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct Privacy {
    settings: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Serialize, JsonSchema)]
struct Pairing {
    account: String,
    /// À saisir sur le téléphone : WhatsApp > Appareils connectés > Connecter un
    /// appareil > Connecter avec le numéro de téléphone. Valable environ 2 minutes.
    code: String,
}

fn participant_error(code: i32) -> Option<String> {
    match code {
        0 => None,
        403 => Some(
            "refusé : sa confidentialité interdit de l'ajouter, une invitation est nécessaire"
                .into(),
        ),
        408 => Some("a quitté récemment le groupe, ajout impossible pour l'instant".into()),
        409 => Some("déjà membre".into()),
        401 => Some("m'a bloqué".into()),
        404 => Some("pas de compte WhatsApp".into()),
        500 => Some("groupe plein".into()),
        other => Some(format!("erreur WhatsApp {other}")),
    }
}

// ------------------------------------------------------------------ mécanique commune

impl WaServer {
    async fn group_op(
        &self,
        account: &AccountAlias,
        jid: &str,
        op: GroupOp,
    ) -> Result<Option<Payload>, String> {
        let res = self
            .bridge
            .group(account, jid, op)
            .await
            .map_err(|e| e.to_string())?;
        // Rafraîchit la fiche du groupe en base sans attendre la notification.
        if !jid.is_empty()
            && let Ok(g) = self.bridge.get_groups(account, vec![jid.to_owned()]).await
        {
            let _ = self.store.apply(account.clone(), Op::Groups(g)).await;
        }
        Ok(res)
    }

    async fn account_op(
        &self,
        account: &AccountAlias,
        op: AccOp,
    ) -> Result<Option<Payload>, String> {
        self.bridge
            .account_command(account, op)
            .await
            .map_err(|e| e.to_string())
    }

    async fn resolve_many(
        &self,
        account: &AccountAlias,
        who: Vec<String>,
    ) -> Result<Vec<String>, String> {
        if who.is_empty() {
            return Err("liste de personnes vide".into());
        }
        let mut out = Vec::with_capacity(who.len());
        for w in who {
            out.push(self.resolve(account, w).await?);
        }
        Ok(out)
    }

    async fn resolve_group(&self, account: &AccountAlias, chat: String) -> Result<String, String> {
        let jid = self.resolve(account, chat).await?;
        if !jid.ends_with("@g.us") {
            return Err(format!("{jid} n'est pas un groupe"));
        }
        Ok(jid)
    }

    fn participants_out(&self, payload: Option<Payload>) -> Result<Json<ParticipantsOut>, String> {
        let Some(Payload::Participants(p)) = payload else {
            return Err("réponse inattendue du bridge".into());
        };
        Ok(Json(ParticipantsOut {
            results: p
                .results
                .into_iter()
                .map(|r| ParticipantOut {
                    ok: r.error == 0,
                    error: participant_error(r.error),
                    requested_at: (r.requested_at_ms > 0).then(|| {
                        jiff::Timestamp::from_millisecond(r.requested_at_ms)
                            .map(|t| {
                                t.to_zoned(self.tz.clone())
                                    .strftime("%Y-%m-%d %H:%M")
                                    .to_string()
                            })
                            .unwrap_or_default()
                    }),
                    jid: r.jid,
                })
                .collect(),
        }))
    }
}

fn ephemeral_seconds(s: &str) -> Result<u32, String> {
    Ok(match s.trim() {
        "off" | "0" => 0,
        "24h" | "1d" => 86_400,
        "7d" | "1w" => 604_800,
        "90d" => 7_776_000,
        other => return Err(format!("durée éphémère {other:?} : off, 24h, 7d ou 90d")),
    })
}

fn mute_until(s: &str) -> Result<i64, String> {
    let now = jiff::Timestamp::now().as_millisecond();
    Ok(match s.trim() {
        "off" => 0,
        "always" => -1,
        "8h" => now + 8 * 3_600_000,
        "1w" | "7d" => now + 7 * 86_400_000,
        other => return Err(format!("durée {other:?} : 8h, 1w, always ou off")),
    })
}

// ------------------------------------------------------------------ outils

#[tool_router(router = manage_router, vis = "pub(super)")]
impl WaServer {
    #[tool(
        description = "Crée un groupe (avec ses membres) ou une communauté ; un groupe peut être rattaché d'emblée à une communauté.",
        annotations(
            title = "Créer un groupe",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn create_group(
        &self,
        Parameters(p): Parameters<CreateGroupParams>,
    ) -> Result<Json<GroupOut>, String> {
        let account = self.account(p.account.as_deref())?;
        if p.name.trim().is_empty() {
            return Err("nom vide".into());
        }
        let participants = if p.participants.is_empty() {
            Vec::new()
        } else {
            self.resolve_many(&account, p.participants).await?
        };
        let parent = match p.parent_community {
            Some(c) => self.resolve_group(&account, c).await?,
            None => String::new(),
        };
        let op = GroupOp::Create(pb::GroupCreate {
            name: p.name,
            participants,
            community: p.community,
            parent,
        });
        match self.group_op(&account, "", op).await? {
            Some(Payload::Groups(mut g)) if !g.groups.is_empty() => {
                let created = g.groups.remove(0);
                let _ = self
                    .store
                    .apply(
                        account.clone(),
                        Op::Groups(pb::Groups {
                            groups: vec![created.clone()],
                        }),
                    )
                    .await;
                Ok(Json(created.into()))
            }
            _ => Err("groupe créé mais réponse inattendue".into()),
        }
    }

    #[tool(
        description = "Modifie un groupe : nom, description, qui peut écrire, qui peut le modifier, approbation des nouveaux membres, messages éphémères, photo. Il faut en être administrateur.",
        annotations(
            title = "Modifier un groupe",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn update_group(
        &self,
        Parameters(p): Parameters<UpdateGroupParams>,
    ) -> Result<Json<Done>, String> {
        let account = self.account(p.account.as_deref())?;
        let jid = self.resolve_group(&account, p.chat).await?;
        let mut ops = Vec::new();
        if let Some(n) = p.name {
            ops.push(GroupOp::SetName(n));
        }
        if let Some(t) = p.topic {
            ops.push(GroupOp::SetTopic(t));
        }
        if let Some(a) = p.announce {
            ops.push(GroupOp::SetAnnounce(a));
        }
        if let Some(l) = p.locked {
            ops.push(GroupOp::SetLocked(l));
        }
        if let Some(j) = p.join_approval {
            ops.push(GroupOp::SetJoinApproval(j));
        }
        if let Some(e) = p.ephemeral {
            ops.push(GroupOp::SetEphemeralSeconds(ephemeral_seconds(&e)?));
        }
        if let Some(path) = p.photo_path {
            if !std::path::Path::new(&path).is_absolute() {
                return Err("chemin absolu attendu pour la photo".into());
            }
            ops.push(GroupOp::SetPhotoPath(path));
        }
        if ops.is_empty() {
            return Err("aucune modification demandée".into());
        }
        let n = ops.len();
        for op in ops {
            self.group_op(&account, &jid, op).await?;
        }
        done(format!("{n} réglage(s) appliqué(s)"))
    }

    #[tool(
        description = "Ajoute, retire, nomme administrateur ou rétrograde des membres d'un groupe. Le résultat est donné personne par personne.",
        annotations(
            title = "Membres d'un groupe",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn update_group_participants(
        &self,
        Parameters(p): Parameters<ParticipantsParams>,
    ) -> Result<Json<ParticipantsOut>, String> {
        let account = self.account(p.account.as_deref())?;
        let jid = self.resolve_group(&account, p.chat).await?;
        let participants = self.resolve_many(&account, p.participants).await?;
        let action = match p.action {
            ParticipantAction::Add => "add",
            ParticipantAction::Remove => "remove",
            ParticipantAction::Promote => "promote",
            ParticipantAction::Demote => "demote",
        };
        let op = GroupOp::Participants(pb::ParticipantsChange {
            action: action.into(),
            participants,
        });
        let res = self.group_op(&account, &jid, op).await?;
        self.participants_out(res)
    }

    #[tool(
        description = "Lien d'invitation d'un groupe (il faut en être administrateur) ; `reset` invalide l'ancien lien.",
        annotations(
            title = "Lien d'invitation",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn group_invite_link(
        &self,
        Parameters(p): Parameters<InviteParams>,
    ) -> Result<Json<TextOut>, String> {
        let account = self.account(p.account.as_deref())?;
        let jid = self.resolve_group(&account, p.chat).await?;
        match self
            .bridge
            .group(&account, &jid, GroupOp::InviteLink(p.reset))
            .await
            .map_err(|e| e.to_string())?
        {
            Some(Payload::Text(t)) => Ok(Json(TextOut { value: t })),
            _ => Err("réponse inattendue du bridge".into()),
        }
    }

    #[tool(
        description = "Aperçu d'un groupe à partir de son lien d'invitation, sans le rejoindre.",
        annotations(
            title = "Aperçu d'invitation",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn preview_group_link(
        &self,
        Parameters(p): Parameters<LinkParams>,
    ) -> Result<Json<GroupOut>, String> {
        let account = self.account(p.account.as_deref())?;
        match self
            .bridge
            .group(&account, "", GroupOp::PreviewLink(p.link))
            .await
            .map_err(|e| e.to_string())?
        {
            Some(Payload::Groups(mut g)) if !g.groups.is_empty() => {
                Ok(Json(g.groups.remove(0).into()))
            }
            _ => Err("réponse inattendue du bridge".into()),
        }
    }

    #[tool(
        description = "Rejoint un groupe par son lien d'invitation.",
        annotations(
            title = "Rejoindre un groupe",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn join_group(
        &self,
        Parameters(p): Parameters<LinkParams>,
    ) -> Result<Json<TextOut>, String> {
        let account = self.account(p.account.as_deref())?;
        match self
            .bridge
            .group(&account, "", GroupOp::JoinLink(p.link))
            .await
            .map_err(|e| e.to_string())?
        {
            Some(Payload::Text(jid)) => {
                if let Ok(g) = self.bridge.get_groups(&account, vec![jid.clone()]).await {
                    let _ = self.store.apply(account.clone(), Op::Groups(g)).await;
                }
                Ok(Json(TextOut { value: jid }))
            }
            _ => Err("réponse inattendue du bridge".into()),
        }
    }

    #[tool(
        description = "Demandes d'adhésion en attente d'un groupe à approbation (il faut en être administrateur).",
        annotations(
            title = "Demandes d'adhésion",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn list_join_requests(
        &self,
        Parameters(p): Parameters<GroupRef>,
    ) -> Result<Json<ParticipantsOut>, String> {
        let account = self.account(p.account.as_deref())?;
        let jid = self.resolve_group(&account, p.chat).await?;
        let res = self
            .bridge
            .group(&account, &jid, GroupOp::ListRequests(true))
            .await
            .map_err(|e| e.to_string())?;
        self.participants_out(res)
    }

    #[tool(
        description = "Accepte ou refuse des demandes d'adhésion à un groupe.",
        annotations(
            title = "Traiter des demandes d'adhésion",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn handle_join_requests(
        &self,
        Parameters(p): Parameters<RequestsParams>,
    ) -> Result<Json<ParticipantsOut>, String> {
        let account = self.account(p.account.as_deref())?;
        let jid = self.resolve_group(&account, p.chat).await?;
        let participants = self.resolve_many(&account, p.participants).await?;
        let op = GroupOp::Requests(pb::RequestsChange {
            approve: p.approve,
            participants,
        });
        let res = self.group_op(&account, &jid, op).await?;
        self.participants_out(res)
    }

    #[tool(
        description = "Quitte un groupe. Pour y revenir, il faudra y être réinvité.",
        annotations(
            title = "Quitter un groupe",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn leave_group(&self, Parameters(p): Parameters<GroupRef>) -> Result<Json<Done>, String> {
        let account = self.account(p.account.as_deref())?;
        let jid = self.resolve_group(&account, p.chat).await?;
        self.bridge
            .group(&account, &jid, GroupOp::Leave(true))
            .await
            .map_err(|e| e.to_string())?;
        done(format!("groupe {jid} quitté"))
    }

    #[tool(
        description = "Rattache un groupe existant à une communauté, ou l'en détache (`unlink`).",
        annotations(
            title = "Communauté",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn link_to_community(
        &self,
        Parameters(p): Parameters<CommunityParams>,
    ) -> Result<Json<Done>, String> {
        let account = self.account(p.account.as_deref())?;
        let community = self.resolve_group(&account, p.community).await?;
        let group = self.resolve_group(&account, p.group).await?;
        let op = if p.unlink {
            GroupOp::UnlinkChild(group.clone())
        } else {
            GroupOp::LinkChild(group.clone())
        };
        self.group_op(&account, &community, op).await?;
        done(if p.unlink {
            "groupe détaché"
        } else {
            "groupe rattaché"
        })
    }

    #[tool(
        description = "Réglages d'une discussion, synchronisés sur tous mes appareils : archiver, épingler, mettre en sourdine, marquer comme lue ou non lue.",
        annotations(
            title = "Réglages d'une discussion",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn update_chat(
        &self,
        Parameters(p): Parameters<UpdateChatParams>,
    ) -> Result<Json<Done>, String> {
        let account = self.account(p.account.as_deref())?;
        let chat = p.chat;
        let (jid, last) = self
            .read(&account, move |cx| {
                let jid = query::resolve_chat(cx, &chat)?;
                let last = query::last_message(cx, &jid)?;
                Ok((jid, last))
            })
            .await?;
        let mut ops = Vec::new();
        if let Some(a) = p.archive {
            ops.push(pb::chat_settings::Op::Archive(a));
        }
        if let Some(x) = p.pin {
            ops.push(pb::chat_settings::Op::Pin(x));
        }
        if let Some(m) = p.mute {
            ops.push(pb::chat_settings::Op::MuteUntilMs(mute_until(&m)?));
        }
        if let Some(u) = p.unread {
            ops.push(pb::chat_settings::Op::MarkRead(!u));
        }
        if ops.is_empty() {
            return Err("aucun réglage demandé".into());
        }
        let last = last.unwrap_or_default();
        for op in ops {
            let changed = pb::ChatSettingsChanged {
                account: account.to_string(),
                chat: jid.clone(),
                archived: match op {
                    pb::chat_settings::Op::Archive(a) => Some(a),
                    _ => None,
                },
                pinned: match op {
                    pb::chat_settings::Op::Pin(x) => Some(x),
                    _ => None,
                },
                mute_end_ms: match op {
                    pb::chat_settings::Op::MuteUntilMs(m) => Some(m),
                    _ => None,
                },
                read: match op {
                    pb::chat_settings::Op::MarkRead(r) => Some(r),
                    _ => None,
                },
            };
            self.bridge
                .chat_settings(pb::ChatSettings {
                    account: account.to_string(),
                    chat: jid.clone(),
                    last_id: last.id.clone(),
                    last_from_me: last.from_me,
                    last_sender: last.sender.clone(),
                    last_ts_ms: last.timestamp_ms,
                    op: Some(op),
                })
                .await
                .map_err(|e| e.to_string())?;
            let _ = self
                .store
                .apply(account.clone(), Op::ChatSettings(changed))
                .await;
        }
        done("réglages appliqués")
    }

    #[tool(
        description = "Profil public d'un contact : texte « Infos », photo, nom vérifié, nombre d'appareils, et fiche entreprise s'il en a une.",
        annotations(
            title = "Profil d'un contact",
            read_only_hint = true,
            open_world_hint = true
        )
    )]
    async fn get_profile(
        &self,
        Parameters(p): Parameters<ContactRef>,
    ) -> Result<Json<Profile>, String> {
        let account = self.account(p.account.as_deref())?;
        let jid = self.resolve(&account, p.contact).await?;
        let user = match self
            .account_op(
                &account,
                AccOp::UserInfo(pb::StringList {
                    values: vec![jid.clone()],
                }),
            )
            .await?
        {
            Some(Payload::Users(mut u)) if !u.users.is_empty() => Some(u.users.remove(0)),
            _ => None,
        };
        let picture = match self.account_op(&account, AccOp::Picture(jid.clone())).await {
            Ok(Some(Payload::Text(t))) if !t.is_empty() => Some(t),
            _ => None,
        };
        let business = if user.as_ref().is_some_and(|u| !u.verified_name.is_empty()) {
            match self
                .account_op(&account, AccOp::Business(jid.clone()))
                .await
            {
                Ok(Some(Payload::KeyValues(kv))) => Some(
                    kv.values
                        .into_iter()
                        .filter(|(_, v)| !v.is_empty())
                        .collect(),
                ),
                _ => None,
            }
        } else {
            None
        };
        let user = user.unwrap_or_default();
        Ok(Json(Profile {
            jid,
            about: Some(user.about.trim().to_owned()).filter(|a| !a.is_empty()),
            picture_url: picture,
            verified_name: Some(user.verified_name).filter(|v| !v.is_empty()),
            devices: user.devices,
            business,
        }))
    }

    #[tool(
        description = "Change le texte « Infos » de mon profil.",
        annotations(
            title = "Mon texte Infos",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn set_about(
        &self,
        Parameters(p): Parameters<AboutParams>,
    ) -> Result<Json<Done>, String> {
        let account = self.account(p.account.as_deref())?;
        self.account_op(&account, AccOp::SetAbout(p.about)).await?;
        done("texte Infos mis à jour")
    }

    #[tool(
        description = "Liste des contacts bloqués, ou blocage / déblocage d'un contact.",
        annotations(
            title = "Contacts bloqués",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn blocklist(
        &self,
        Parameters(p): Parameters<BlockParams>,
    ) -> Result<Json<Blocked>, String> {
        let account = self.account(p.account.as_deref())?;
        let op = match p.action {
            BlockAction::List => AccOp::GetBlocklist(true),
            BlockAction::Block | BlockAction::Unblock => {
                let who = p
                    .contact
                    .ok_or("contact obligatoire pour bloquer ou débloquer")?;
                let jid = self.resolve(&account, who).await?;
                if matches!(p.action, BlockAction::Block) {
                    AccOp::Block(jid)
                } else {
                    AccOp::Unblock(jid)
                }
            }
        };
        match self.account_op(&account, op).await? {
            Some(Payload::Strings(s)) => {
                // Numéro plutôt que LID quand la correspondance est connue.
                let mut blocked = Vec::with_capacity(s.values.len());
                for j in s.values {
                    blocked.push(self.resolve(&account, j.clone()).await.unwrap_or(j));
                }
                Ok(Json(Blocked { blocked }))
            }
            _ => Err("réponse inattendue du bridge".into()),
        }
    }

    #[tool(
        description = "Réglages de confidentialité (vu à, en ligne, photo, statut, confirmations de lecture, qui peut m'ajouter à un groupe…), en lecture ou pour en modifier un.",
        annotations(
            title = "Confidentialité",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn privacy_settings(
        &self,
        Parameters(p): Parameters<PrivacyParams>,
    ) -> Result<Json<Privacy>, String> {
        let account = self.account(p.account.as_deref())?;
        let op = match (p.setting, p.value) {
            (Some(k), Some(v)) => AccOp::SetPrivacy(pb::KeyValue { key: k, value: v }),
            (None, None) => AccOp::GetPrivacy(true),
            _ => return Err("préciser à la fois `setting` et `value`, ou aucun des deux".into()),
        };
        match self.account_op(&account, op).await? {
            Some(Payload::KeyValues(kv)) => Ok(Json(Privacy {
                settings: kv
                    .values
                    .into_iter()
                    .filter(|(_, v)| !v.is_empty())
                    .collect(),
            })),
            _ => Err("réponse inattendue du bridge".into()),
        }
    }

    #[tool(
        description = "Lie un nouveau compte WhatsApp à ce serveur : rend un code à 8 caractères à saisir sur le téléphone. L'historique arrive ensuite en quelques minutes.",
        annotations(
            title = "Ajouter un compte",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn session_pair(
        &self,
        Parameters(p): Parameters<PairParams>,
    ) -> Result<Json<Pairing>, String> {
        let alias: AccountAlias = p.account.parse().map_err(|e| format!("{e}"))?;
        if self.accounts().contains(&alias) {
            return Err(format!("le compte {alias} existe déjà"));
        }
        let phone: String = p.phone.chars().filter(char::is_ascii_digit).collect();
        if phone.len() < 7 || phone.starts_with('0') {
            return Err("numéro international attendu, sans 0 initial (ex. 33612345678)".into());
        }
        let lock = crate::lock::lock(&self.data_dir, &alias).map_err(|e| e.to_string())?;
        self.store
            .apply(alias.clone(), Op::Open)
            .await
            .map_err(|e| e.to_string())?;
        let code = self
            .bridge
            .pair_with_code(&alias, &phone)
            .await
            .map_err(|e| format!("appairage impossible : {e}"))?;
        self.locks.lock().push(lock);
        self.accounts.write().push(alias.clone());
        Ok(Json(Pairing {
            account: alias.to_string(),
            code,
        }))
    }

    #[tool(
        description = "Délie un compte : la session est fermée côté WhatsApp (l'appareil disparaît du téléphone). Les messages déjà stockés restent sur disque. Irréversible sans nouvel appairage.",
        annotations(
            title = "Délier un compte",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = true
        )
    )]
    async fn session_logout(
        &self,
        Parameters(p): Parameters<LogoutParams>,
    ) -> Result<Json<Done>, String> {
        let account = self.account(Some(&p.account))?;
        self.bridge
            .logout(&account)
            .await
            .map_err(|e| e.to_string())?;
        self.accounts.write().retain(|a| a != &account);
        done(format!("compte {account} délié"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(ephemeral_seconds("7d"), Ok(604_800));
        assert!(ephemeral_seconds("3d").is_err());
        assert_eq!(mute_until("off"), Ok(0));
        assert_eq!(mute_until("always"), Ok(-1));
        assert!(mute_until("8h").is_ok_and(|t| t > 0));
    }
}

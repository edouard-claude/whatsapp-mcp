//! Ingestion : chaque événement du bridge devient une opération de stockage, et
//! tout ce qui porte un numéro de séquence est acquitté après le commit.
//!
//! Les données qu'il faut aller chercher (contacts, groupes) le sont par une
//! tâche à part, [`Refresher`], qui regroupe les demandes : la boucle d'événements
//! n'attend jamais le bridge (voir `bridge.rs`).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use anyhow::{Context as _, Result};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use wa_proto::wabridge::v1 as pb;

use crate::bridge::BridgeHandle;
use crate::domain::AccountAlias;
use crate::parse::{self, Parsed};
use crate::store::{MessageIn, Op, Source, Store};

/// Délai de regroupement des rafraîchissements : un historique arrive en dizaines
/// de lots, un seul appel suffit à la fin.
const REFRESH_DEBOUNCE: Duration = Duration::from_secs(5);

pub struct Ingest {
    bridge: BridgeHandle,
    store: Store,
    refresh: mpsc::Sender<Refresh>,
    updates: broadcast::Sender<ChatUpdate>,
}

/// Une discussion a changé (nouveau message, réaction, édition...), une fois en base.
/// Diffusé aux abonnements MCP ; sans abonné, l'envoi ne coûte rien.
#[derive(Debug, Clone)]
pub struct ChatUpdate {
    pub account: AccountAlias,
    /// JID canonique (numéro plutôt que LID quand le bridge fournit les deux).
    pub chat: String,
    /// Vient d'un lot d'historique plutôt que du direct.
    pub history: bool,
}

/// Résumé d'un message pour l'affichage.
pub struct Line {
    pub account: AccountAlias,
    pub chat: String,
    pub who: String,
    pub from_me: bool,
    pub what: String,
}

impl Ingest {
    pub fn new(
        bridge: BridgeHandle,
        store: Store,
        updates: broadcast::Sender<ChatUpdate>,
    ) -> (Ingest, JoinHandle<()>) {
        let (refresh, rx) = mpsc::channel(64);
        let task = tokio::spawn(Refresher::run(bridge.clone(), store.clone(), rx));
        (
            Ingest {
                bridge,
                store,
                refresh,
                updates,
            },
            task,
        )
    }

    fn announce(&self, account: &AccountAlias, chat: String, history: bool) {
        // Aucun abonné : erreur attendue, rien à faire.
        let _ = self.updates.send(ChatUpdate {
            account: account.clone(),
            chat,
            history,
        });
    }

    /// Message en direct. Rend une ligne à afficher, `None` pour la signalisation.
    pub async fn message(&self, m: pb::IncomingMessage) -> Result<Option<Line>> {
        let account: AccountAlias = m.account.parse()?;
        let info = m.info.unwrap_or_default();
        let parsed = parsed(&m.raw, &info.id);
        let line = line(&account, &info, &parsed);
        let chat =
            (!matches!(parsed, Parsed::Plumbing)).then(|| prefer_pn(&info.chat, &info.chat_alt));
        let op = Op::Message(MessageIn {
            info,
            parsed,
            raw: m.raw,
            source: Source::Live,
        });
        if self.persist_then_ack(account.clone(), op, m.seq).await?
            && let Some(chat) = chat
        {
            self.announce(&account, chat, false);
        }
        Ok(line)
    }

    /// Lot d'historique. Rend le nombre de messages du lot.
    pub async fn history(&self, h: pb::HistoryBatch) -> Result<usize> {
        let account: AccountAlias = h.account.parse()?;
        let messages: Vec<MessageIn> = h
            .messages
            .into_iter()
            .map(|hm| {
                let info = hm.info.unwrap_or_default();
                MessageIn {
                    parsed: parsed(&hm.raw, &info.id),
                    info,
                    raw: hm.raw,
                    source: Source::History,
                }
            })
            .collect();
        let n = messages.len();
        let chats: std::collections::BTreeSet<String> = messages
            .iter()
            .filter(|m| !matches!(m.parsed, Parsed::Plumbing))
            .map(|m| prefer_pn(&m.info.chat, &m.info.chat_alt))
            .collect();
        let op = Op::History {
            chats: h.chats,
            messages,
            reactions: h.reactions,
            mappings: h.mappings,
        };
        if self.persist_then_ack(account.clone(), op, h.seq).await? {
            for chat in chats {
                self.announce(&account, chat, true);
            }
        }
        self.ask(Refresh::Contacts(account)).await;
        Ok(n)
    }

    pub async fn receipt(&self, r: pb::Receipt) -> Result<()> {
        let account: AccountAlias = r.account.parse()?;
        self.store.apply(account, Op::Receipt(r)).await?;
        Ok(())
    }

    pub async fn connection(&self, s: pb::ConnectionState) -> Result<()> {
        let account: AccountAlias = s.account.parse()?;
        let connected = s.state() == pb::connection_state::State::Connected;
        self.store.apply(account.clone(), Op::Connection(s)).await?;
        if connected {
            self.ask(Refresh::Contacts(account.clone())).await;
            self.ask(Refresh::AllGroups(account)).await;
        }
        Ok(())
    }

    pub async fn chat_settings(&self, c: pb::ChatSettingsChanged) -> Result<()> {
        let account: AccountAlias = c.account.parse()?;
        self.store.apply(account, Op::ChatSettings(c)).await?;
        Ok(())
    }

    pub async fn group_changed(&self, g: pb::GroupChanged) -> Result<()> {
        let account: AccountAlias = g.account.parse()?;
        self.ask(Refresh::Group(account, g.jid)).await;
        Ok(())
    }

    /// Commit puis Ack. Un échec d'écriture donne un Ack négatif : WhatsApp
    /// redélivrera, rien n'est perdu.
    /// Rend vrai si les données sont en base.
    async fn persist_then_ack(&self, account: AccountAlias, op: Op, seq: u64) -> Result<bool> {
        let ok = match self.store.apply(account, op).await {
            Ok(()) => true,
            Err(e) => {
                tracing::error!(error = %e, seq, "écriture, ack négatif");
                false
            }
        };
        self.bridge.ack(seq, ok).await.context("ack")?;
        Ok(ok)
    }

    async fn ask(&self, r: Refresh) {
        // File pleine : un rafraîchissement du même compte est déjà en route.
        let _ = self.refresh.try_send(r);
    }
}

fn parsed(raw: &[u8], id: &str) -> Parsed {
    match parse::decode(raw) {
        Ok(m) => parse::parse(&m),
        Err(e) => {
            // Le brut est gardé : un décodeur corrigé le relira.
            tracing::warn!(error = %e, id, "message illisible");
            Parsed::Content {
                kind: parse::Kind::Other,
                text: None,
                quoted_id: None,
            }
        }
    }
}

/// Le numéro plutôt que le LID quand le bridge fournit les deux, comme en base.
pub fn prefer_pn(jid: &str, alt: &str) -> String {
    if jid.ends_with("@lid") && alt.ends_with("@s.whatsapp.net") {
        alt.to_owned()
    } else {
        jid.to_owned()
    }
}

fn line(account: &AccountAlias, info: &pb::MessageInfo, p: &Parsed) -> Option<Line> {
    let what = match p {
        Parsed::Plumbing => return None,
        Parsed::Content { kind, text, .. } => {
            format!("({}) {}", kind.as_str(), text.as_deref().unwrap_or(""))
        }
        Parsed::Reaction { target_id, emoji } if emoji.is_empty() => {
            format!("retire sa réaction à {target_id}")
        }
        Parsed::Reaction { target_id, emoji } => format!("réagit {emoji} à {target_id}"),
        Parsed::Edit { target_id, text } => {
            format!("modifie {target_id} : {}", text.as_deref().unwrap_or(""))
        }
        Parsed::Revoke { target_id } => format!("supprime {target_id}"),
    };
    Some(Line {
        account: account.clone(),
        chat: prefer_pn(&info.chat, &info.chat_alt),
        who: if info.push_name.is_empty() {
            prefer_pn(&info.sender, &info.sender_alt)
        } else {
            info.push_name.clone()
        },
        from_me: info.from_me,
        what,
    })
}

// ------------------------------------------------------------ rafraîchissements

enum Refresh {
    Contacts(AccountAlias),
    AllGroups(AccountAlias),
    Group(AccountAlias, String),
}

#[derive(Default)]
struct Pending {
    contacts: bool,
    all_groups: bool,
    groups: BTreeSet<String>,
}

struct Refresher;

impl Refresher {
    async fn run(bridge: BridgeHandle, store: Store, mut rx: mpsc::Receiver<Refresh>) {
        while let Some(first) = rx.recv().await {
            let mut pending: BTreeMap<AccountAlias, Pending> = BTreeMap::new();
            Self::add(&mut pending, first);
            // Regroupe tout ce qui arrive pendant la fenêtre.
            let deadline = tokio::time::sleep(REFRESH_DEBOUNCE);
            tokio::pin!(deadline);
            loop {
                tokio::select! {
                    () = &mut deadline => break,
                    next = rx.recv() => match next {
                        Some(r) => Self::add(&mut pending, r),
                        None => break,
                    },
                }
            }
            for (account, p) in pending {
                if let Err(e) = Self::refresh(&bridge, &store, &account, p).await {
                    tracing::warn!(%account, error = %e, "rafraîchissement");
                }
            }
        }
    }

    fn add(pending: &mut BTreeMap<AccountAlias, Pending>, r: Refresh) {
        match r {
            Refresh::Contacts(a) => pending.entry(a).or_default().contacts = true,
            Refresh::AllGroups(a) => pending.entry(a).or_default().all_groups = true,
            Refresh::Group(a, jid) => {
                pending.entry(a).or_default().groups.insert(jid);
            }
        }
    }

    async fn refresh(
        bridge: &BridgeHandle,
        store: &Store,
        account: &AccountAlias,
        p: Pending,
    ) -> Result<()> {
        if p.contacts {
            let c = bridge.get_contacts(account).await?;
            tracing::info!(%account, contacts = c.contacts.len(), mappings = c.mappings.len(), "contacts");
            store.apply(account.clone(), Op::Contacts(c)).await?;
        }
        let jids = if p.all_groups {
            Some(Vec::new())
        } else if p.groups.is_empty() {
            None
        } else {
            Some(p.groups.into_iter().collect())
        };
        if let Some(jids) = jids {
            let g = bridge.get_groups(account, jids).await?;
            tracing::info!(%account, groups = g.groups.len(), "groupes");
            store.apply(account.clone(), Op::Groups(g)).await?;
        }
        Ok(())
    }
}

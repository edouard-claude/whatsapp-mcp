//! Persistance : un SQLite par compte (`accounts/<alias>/store.db`), un seul thread
//! écrivain.
//!
//! Le thread possède toutes les connexions ; le reste du programme lui envoie des
//! opérations sur un canal borné et attend le commit. C'est ce commit qui autorise
//! l'Ack vers WhatsApp. Chaque opération est une transaction.
//!
//! Identité des contacts : WhatsApp adresse une même personne par son numéro
//! (`…@s.whatsapp.net`) ou par un identifiant opaque (`…@lid`). La base range tout
//! sous le numéro dès que la correspondance est connue, et fusionne les lignes
//! rangées sous le LID au moment où elle l'apprend.

use std::collections::HashMap;
use std::path::PathBuf;

use rusqlite::{Connection, OptionalExtension, Transaction, params};
use tokio::sync::{mpsc, oneshot};
use wa_proto::wabridge::v1 as pb;

use crate::domain::AccountAlias;
use crate::parse::{self, Kind, Parsed};

const SCHEMA_VERSION: i64 = 2;

const SCHEMA_V1: &str = "
CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE lid_map (
    lid TEXT PRIMARY KEY,
    pn  TEXT NOT NULL
) WITHOUT ROWID;
CREATE INDEX lid_map_pn ON lid_map (pn);

CREATE TABLE chats (
    jid             TEXT PRIMARY KEY,
    kind            TEXT NOT NULL,          -- dm | group | broadcast | newsletter
    name            TEXT,
    unread_count    INTEGER NOT NULL DEFAULT 0,
    archived        INTEGER NOT NULL DEFAULT 0,
    pinned          INTEGER NOT NULL DEFAULT 0,
    mute_end_ms     INTEGER NOT NULL DEFAULT 0,
    last_message_ms INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID;
CREATE INDEX chats_last ON chats (last_message_ms DESC);

CREATE TABLE contacts (
    jid            TEXT PRIMARY KEY,
    first_name     TEXT,
    full_name      TEXT,
    push_name      TEXT,
    business_name  TEXT,
    redacted_phone TEXT
) WITHOUT ROWID;

CREATE TABLE groups (
    jid               TEXT PRIMARY KEY,
    name              TEXT,
    topic             TEXT,
    owner             TEXT,
    created_ms        INTEGER,
    announce          INTEGER NOT NULL DEFAULT 0,
    locked            INTEGER NOT NULL DEFAULT 0,
    is_community      INTEGER NOT NULL DEFAULT 0,
    parent            TEXT,
    ephemeral_seconds INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID;

CREATE TABLE group_participants (
    group_jid    TEXT NOT NULL,
    jid          TEXT NOT NULL,
    role         TEXT NOT NULL,
    display_name TEXT,
    PRIMARY KEY (group_jid, jid)
) WITHOUT ROWID;
CREATE INDEX group_participants_jid ON group_participants (jid);

-- rowid explicite : sert de clé au contenu externe de l'index FTS.
CREATE TABLE messages (
    rowid        INTEGER PRIMARY KEY,
    chat         TEXT    NOT NULL,
    id           TEXT    NOT NULL,
    sender       TEXT    NOT NULL,
    from_me      INTEGER NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    push_name    TEXT,
    kind         TEXT    NOT NULL,
    text         TEXT,
    quoted_id    TEXT,
    edited_ms    INTEGER,
    revoked_ms   INTEGER,
    source       TEXT    NOT NULL,          -- live | history
    raw          BLOB    NOT NULL,
    UNIQUE (chat, id)
);
CREATE INDEX messages_chat_time ON messages (chat, timestamp_ms);
CREATE INDEX messages_sender ON messages (sender);

CREATE VIRTUAL TABLE messages_fts USING fts5 (
    text,
    content = 'messages',
    content_rowid = 'rowid',
    tokenize = 'unicode61 remove_diacritics 2'
);
CREATE TRIGGER messages_ai AFTER INSERT ON messages BEGIN
    INSERT INTO messages_fts (rowid, text) VALUES (new.rowid, new.text);
END;
CREATE TRIGGER messages_ad AFTER DELETE ON messages BEGIN
    INSERT INTO messages_fts (messages_fts, rowid, text) VALUES ('delete', old.rowid, old.text);
END;
CREATE TRIGGER messages_au AFTER UPDATE OF text ON messages BEGIN
    INSERT INTO messages_fts (messages_fts, rowid, text) VALUES ('delete', old.rowid, old.text);
    INSERT INTO messages_fts (rowid, text) VALUES (new.rowid, new.text);
END;

CREATE TABLE reactions (
    chat         TEXT NOT NULL,
    target_id    TEXT NOT NULL,
    sender       TEXT NOT NULL,
    emoji        TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    PRIMARY KEY (chat, target_id, sender)
) WITHOUT ROWID;

CREATE TABLE receipts (
    chat         TEXT NOT NULL,
    message_id   TEXT NOT NULL,
    recipient    TEXT NOT NULL,
    type         TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    PRIMARY KEY (chat, message_id, recipient, type)
) WITHOUT ROWID;
";

/// v2 : médias téléchargés, et nom d'affichage d'un contact.
const SCHEMA_V2: &str = "
CREATE TABLE media (
    chat          TEXT    NOT NULL,
    id            TEXT    NOT NULL,
    path          TEXT    NOT NULL,
    mime          TEXT,
    size          INTEGER NOT NULL,
    downloaded_ms INTEGER NOT NULL,
    PRIMARY KEY (chat, id)
) WITHOUT ROWID;

-- Nom choisi par l'utilisateur dans son carnet d'abord, puis celui de l'entreprise,
-- puis celui que le contact s'est donné.
CREATE VIEW contact_names AS
SELECT jid, COALESCE(full_name, first_name, business_name, push_name) AS name
FROM contacts;
";

/// Types d'accusés conservés ; les autres (`retry`, `sender`, `server-error`...)
/// sont de la signalisation.
const KEPT_RECEIPTS: [&str; 5] = ["delivered", "read", "played", "read-self", "played-self"];

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sqlite : {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("répertoire du compte : {0}")]
    Io(#[from] std::io::Error),
    #[error("base créée par une version plus récente (schéma {0})")]
    FutureSchema(i64),
    #[error("le thread d'écriture est arrêté")]
    Stopped,
}

#[derive(Debug, Clone, Copy)]
pub enum Source {
    Live,
    History,
}

impl Source {
    fn as_str(self) -> &'static str {
        match self {
            Source::Live => "live",
            Source::History => "history",
        }
    }
}

pub struct MessageIn {
    pub info: pb::MessageInfo,
    pub parsed: Parsed,
    pub raw: Vec<u8>,
    pub source: Source,
}

/// Une opération, appliquée dans une transaction.
pub enum Op {
    /// Ouvre la base (et la migre) sans rien écrire.
    Open,
    Message(MessageIn),
    History {
        chats: Vec<pb::ChatMeta>,
        messages: Vec<MessageIn>,
        reactions: Vec<pb::Reaction>,
        mappings: Vec<pb::LidMapping>,
    },
    Receipt(pb::Receipt),
    Contacts(pb::Contacts),
    Groups(pb::Groups),
    Connection(pb::ConnectionState),
    ChatSettings(pb::ChatSettingsChanged),
    /// Brut d'un message mis à jour (nouveau chemin de média renvoyé par le téléphone).
    UpdateRaw {
        chat: String,
        id: String,
        raw: Vec<u8>,
    },
    Media {
        chat: String,
        id: String,
        path: String,
        mime: Option<String>,
        size: u64,
    },
}

struct Job {
    account: AccountAlias,
    op: Op,
    at_ms: i64,
    done: oneshot::Sender<Result<(), StoreError>>,
}

#[derive(Clone)]
pub struct Store {
    tx: mpsc::Sender<Job>,
}

impl Store {
    /// Lance le thread écrivain. Il s'arrête quand la dernière poignée est lâchée.
    pub fn open(data_dir: PathBuf) -> (Store, std::thread::JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel::<Job>(128);
        let thread = std::thread::spawn(move || {
            let mut conns: HashMap<AccountAlias, Connection> = HashMap::new();
            while let Some(job) = rx.blocking_recv() {
                let res = connection(&mut conns, &data_dir, &job.account)
                    .and_then(|c| apply(c, job.op, job.at_ms));
                let _ = job.done.send(res);
            }
        });
        (Store { tx }, thread)
    }

    /// Applique l'opération et attend le commit.
    pub async fn apply(&self, account: AccountAlias, op: Op) -> Result<(), StoreError> {
        let (done, rx) = oneshot::channel();
        let at_ms = now_ms();
        self.tx
            .send(Job {
                account,
                op,
                at_ms,
                done,
            })
            .await
            .map_err(|_| StoreError::Stopped)?;
        rx.await.map_err(|_| StoreError::Stopped)?
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

fn connection<'a>(
    conns: &'a mut HashMap<AccountAlias, Connection>,
    data_dir: &std::path::Path,
    account: &AccountAlias,
) -> Result<&'a mut Connection, StoreError> {
    if !conns.contains_key(account) {
        let dir = data_dir.join("accounts").join(account.as_str());
        std::fs::create_dir_all(&dir)?;
        let mut c = Connection::open(dir.join("store.db"))?;
        migrate(&mut c)?;
        let n = reparse_unknown(&mut c)?;
        if n > 0 {
            tracing::info!(%account, messages = n, "messages inconnus relus avec le décodeur actuel");
        }
        conns.insert(account.clone(), c);
    }
    conns.get_mut(account).ok_or(StoreError::Stopped)
}

pub fn migrate(c: &mut Connection) -> Result<(), StoreError> {
    c.execute_batch(
        "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;",
    )?;
    let version: i64 = c.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if version > SCHEMA_VERSION {
        return Err(StoreError::FutureSchema(version));
    }
    if version < 1 {
        let tx = c.transaction()?;
        // Base du spike P0 : table jetable, sans rapport avec ce schéma.
        tx.execute_batch("DROP TABLE IF EXISTS messages;")?;
        tx.execute_batch(SCHEMA_V1)?;
        tx.execute_batch("PRAGMA user_version = 1;")?;
        tx.commit()?;
    }
    if version < 2 {
        let tx = c.transaction()?;
        tx.execute_batch(SCHEMA_V2)?;
        tx.execute_batch("PRAGMA user_version = 2;")?;
        tx.commit()?;
    }
    Ok(())
}

/// Relit les messages restés `other` : le brut est conservé justement pour qu'un
/// décodeur plus récent les reconnaisse. Rend le nombre de lignes reclassées.
pub fn reparse_unknown(c: &mut Connection) -> Result<usize, StoreError> {
    let tx = c.transaction()?;
    let rows: Vec<(i64, Vec<u8>)> = {
        let mut st =
            tx.prepare("SELECT rowid, raw FROM messages WHERE kind = 'other' AND length(raw) > 0")?;
        st.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?
    };
    let mut changed = 0;
    for (rowid, raw) in rows {
        let Ok(msg) = parse::decode(&raw) else {
            continue;
        };
        changed += match parse::parse(&msg) {
            Parsed::Content {
                kind: Kind::Other, ..
            } => 0,
            Parsed::Content {
                kind,
                text,
                quoted_id,
            } => tx.execute(
                "UPDATE messages SET kind = ?1, text = ?2, quoted_id = ?3 WHERE rowid = ?4",
                params![kind.as_str(), text, quoted_id, rowid],
            )?,
            Parsed::Plumbing => tx.execute("DELETE FROM messages WHERE rowid = ?1", [rowid])?,
            // Réaction, édition, suppression : n'ont jamais été classées `other`.
            Parsed::Reaction { .. } | Parsed::Edit { .. } | Parsed::Revoke { .. } => 0,
        };
    }
    tx.commit()?;
    Ok(changed)
}

pub fn apply(c: &mut Connection, op: Op, at_ms: i64) -> Result<(), StoreError> {
    let tx = c.transaction()?;
    match op {
        Op::Open => {}
        Op::Message(m) => put_message(&tx, &m)?,
        Op::History {
            chats,
            messages,
            reactions,
            mappings,
        } => {
            for m in &mappings {
                learn(&tx, &m.lid, &m.pn)?;
            }
            for ch in &chats {
                learn(&tx, &ch.lid, &ch.pn)?;
                put_chat_meta(&tx, ch)?;
            }
            for m in &messages {
                put_message(&tx, m)?;
            }
            for r in &reactions {
                let chat = canon(&tx, &r.chat)?;
                let sender = canon(&tx, &r.sender)?;
                put_reaction(&tx, &chat, &r.target_id, &sender, &r.emoji, r.timestamp_ms)?;
            }
        }
        Op::Receipt(r) => put_receipt(&tx, &r)?,
        Op::Contacts(cs) => {
            for m in &cs.mappings {
                learn(&tx, &m.lid, &m.pn)?;
            }
            for ct in &cs.contacts {
                put_contact(&tx, ct)?;
            }
        }
        Op::Groups(gs) => {
            for g in &gs.groups {
                put_group(&tx, g)?;
            }
        }
        Op::Connection(s) => put_connection(&tx, &s, at_ms)?,
        Op::ChatSettings(c) => {
            let jid = canon(&tx, &c.chat)?;
            // Ligne créée au besoin : un réglage peut précéder le premier message.
            tx.execute(
                "INSERT INTO chats (jid, kind) VALUES (?1, ?2) ON CONFLICT DO NOTHING",
                params![jid, chat_kind(&jid)],
            )?;
            tx.execute(
                "UPDATE chats SET
                     archived     = COALESCE(?2, archived),
                     pinned       = COALESCE(?3, pinned),
                     mute_end_ms  = COALESCE(?4, mute_end_ms),
                     unread_count = CASE WHEN ?5 = 1 THEN 0 WHEN ?5 = 0 THEN max(unread_count, 1) ELSE unread_count END
                 WHERE jid = ?1",
                params![jid, c.archived, c.pinned, c.mute_end_ms, c.read],
            )?;
        }
        Op::UpdateRaw { chat, id, raw } => {
            tx.execute(
                "UPDATE messages SET raw = ?3 WHERE chat = ?1 AND id = ?2 AND revoked_ms IS NULL",
                params![chat, id, raw],
            )?;
        }
        Op::Media {
            chat,
            id,
            path,
            mime,
            size,
        } => {
            tx.execute(
                "INSERT INTO media (chat, id, path, mime, size, downloaded_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT (chat, id) DO UPDATE SET path = excluded.path, mime = excluded.mime,
                     size = excluded.size, downloaded_ms = excluded.downloaded_ms",
                params![chat, id, path, mime, i64::try_from(size).unwrap_or(i64::MAX), at_ms],
            )?;
        }
    }
    tx.commit()?;
    Ok(())
}

// ------------------------------------------------------------------ identité

fn is_lid(j: &str) -> bool {
    j.ends_with("@lid")
}

fn is_pn(j: &str) -> bool {
    j.ends_with("@s.whatsapp.net")
}

/// Forme canonique : le numéro quand la correspondance est connue.
fn canon(tx: &Transaction<'_>, jid: &str) -> rusqlite::Result<String> {
    if !is_lid(jid) {
        return Ok(jid.to_owned());
    }
    let pn: Option<String> = tx
        .query_row("SELECT pn FROM lid_map WHERE lid = ?1", [jid], |r| r.get(0))
        .optional()?;
    Ok(pn.unwrap_or_else(|| jid.to_owned()))
}

/// Enregistre une paire, dans n'importe quel ordre, et fusionne sous le numéro
/// tout ce qui était rangé sous le LID.
fn learn_pair(tx: &Transaction<'_>, a: &str, b: &str) -> rusqlite::Result<()> {
    match (is_lid(a), is_lid(b)) {
        (true, false) => learn(tx, a, b),
        (false, true) => learn(tx, b, a),
        _ => Ok(()),
    }
}

fn learn(tx: &Transaction<'_>, lid: &str, pn: &str) -> rusqlite::Result<()> {
    if !is_lid(lid) || !is_pn(pn) {
        return Ok(());
    }
    let inserted = tx.execute(
        "INSERT INTO lid_map (lid, pn) VALUES (?1, ?2) ON CONFLICT (lid) DO NOTHING",
        [lid, pn],
    )?;
    if inserted == 0 {
        return Ok(());
    }
    // Clés primaires : UPDATE OR IGNORE garde la ligne déjà rangée sous le numéro,
    // le DELETE qui suit retire le doublon resté sous le LID.
    const MOVE: [(&str, &str, bool); 9] = [
        // (table, colonne, fait partie d'une clé unique)
        ("chats", "jid", true),
        ("messages", "chat", true),
        ("messages", "sender", false),
        ("reactions", "chat", true),
        ("reactions", "sender", true),
        ("receipts", "chat", true),
        ("receipts", "recipient", true),
        ("contacts", "jid", true),
        ("group_participants", "jid", true),
    ];
    for (table, col, keyed) in MOVE {
        // Noms de table et de colonne constants : pas d'entrée externe dans ce format!.
        tx.execute(
            &format!("UPDATE OR IGNORE {table} SET {col} = ?1 WHERE {col} = ?2"),
            [pn, lid],
        )?;
        if keyed {
            tx.execute(&format!("DELETE FROM {table} WHERE {col} = ?1"), [lid])?;
        }
    }
    Ok(())
}

fn chat_kind(jid: &str) -> &'static str {
    if jid.ends_with("@g.us") {
        "group"
    } else if jid.ends_with("@broadcast") {
        "broadcast"
    } else if jid.ends_with("@newsletter") {
        "newsletter"
    } else {
        "dm"
    }
}

// ------------------------------------------------------------------ écritures

fn touch_chat(tx: &Transaction<'_>, chat: &str, ts: i64) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO chats (jid, kind, last_message_ms) VALUES (?1, ?2, ?3)
         ON CONFLICT (jid) DO UPDATE SET last_message_ms = max(last_message_ms, excluded.last_message_ms)",
        params![chat, chat_kind(chat), ts],
    )?;
    Ok(())
}

fn put_message(tx: &Transaction<'_>, m: &MessageIn) -> rusqlite::Result<()> {
    let i = &m.info;
    learn_pair(tx, &i.sender, &i.sender_alt)?;
    learn_pair(tx, &i.chat, &i.chat_alt)?;
    let chat = canon(tx, &i.chat)?;
    let sender = canon(tx, &i.sender)?;
    let ts = i.timestamp_ms;

    if !i.from_me && !i.push_name.is_empty() && !sender.ends_with("@g.us") {
        tx.execute(
            "INSERT INTO contacts (jid, push_name) VALUES (?1, ?2)
             ON CONFLICT (jid) DO UPDATE SET push_name = excluded.push_name",
            [&sender, &i.push_name],
        )?;
    }

    match &m.parsed {
        Parsed::Plumbing => {}
        Parsed::Content {
            kind,
            text,
            quoted_id,
        } => {
            touch_chat(tx, &chat, ts)?;
            // Redélivrance ou historique : on ne réécrit ni un texte édité ni un
            // message supprimé pour tous.
            tx.execute(
                "INSERT INTO messages (chat, id, sender, from_me, timestamp_ms, push_name, kind, text, quoted_id, source, raw)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                 ON CONFLICT (chat, id) DO UPDATE SET
                     sender       = excluded.sender,
                     from_me      = excluded.from_me,
                     timestamp_ms = excluded.timestamp_ms,
                     push_name    = COALESCE(excluded.push_name, messages.push_name),
                     kind         = excluded.kind,
                     quoted_id    = excluded.quoted_id,
                     text = CASE WHEN messages.revoked_ms IS NOT NULL THEN NULL
                                 WHEN messages.edited_ms IS NOT NULL THEN messages.text
                                 ELSE excluded.text END,
                     raw  = CASE WHEN messages.revoked_ms IS NOT NULL THEN x'' ELSE excluded.raw END",
                params![
                    chat,
                    i.id,
                    sender,
                    i.from_me,
                    ts,
                    Some(&i.push_name).filter(|p| !p.is_empty()),
                    kind.as_str(),
                    text,
                    quoted_id,
                    m.source.as_str(),
                    m.raw
                ],
            )?;
        }
        Parsed::Reaction { target_id, emoji } => {
            put_reaction(tx, &chat, target_id, &sender, emoji, ts)?;
        }
        Parsed::Edit { target_id, text } => {
            tx.execute(
                "UPDATE messages SET text = ?1, edited_ms = ?2
                 WHERE chat = ?3 AND id = ?4 AND revoked_ms IS NULL",
                params![text, ts, chat, target_id],
            )?;
        }
        Parsed::Revoke { target_id } => {
            // Pierre tombale si l'original n'est pas encore là (ordre de l'historique) :
            // il ne ressuscitera pas en arrivant.
            tx.execute(
                "INSERT INTO messages (chat, id, sender, from_me, timestamp_ms, kind, source, raw, revoked_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'other', ?6, x'', ?5)
                 ON CONFLICT (chat, id) DO UPDATE SET revoked_ms = ?5, text = NULL, raw = x''",
                params![chat, target_id, sender, i.from_me, ts, m.source.as_str()],
            )?;
        }
    }
    Ok(())
}

fn put_reaction(
    tx: &Transaction<'_>,
    chat: &str,
    target_id: &str,
    sender: &str,
    emoji: &str,
    ts: i64,
) -> rusqlite::Result<()> {
    if emoji.is_empty() {
        tx.execute(
            "DELETE FROM reactions WHERE chat = ?1 AND target_id = ?2 AND sender = ?3 AND timestamp_ms <= ?4",
            params![chat, target_id, sender, ts],
        )?;
    } else {
        tx.execute(
            "INSERT INTO reactions (chat, target_id, sender, emoji, timestamp_ms) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (chat, target_id, sender) DO UPDATE SET emoji = excluded.emoji, timestamp_ms = excluded.timestamp_ms
             WHERE excluded.timestamp_ms >= reactions.timestamp_ms",
            params![chat, target_id, sender, emoji, ts],
        )?;
    }
    Ok(())
}

fn put_chat_meta(tx: &Transaction<'_>, ch: &pb::ChatMeta) -> rusqlite::Result<()> {
    let jid = canon(tx, &ch.jid)?;
    tx.execute(
        "INSERT INTO chats (jid, kind, name, unread_count, archived, pinned, mute_end_ms, last_message_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT (jid) DO UPDATE SET
             name            = COALESCE(excluded.name, chats.name),
             unread_count    = excluded.unread_count,
             archived        = excluded.archived,
             pinned          = excluded.pinned,
             mute_end_ms     = excluded.mute_end_ms,
             last_message_ms = max(chats.last_message_ms, excluded.last_message_ms)",
        params![
            jid,
            chat_kind(&jid),
            Some(&ch.name).filter(|n| !n.is_empty()),
            ch.unread_count,
            ch.archived,
            ch.pinned,
            ch.mute_end_ms,
            ch.last_message_ms
        ],
    )?;
    Ok(())
}

fn put_receipt(tx: &Transaction<'_>, r: &pb::Receipt) -> rusqlite::Result<()> {
    if !KEPT_RECEIPTS.contains(&r.r#type.as_str()) {
        return Ok(());
    }
    let chat = canon(tx, &r.chat)?;
    let recipient = canon(tx, &r.sender)?;
    for id in &r.message_ids {
        tx.execute(
            "INSERT INTO receipts (chat, message_id, recipient, type, timestamp_ms) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT DO NOTHING",
            params![chat, id, recipient, r.r#type, r.timestamp_ms],
        )?;
    }
    Ok(())
}

fn put_contact(tx: &Transaction<'_>, c: &pb::Contact) -> rusqlite::Result<()> {
    let jid = canon(tx, &c.jid)?;
    let opt = |s: &String| Some(s.clone()).filter(|s| !s.is_empty());
    tx.execute(
        "INSERT INTO contacts (jid, first_name, full_name, push_name, business_name, redacted_phone)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT (jid) DO UPDATE SET
             first_name     = COALESCE(excluded.first_name, contacts.first_name),
             full_name      = COALESCE(excluded.full_name, contacts.full_name),
             push_name      = COALESCE(excluded.push_name, contacts.push_name),
             business_name  = COALESCE(excluded.business_name, contacts.business_name),
             redacted_phone = COALESCE(excluded.redacted_phone, contacts.redacted_phone)",
        params![
            jid,
            opt(&c.first_name),
            opt(&c.full_name),
            opt(&c.push_name),
            opt(&c.business_name),
            opt(&c.redacted_phone)
        ],
    )?;
    Ok(())
}

fn put_group(tx: &Transaction<'_>, g: &pb::Group) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO groups (jid, name, topic, owner, created_ms, announce, locked, is_community, parent, ephemeral_seconds)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
         ON CONFLICT (jid) DO UPDATE SET
             name = excluded.name, topic = excluded.topic, owner = excluded.owner,
             created_ms = excluded.created_ms, announce = excluded.announce, locked = excluded.locked,
             is_community = excluded.is_community, parent = excluded.parent,
             ephemeral_seconds = excluded.ephemeral_seconds",
        params![
            g.jid,
            g.name,
            g.topic,
            canon(tx, &g.owner)?,
            g.created_ms,
            g.announce,
            g.locked,
            g.is_community,
            Some(&g.parent).filter(|p| !p.is_empty()),
            g.ephemeral_seconds
        ],
    )?;
    tx.execute(
        "INSERT INTO chats (jid, kind, name) VALUES (?1, 'group', ?2)
         ON CONFLICT (jid) DO UPDATE SET name = excluded.name",
        params![g.jid, g.name],
    )?;
    tx.execute(
        "DELETE FROM group_participants WHERE group_jid = ?1",
        [&g.jid],
    )?;
    for p in &g.participants {
        learn_pair(tx, &p.lid, &p.pn)?;
        let jid = canon(tx, &p.jid)?;
        tx.execute(
            "INSERT INTO group_participants (group_jid, jid, role, display_name) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT DO UPDATE SET role = excluded.role, display_name = excluded.display_name",
            params![g.jid, jid, p.role, Some(&p.display_name).filter(|d| !d.is_empty())],
        )?;
    }
    Ok(())
}

fn put_connection(
    tx: &Transaction<'_>,
    s: &pb::ConnectionState,
    at_ms: i64,
) -> rusqlite::Result<()> {
    let state = s.state().as_str_name().to_ascii_lowercase();
    let set = |k: &str, v: &str| {
        tx.execute(
            "INSERT INTO meta (key, value) VALUES (?1, ?2) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            [k, v],
        )
        .map(|_| ())
    };
    set("connection.state", &state)?;
    set("connection.detail", &s.detail)?;
    set("connection.changed_ms", &at_ms.to_string())?;
    if s.state() == pb::connection_state::State::Connected {
        set("connection.last_connected_ms", &at_ms.to_string())?;
        if !s.jid.is_empty() {
            set("own.jid", &s.jid)?;
        }
        if !s.lid.is_empty() {
            set("own.lid", &s.lid)?;
        }
        learn(tx, &s.lid, &s.jid)?;
    }
    if s.until_ms > 0 {
        set("connection.until_ms", &s.until_ms.to_string())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::Kind;

    fn db() -> Connection {
        let mut c = Connection::open_in_memory().expect("sqlite en mémoire");
        migrate(&mut c).expect("migration");
        c
    }

    fn text(chat: &str, id: &str, sender: &str, body: &str, ts: i64) -> Op {
        Op::Message(MessageIn {
            info: pb::MessageInfo {
                id: id.into(),
                chat: chat.into(),
                sender: sender.into(),
                timestamp_ms: ts,
                ..Default::default()
            },
            parsed: Parsed::Content {
                kind: Kind::Text,
                text: Some(body.into()),
                quoted_id: None,
            },
            raw: vec![1],
            source: Source::Live,
        })
    }

    fn one<T: rusqlite::types::FromSql>(c: &Connection, sql: &str) -> T {
        c.query_row(sql, [], |r| r.get(0)).expect(sql)
    }

    const LID: &str = "111@lid";
    const PN: &str = "33600000000@s.whatsapp.net";

    #[test]
    fn redelivery_is_idempotent_and_fts_finds_accents() {
        let mut c = db();
        for _ in 0..2 {
            apply(&mut c, text(PN, "A", PN, "Rendez-vous à l'école", 1), 0).expect("apply");
        }
        assert_eq!(one::<i64>(&c, "SELECT count(*) FROM messages"), 1);
        assert_eq!(
            one::<i64>(
                &c,
                "SELECT count(*) FROM messages_fts WHERE messages_fts MATCH 'ecole'"
            ),
            1
        );
    }

    #[test]
    fn lid_rows_merge_under_phone_number_when_mapping_arrives() {
        let mut c = db();
        apply(&mut c, text(LID, "A", LID, "avant", 1), 0).expect("apply");
        apply(&mut c, text(PN, "B", PN, "déjà sous le numéro", 2), 0).expect("apply");
        apply(
            &mut c,
            Op::Contacts(pb::Contacts {
                contacts: vec![],
                mappings: vec![pb::LidMapping {
                    lid: LID.into(),
                    pn: PN.into(),
                }],
            }),
            0,
        )
        .expect("apply");
        assert_eq!(
            one::<i64>(
                &c,
                &format!("SELECT count(*) FROM messages WHERE chat = '{PN}'")
            ),
            2
        );
        assert_eq!(one::<i64>(&c, "SELECT count(*) FROM chats"), 1);
        // Un message ultérieur adressé au LID atterrit sous le numéro.
        apply(&mut c, text(LID, "C", LID, "après", 3), 0).expect("apply");
        assert_eq!(
            one::<i64>(
                &c,
                &format!("SELECT count(*) FROM messages WHERE chat = '{PN}'")
            ),
            3
        );
    }

    #[test]
    fn edit_and_revoke_survive_redelivery() {
        let mut c = db();
        apply(&mut c, text(PN, "A", PN, "fote", 1), 0).expect("apply");
        let edit = |target: &str, parsed| {
            Op::Message(MessageIn {
                info: pb::MessageInfo {
                    id: format!("E{target}"),
                    chat: PN.into(),
                    sender: PN.into(),
                    timestamp_ms: 5,
                    ..Default::default()
                },
                parsed,
                raw: vec![],
                source: Source::Live,
            })
        };
        apply(
            &mut c,
            edit(
                "A",
                Parsed::Edit {
                    target_id: "A".into(),
                    text: Some("faute".into()),
                },
            ),
            0,
        )
        .expect("apply");
        apply(&mut c, text(PN, "A", PN, "fote", 1), 0).expect("apply");
        assert_eq!(
            one::<String>(&c, "SELECT text FROM messages WHERE id = 'A'"),
            "faute"
        );

        // Suppression reçue avant l'original (ordre de l'historique).
        apply(
            &mut c,
            edit(
                "B",
                Parsed::Revoke {
                    target_id: "B".into(),
                },
            ),
            0,
        )
        .expect("apply");
        apply(&mut c, text(PN, "B", PN, "secret", 1), 0).expect("apply");
        assert_eq!(
            one::<Option<String>>(&c, "SELECT text FROM messages WHERE id = 'B'"),
            None
        );
        assert_eq!(
            one::<i64>(
                &c,
                "SELECT count(*) FROM messages_fts WHERE messages_fts MATCH 'secret'"
            ),
            0
        );
    }
}

/// Relecture sur une copie d'une vraie base : `WA_TEST_DB=… cargo test -- --ignored`.
#[cfg(test)]
#[test]
#[ignore = "demande une copie de base réelle"]
fn reparse_real_copy() {
    let path = std::env::var("WA_TEST_DB").expect("WA_TEST_DB");
    let mut c = Connection::open(path).expect("ouverture");
    let before: i64 = c
        .query_row(
            "SELECT count(*) FROM messages WHERE kind = 'other'",
            [],
            |r| r.get(0),
        )
        .expect("comptage");
    let n = reparse_unknown(&mut c).expect("relecture");
    let after: i64 = c
        .query_row(
            "SELECT count(*) FROM messages WHERE kind = 'other'",
            [],
            |r| r.get(0),
        )
        .expect("comptage");
    println!("other : {before} -> {after} ({n} lignes reclassées ou retirées)");
}

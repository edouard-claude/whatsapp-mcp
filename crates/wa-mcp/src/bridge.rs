//! Process `wa-bridge` : lancement, trames sur ses pipes, corrélation des réponses.
//!
//! Une tâche unique possède le process, ses pipes et la table des requêtes en
//! attente. Le reste du programme lui parle par [`BridgeHandle`] et reçoit les
//! événements sur un canal borné.
//!
//! Règle d'usage : la boucle qui consomme les événements ne doit jamais attendre
//! une réponse de requête elle-même. Si le canal d'événements est plein, la tâche
//! du bridge attend ce consommateur, qui attendrait la tâche : interblocage.
//! Les requêtes partent donc d'une autre tâche.

use std::collections::HashMap;
use std::path::Path;
use std::process::{ExitStatus, Stdio};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use prost::Message as _;
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use wa_proto::wabridge::v1::{self as pb, command, event, reply};

use crate::domain::AccountAlias;

/// Taille maximale d'une trame, identique côté Go.
const MAX_FRAME: usize = 64 << 20;
/// Événements en attente de consommation avant que le bridge ne ralentisse.
const EVENT_BUFFER: usize = 256;

#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("impossible de lancer {path} : {source}")]
    Spawn {
        path: String,
        source: std::io::Error,
    },
    #[error("pipes du bridge : {0}")]
    Io(#[from] std::io::Error),
    #[error("trame illisible du bridge : {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("le bridge s'est arrêté ({0})")]
    Exited(ExitStatus),
    #[error("le bridge a fermé la connexion")]
    Closed,
    #[error("{0}")]
    Remote(String),
}

/// Événement du bridge autre qu'une réponse.
pub type BridgeEvent = event::Kind;

enum Outgoing {
    Request {
        kind: command::Kind,
        reply: oneshot::Sender<pb::Reply>,
    },
    Notify(command::Kind),
}

/// Poignée clonable vers la tâche du bridge. Quand la dernière est lâchée, stdin
/// du bridge se ferme et il s'arrête.
#[derive(Clone)]
pub struct BridgeHandle {
    tx: mpsc::Sender<Outgoing>,
}

pub struct Bridge {
    pub handle: BridgeHandle,
    pub events: mpsc::Receiver<BridgeEvent>,
    pub task: JoinHandle<Result<ExitStatus, BridgeError>>,
}

pub fn spawn(bin: &Path, data_dir: &Path) -> Result<Bridge, BridgeError> {
    let mut child = Command::new(bin)
        .arg("--data-dir")
        .arg(data_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .map_err(|source| BridgeError::Spawn {
            path: bin.display().to_string(),
            source,
        })?;
    let (stdin, stdout) = match (child.stdin.take(), child.stdout.take()) {
        (Some(i), Some(o)) => (i, o),
        _ => return Err(BridgeError::Closed),
    };
    let codec = || {
        LengthDelimitedCodec::builder()
            .length_field_length(4)
            .big_endian()
            .max_frame_length(MAX_FRAME)
            .new_codec()
    };
    let (tx, rx) = mpsc::channel(64);
    let (events_tx, events) = mpsc::channel(EVENT_BUFFER);
    let task = tokio::spawn(run(
        child,
        FramedWrite::new(stdin, codec()),
        FramedRead::new(stdout, codec()),
        rx,
        events_tx,
    ));
    Ok(Bridge {
        handle: BridgeHandle { tx },
        events,
        task,
    })
}

type Sink = FramedWrite<tokio::process::ChildStdin, LengthDelimitedCodec>;

async fn run(
    mut child: Child,
    sink: Sink,
    mut frames: FramedRead<tokio::process::ChildStdout, LengthDelimitedCodec>,
    mut rx: mpsc::Receiver<Outgoing>,
    events: mpsc::Sender<BridgeEvent>,
) -> Result<ExitStatus, BridgeError> {
    let mut pending: HashMap<u64, oneshot::Sender<pb::Reply>> = HashMap::new();
    let mut next_id: u64 = 0;
    // `None` une fois stdin fermé.
    let mut sink = Some(sink);

    loop {
        tokio::select! {
            out = rx.recv(), if sink.is_some() => match (out, sink.as_mut()) {
                (Some(out), Some(w)) => {
                    let (id, kind) = match out {
                        Outgoing::Request { kind, reply } => {
                            next_id += 1;
                            pending.insert(next_id, reply);
                            (next_id, kind)
                        }
                        Outgoing::Notify(kind) => (0, kind),
                    };
                    let frame = Bytes::from(pb::Command { id, kind: Some(kind) }.encode_to_vec());
                    if let Err(e) = w.send(frame).await {
                        // Le bridge est mort : la lecture de stdout remontera la cause.
                        tracing::warn!(error = %e, "écriture vers le bridge");
                        pending.remove(&id);
                    }
                }
                (None, _) | (_, None) => {
                    // Plus aucune poignée : fermer stdin, le bridge se déconnecte et sort.
                    if let Some(mut w) = sink.take() {
                        let _ = w.close().await;
                    }
                }
            },
            frame = frames.next() => match frame {
                Some(Ok(buf)) => match pb::Event::decode(buf)?.kind {
                    Some(event::Kind::Reply(r)) => {
                        if let Some(tx) = pending.remove(&r.id) {
                            let _ = tx.send(r);
                        }
                    }
                    Some(kind) => {
                        // Consommateur parti : on continue de vider stdout pour ne pas bloquer le bridge.
                        let _ = events.send(kind).await;
                    }
                    None => tracing::warn!("événement vide du bridge"),
                },
                Some(Err(e)) => return Err(e.into()),
                None => {
                    let status = child.wait().await?;
                    return if status.success() { Ok(status) } else { Err(BridgeError::Exited(status)) };
                }
            },
        }
    }
}

impl BridgeHandle {
    async fn request(&self, kind: command::Kind) -> Result<pb::Reply, BridgeError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Outgoing::Request { kind, reply })
            .await
            .map_err(|_| BridgeError::Closed)?;
        let r = rx.await.map_err(|_| BridgeError::Closed)?;
        if r.error.is_empty() {
            Ok(r)
        } else {
            Err(BridgeError::Remote(r.error))
        }
    }

    /// Démarre un compte. S'il n'est pas appairé : événements `Qr`, ou `PairCode`
    /// avec un numéro.
    pub async fn start_account(
        &self,
        account: &AccountAlias,
        pair_phone: Option<&str>,
    ) -> Result<(), BridgeError> {
        self.request(command::Kind::StartAccount(pb::StartAccount {
            account: account.to_string(),
            pair_phone: pair_phone.unwrap_or_default().to_owned(),
        }))
        .await
        .map(|_| ())
    }

    /// Envoie un message construit par `compose`. `id` : identifiant imposé, sinon généré.
    pub async fn send_message(
        &self,
        account: &AccountAlias,
        to: &str,
        message: &wa_proto::WaMessage,
        id: Option<String>,
    ) -> Result<pb::SendResult, BridgeError> {
        let r = self
            .request(command::Kind::SendMessage(pb::SendMessage {
                account: account.to_string(),
                to: to.to_owned(),
                message: prost::Message::encode_to_vec(message),
                id: id.unwrap_or_default(),
            }))
            .await?;
        match r.payload {
            Some(reply::Payload::Send(s)) => Ok(s),
            _ => Err(BridgeError::Remote("réponse d'envoi sans résultat".into())),
        }
    }

    /// Démarre un compte non appairé et rend le code d'appairage à saisir sur le téléphone.
    pub async fn pair_with_code(
        &self,
        account: &AccountAlias,
        phone: &str,
    ) -> Result<String, BridgeError> {
        let r = self
            .request(command::Kind::StartAccount(pb::StartAccount {
                account: account.to_string(),
                pair_phone: phone.to_owned(),
            }))
            .await?;
        match r.payload {
            Some(reply::Payload::PairCode(p)) => Ok(p.code),
            _ => Err(BridgeError::Remote(
                "compte déjà appairé : aucun code produit".into(),
            )),
        }
    }

    pub async fn logout(&self, account: &AccountAlias) -> Result<(), BridgeError> {
        self.request(command::Kind::Logout(pb::Logout {
            account: account.to_string(),
        }))
        .await
        .map(|_| ())
    }

    /// Action sur un groupe ; la réponse dépend de l'opération (voir `bridge.proto`).
    pub async fn group(
        &self,
        account: &AccountAlias,
        jid: &str,
        op: pb::group_command::Op,
    ) -> Result<Option<reply::Payload>, BridgeError> {
        self.request(command::Kind::Group(pb::GroupCommand {
            account: account.to_string(),
            jid: jid.to_owned(),
            op: Some(op),
        }))
        .await
        .map(|r| r.payload)
    }

    pub async fn chat_settings(&self, settings: pb::ChatSettings) -> Result<(), BridgeError> {
        self.request(command::Kind::ChatSettings(settings))
            .await
            .map(|_| ())
    }

    /// Profil, blocage, confidentialité ; la réponse dépend de l'opération.
    pub async fn account_command(
        &self,
        account: &AccountAlias,
        op: pb::account_command::Op,
    ) -> Result<Option<reply::Payload>, BridgeError> {
        self.request(command::Kind::AccountCommand(pb::AccountCommand {
            account: account.to_string(),
            op: Some(op),
        }))
        .await
        .map(|r| r.payload)
    }

    /// Demande au téléphone de renvoyer un média expiré ; rend son nouveau chemin.
    pub async fn media_retry(&self, req: pb::MediaRetry) -> Result<String, BridgeError> {
        match self.request(command::Kind::MediaRetry(req)).await?.payload {
            Some(reply::Payload::Text(path)) => Ok(path),
            _ => Err(BridgeError::Remote("réponse de renvoi sans chemin".into())),
        }
    }

    pub async fn request_history(&self, req: pb::RequestHistory) -> Result<(), BridgeError> {
        self.request(command::Kind::RequestHistory(req))
            .await
            .map(|_| ())
    }

    pub async fn upload_media(
        &self,
        account: &AccountAlias,
        path: &std::path::Path,
        media_type: &str,
    ) -> Result<pb::UploadResult, BridgeError> {
        let r = self
            .request(command::Kind::UploadMedia(pb::UploadMedia {
                account: account.to_string(),
                path: path.display().to_string(),
                media_type: media_type.to_owned(),
            }))
            .await?;
        match r.payload {
            Some(reply::Payload::Upload(u)) => Ok(u),
            _ => Err(BridgeError::Remote("réponse d'upload sans résultat".into())),
        }
    }

    pub async fn mark_read(
        &self,
        account: &AccountAlias,
        chat: &str,
        sender: &str,
        ids: Vec<String>,
    ) -> Result<(), BridgeError> {
        self.request(command::Kind::MarkRead(pb::MarkRead {
            account: account.to_string(),
            chat: chat.to_owned(),
            sender: sender.to_owned(),
            ids,
        }))
        .await
        .map(|_| ())
    }

    pub async fn chat_presence(
        &self,
        account: &AccountAlias,
        chat: &str,
        state: &str,
    ) -> Result<(), BridgeError> {
        self.request(command::Kind::ChatPresence(pb::ChatPresence {
            account: account.to_string(),
            chat: chat.to_owned(),
            state: state.to_owned(),
        }))
        .await
        .map(|_| ())
    }

    pub async fn check_phones(
        &self,
        account: &AccountAlias,
        phones: Vec<String>,
    ) -> Result<pb::PhoneChecks, BridgeError> {
        let r = self
            .request(command::Kind::CheckPhones(pb::CheckPhones {
                account: account.to_string(),
                phones,
            }))
            .await?;
        match r.payload {
            Some(reply::Payload::Phones(p)) => Ok(p),
            _ => Err(BridgeError::Remote("réponse sans vérification".into())),
        }
    }

    pub async fn get_contacts(&self, account: &AccountAlias) -> Result<pb::Contacts, BridgeError> {
        let r = self
            .request(command::Kind::GetContacts(pb::GetContacts {
                account: account.to_string(),
            }))
            .await?;
        match r.payload {
            Some(reply::Payload::Contacts(c)) => Ok(c),
            _ => Err(BridgeError::Remote("réponse sans contacts".into())),
        }
    }

    /// Groupes demandés, ou tous les groupes rejoints si `jids` est vide.
    pub async fn get_groups(
        &self,
        account: &AccountAlias,
        jids: Vec<String>,
    ) -> Result<pb::Groups, BridgeError> {
        let r = self
            .request(command::Kind::GetGroups(pb::GetGroups {
                account: account.to_string(),
                jids,
            }))
            .await?;
        match r.payload {
            Some(reply::Payload::Groups(g)) => Ok(g),
            _ => Err(BridgeError::Remote("réponse sans groupes".into())),
        }
    }

    /// Télécharge le média d'un message (enveloppes retirées) vers `path`.
    pub async fn download_media(
        &self,
        account: &AccountAlias,
        message: Vec<u8>,
        path: &std::path::Path,
    ) -> Result<pb::MediaResult, BridgeError> {
        let r = self
            .request(command::Kind::DownloadMedia(pb::DownloadMedia {
                account: account.to_string(),
                message,
                path: path.display().to_string(),
            }))
            .await?;
        match r.payload {
            Some(reply::Payload::Media(m)) => Ok(m),
            _ => Err(BridgeError::Remote("réponse sans média".into())),
        }
    }

    /// Acquitte un message reçu ou un lot d'historique. `ok = false` : WhatsApp le redélivrera.
    pub async fn ack(&self, seq: u64, ok: bool) -> Result<(), BridgeError> {
        self.tx
            .send(Outgoing::Notify(command::Kind::Ack(pb::Ack { seq, ok })))
            .await
            .map_err(|_| BridgeError::Closed)
    }
}

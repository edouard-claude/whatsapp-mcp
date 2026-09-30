//! Construction des messages sortants (`waE2E.Message`), comme le fait whatsmeow
//! (`BuildReaction`, `BuildEdit`, `BuildRevoke`, `BuildPollCreation`), mais côté
//! Rust : le bridge se contente d'envoyer.

use wa_proto::WaMessage;
use wa_proto::wa_common::MessageKey;
use wa_proto::wa_web_protobufs_e2e::{
    AudioMessage, ContextInfo, DocumentMessage, ExtendedTextMessage, FutureProofMessage,
    ImageMessage, LocationMessage, MessageContextInfo, PollCreationMessage, ProtocolMessage,
    ReactionMessage, VideoMessage, poll_creation_message, protocol_message,
};
use wa_proto::wabridge::v1 as pb;

/// Message cité par une réponse.
pub struct Quoted {
    pub id: String,
    /// Auteur du message cité (utile dans un groupe).
    pub sender: String,
    /// Message cité lui-même, enveloppes retirées : WhatsApp l'affiche au-dessus de la réponse.
    pub message: Option<WaMessage>,
}

fn context(quoted: Option<Quoted>, mentions: Vec<String>) -> Option<Box<ContextInfo>> {
    if quoted.is_none() && mentions.is_empty() {
        return None;
    }
    let mut ci = ContextInfo {
        mentioned_jid: mentions,
        ..Default::default()
    };
    if let Some(q) = quoted {
        ci.stanza_id = Some(q.id);
        ci.participant = Some(q.sender);
        ci.quoted_message = q.message.map(Box::new);
    }
    Some(Box::new(ci))
}

/// Texte, éventuellement en réponse et avec mentions (`@33612345678` dans le texte).
pub fn text(body: &str, quoted: Option<Quoted>, mentions: Vec<String>) -> WaMessage {
    match context(quoted, mentions) {
        None => WaMessage {
            conversation: Some(body.to_owned()),
            ..Default::default()
        },
        Some(ci) => WaMessage {
            extended_text_message: Some(Box::new(ExtendedTextMessage {
                text: Some(body.to_owned()),
                context_info: Some(ci),
                ..Default::default()
            })),
            ..Default::default()
        },
    }
}

/// Clé d'un message existant, comme `BuildMessageKey` : `participant` seulement
/// pour un message d'un autre dans un groupe.
pub fn key(chat: &str, id: &str, from_me: bool, sender: &str) -> MessageKey {
    let individual = chat.ends_with("@s.whatsapp.net") || chat.ends_with("@lid");
    MessageKey {
        remote_jid: Some(chat.to_owned()),
        from_me: Some(from_me),
        id: Some(id.to_owned()),
        participant: (!from_me && !individual).then(|| sender.to_owned()),
    }
}

pub fn reaction(key: MessageKey, emoji: &str, now_ms: i64) -> WaMessage {
    WaMessage {
        reaction_message: Some(ReactionMessage {
            key: Some(key),
            text: Some(emoji.to_owned()),
            sender_timestamp_ms: Some(now_ms),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn edit(chat: &str, id: &str, body: &str, now_ms: i64) -> WaMessage {
    WaMessage {
        edited_message: Some(Box::new(FutureProofMessage {
            message: Some(Box::new(WaMessage {
                protocol_message: Some(Box::new(ProtocolMessage {
                    key: Some(key(chat, id, true, "")),
                    r#type: Some(protocol_message::Type::MessageEdit as i32),
                    edited_message: Some(Box::new(text(body, None, Vec::new()))),
                    timestamp_ms: Some(now_ms),
                    ..Default::default()
                })),
                ..Default::default()
            })),
        })),
        ..Default::default()
    }
}

pub fn revoke(key: MessageKey) -> WaMessage {
    WaMessage {
        protocol_message: Some(Box::new(ProtocolMessage {
            key: Some(key),
            r#type: Some(protocol_message::Type::Revoke as i32),
            ..Default::default()
        })),
        ..Default::default()
    }
}

pub fn location(lat: f64, lon: f64, name: Option<String>, address: Option<String>) -> WaMessage {
    WaMessage {
        location_message: Some(Box::new(LocationMessage {
            degrees_latitude: Some(lat),
            degrees_longitude: Some(lon),
            name,
            address,
            ..Default::default()
        })),
        ..Default::default()
    }
}

/// Sondage. `secret` : 32 octets aléatoires, nécessaires pour déchiffrer les votes.
pub fn poll(question: &str, options: &[String], multiple: bool, secret: [u8; 32]) -> WaMessage {
    WaMessage {
        poll_creation_message: Some(Box::new(PollCreationMessage {
            name: Some(question.to_owned()),
            options: options
                .iter()
                .map(|o| poll_creation_message::Option {
                    option_name: Some(o.clone()),
                    ..Default::default()
                })
                .collect(),
            // 0 : autant de choix que voulu ; 1 : un seul.
            selectable_options_count: Some(if multiple { 0 } else { 1 }),
            ..Default::default()
        })),
        message_context_info: Some(MessageContextInfo {
            message_secret: Some(secret.to_vec()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Nature d'un média sortant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaKind {
    Image,
    Video,
    /// Fichier audio ordinaire.
    Audio,
    /// Message vocal (OGG Opus).
    Voice,
    Document,
}

impl MediaKind {
    /// Type attendu par l'upload de whatsmeow (clés de chiffrement différentes).
    pub fn upload_type(self) -> &'static str {
        match self {
            MediaKind::Image => "image",
            MediaKind::Video => "video",
            MediaKind::Audio | MediaKind::Voice => "audio",
            MediaKind::Document => "document",
        }
    }
}

pub struct Media {
    pub kind: MediaKind,
    pub mime: String,
    pub caption: Option<String>,
    pub file_name: Option<String>,
    /// Durée en secondes (audio, vidéo).
    pub seconds: Option<u32>,
}

pub fn media(m: Media, up: pb::UploadResult, quoted: Option<Quoted>) -> WaMessage {
    let ci = context(quoted, Vec::new());
    let length = Some(up.file_length);
    let mut out = WaMessage::default();
    match m.kind {
        MediaKind::Image => {
            out.image_message = Some(Box::new(ImageMessage {
                url: Some(up.url),
                direct_path: Some(up.direct_path),
                media_key: Some(up.media_key),
                file_enc_sha256: Some(up.file_enc_sha256),
                file_sha256: Some(up.file_sha256),
                file_length: length,
                mimetype: Some(m.mime),
                caption: m.caption,
                context_info: ci,
                ..Default::default()
            }));
        }
        MediaKind::Video => {
            out.video_message = Some(Box::new(VideoMessage {
                url: Some(up.url),
                direct_path: Some(up.direct_path),
                media_key: Some(up.media_key),
                file_enc_sha256: Some(up.file_enc_sha256),
                file_sha256: Some(up.file_sha256),
                file_length: length,
                mimetype: Some(m.mime),
                caption: m.caption,
                seconds: m.seconds,
                context_info: ci,
                ..Default::default()
            }));
        }
        MediaKind::Audio | MediaKind::Voice => {
            out.audio_message = Some(Box::new(AudioMessage {
                url: Some(up.url),
                direct_path: Some(up.direct_path),
                media_key: Some(up.media_key),
                file_enc_sha256: Some(up.file_enc_sha256),
                file_sha256: Some(up.file_sha256),
                file_length: length,
                mimetype: Some(m.mime),
                seconds: m.seconds,
                ptt: Some(m.kind == MediaKind::Voice),
                context_info: ci,
                ..Default::default()
            }));
        }
        MediaKind::Document => {
            out.document_message = Some(Box::new(DocumentMessage {
                url: Some(up.url),
                direct_path: Some(up.direct_path),
                media_key: Some(up.media_key),
                file_enc_sha256: Some(up.file_enc_sha256),
                file_sha256: Some(up.file_sha256),
                file_length: length,
                mimetype: Some(m.mime),
                title: m.file_name.clone(),
                file_name: m.file_name,
                caption: m.caption,
                context_info: ci,
                ..Default::default()
            }));
        }
    }
    out
}

/// Type MIME et nature d'un fichier d'après son extension.
pub fn guess(path: &std::path::Path, as_voice: bool) -> (MediaKind, String) {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let (kind, mime) = match ext.as_str() {
        "jpg" | "jpeg" => (MediaKind::Image, "image/jpeg"),
        "png" => (MediaKind::Image, "image/png"),
        "webp" => (MediaKind::Image, "image/webp"),
        "mp4" | "m4v" => (MediaKind::Video, "video/mp4"),
        "mov" => (MediaKind::Video, "video/quicktime"),
        "ogg" | "opus" | "oga" => (MediaKind::Audio, "audio/ogg; codecs=opus"),
        "mp3" => (MediaKind::Audio, "audio/mpeg"),
        "m4a" | "aac" => (MediaKind::Audio, "audio/mp4"),
        "wav" => (MediaKind::Audio, "audio/wav"),
        "pdf" => (MediaKind::Document, "application/pdf"),
        "txt" | "md" => (MediaKind::Document, "text/plain"),
        "csv" => (MediaKind::Document, "text/csv"),
        "zip" => (MediaKind::Document, "application/zip"),
        "doc" => (MediaKind::Document, "application/msword"),
        "docx" => (
            MediaKind::Document,
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ),
        "xls" => (MediaKind::Document, "application/vnd.ms-excel"),
        "xlsx" => (
            MediaKind::Document,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ),
        "pptx" => (
            MediaKind::Document,
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        ),
        _ => (MediaKind::Document, "application/octet-stream"),
    };
    let kind = if as_voice && mime.starts_with("audio/ogg") {
        MediaKind::Voice
    } else {
        kind
    };
    (kind, mime.to_owned())
}

/// Durée d'un fichier OGG Opus en secondes : position (granule) de la dernière
/// page, moins le pré-saut de l'en-tête, à 48 kHz. `None` si ce n'est pas de l'OGG.
pub fn ogg_opus_seconds(data: &[u8]) -> Option<u32> {
    // En-tête OpusHead : pré-saut (u16 little-endian) à l'octet 10 du paquet.
    let head = find(data, b"OpusHead")?;
    let pre_skip = u64::from(u16::from_le_bytes([
        *data.get(head + 10)?,
        *data.get(head + 11)?,
    ]));
    // Dernière page : dernier « OggS », granule sur 8 octets à partir de l'octet 6.
    let last = data.windows(4).rposition(|w| w == b"OggS")?;
    let g = data.get(last + 6..last + 14)?;
    let granule = u64::from_le_bytes(g.try_into().ok()?);
    let samples = granule.checked_sub(pre_skip)?;
    u32::try_from(samples.div_ceil(48_000)).ok()
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::{self, Kind, Parsed};

    #[test]
    fn built_messages_parse_back() {
        assert!(matches!(
            parse::parse(&text("salut", None, vec![])),
            Parsed::Content { kind: Kind::Text, text: Some(t), .. } if t == "salut"
        ));
        let reply = text(
            "oui",
            Some(Quoted {
                id: "Q1".into(),
                sender: "1@s.whatsapp.net".into(),
                message: None,
            }),
            vec![],
        );
        assert!(
            matches!(parse::parse(&reply), Parsed::Content { quoted_id: Some(q), .. } if q == "Q1")
        );
        let k = key("g@g.us", "M1", false, "2@s.whatsapp.net");
        assert_eq!(k.participant.as_deref(), Some("2@s.whatsapp.net"));
        assert_eq!(
            parse::parse(&reaction(k.clone(), "👍", 1)),
            Parsed::Reaction {
                target_id: "M1".into(),
                emoji: "👍".into()
            }
        );
        assert_eq!(
            parse::parse(&edit("1@s.whatsapp.net", "M2", "corrigé", 1)),
            Parsed::Edit {
                target_id: "M2".into(),
                text: Some("corrigé".into())
            }
        );
        assert_eq!(
            parse::parse(&revoke(k)),
            Parsed::Revoke {
                target_id: "M1".into()
            }
        );
        assert!(matches!(
            parse::parse(&poll("Qui vient ?", &["moi".into()], false, [0; 32])),
            Parsed::Content {
                kind: Kind::Poll,
                ..
            }
        ));
    }

    #[test]
    fn dm_keys_have_no_participant() {
        assert_eq!(
            key("1@s.whatsapp.net", "M", false, "1@s.whatsapp.net").participant,
            None
        );
        assert_eq!(key("g@g.us", "M", true, "").participant, None);
    }

    #[test]
    fn ogg_duration() {
        let mut data = Vec::new();
        data.extend_from_slice(b"OggS\0\0");
        data.extend_from_slice(&0u64.to_le_bytes());
        data.extend_from_slice(b"....OpusHead\x01\x01");
        data.extend_from_slice(&312u16.to_le_bytes());
        data.extend_from_slice(b"....OggS\0\x04");
        data.extend_from_slice(&(312u64 + 48_000 * 42).to_le_bytes());
        assert_eq!(ogg_opus_seconds(&data), Some(42));
        assert_eq!(ogg_opus_seconds(b"pas de l'ogg"), None);
    }
}

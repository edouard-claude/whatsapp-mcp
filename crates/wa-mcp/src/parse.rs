//! Lecture du message WhatsApp brut (`waE2E.Message`) : que faut-il en faire ?

use prost::Message as _;
use wa_proto::WaMessage;
use wa_proto::wa_web_protobufs_e2e::{ContextInfo, protocol_message};

/// Nature d'un message conservé.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Text,
    Image,
    Video,
    Voice,
    Audio,
    Document,
    Sticker,
    Poll,
    /// Vote chiffré : gardé en brut, déchiffré plus tard (envoi de sondages, P3).
    PollVote,
    Location,
    Contact,
    /// En-tête d'album : les photos suivent en messages séparés.
    Album,
    Event,
    GroupInvite,
    /// Message d'entreprise (modèle, boutons, interactif) ou réponse à un bouton.
    Business,
    /// Type non reconnu : gardé en brut, un décodeur futur le relira.
    Other,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Text => "text",
            Kind::Image => "image",
            Kind::Video => "video",
            Kind::Voice => "voice",
            Kind::Audio => "audio",
            Kind::Document => "document",
            Kind::Sticker => "sticker",
            Kind::Poll => "poll",
            Kind::PollVote => "poll_vote",
            Kind::Location => "location",
            Kind::Contact => "contact",
            Kind::Album => "album",
            Kind::Event => "event",
            Kind::GroupInvite => "group_invite",
            Kind::Business => "business",
            Kind::Other => "other",
        }
    }
}

/// Ce qu'un message demande au stockage.
#[derive(Debug, PartialEq, Eq)]
pub enum Parsed {
    /// Nouveau contenu dans la discussion.
    Content {
        kind: Kind,
        text: Option<String>,
        /// Message cité (réponse).
        quoted_id: Option<String>,
    },
    /// Réaction à un message de la même discussion. `emoji` vide : retirée.
    Reaction { target_id: String, emoji: String },
    /// Nouveau texte d'un message existant.
    Edit {
        target_id: String,
        text: Option<String>,
    },
    /// Message supprimé pour tous.
    Revoke { target_id: String },
    /// Signalisation sans contenu (clés, avis d'historique, synchro d'état...) :
    /// acquittée, pas stockée.
    Plumbing,
}

pub fn decode(raw: &[u8]) -> Result<WaMessage, prost::DecodeError> {
    WaMessage::decode(raw)
}

/// Retire les enveloppes (autre appareil, éphémère, vue unique, édition, document
/// avec légende, élément d'album, commentaire) jusqu'au message utile.
pub fn unwrap(mut m: &WaMessage) -> &WaMessage {
    // Profondeur bornée : un message malveillant ne fait pas boucler.
    for _ in 0..8 {
        let inner = m
            .device_sent_message
            .as_ref()
            .and_then(|d| d.message.as_deref())
            .or_else(|| {
                [
                    &m.ephemeral_message,
                    &m.view_once_message,
                    &m.view_once_message_v2,
                    &m.document_with_caption_message,
                    &m.edited_message,
                    &m.associated_child_message,
                ]
                .into_iter()
                .find_map(|f| f.as_ref().and_then(|f| f.message.as_deref()))
            })
            .or_else(|| {
                m.comment_message
                    .as_ref()
                    .and_then(|c| c.message.as_deref())
            });
        match inner {
            Some(i) => m = i,
            None => break,
        }
    }
    m
}

fn non_empty(s: Option<&String>) -> Option<String> {
    s.filter(|t| !t.trim().is_empty()).cloned()
}

fn quoted(ci: Option<&ContextInfo>) -> Option<String> {
    non_empty(ci.and_then(|c| c.stanza_id.as_ref()))
}

pub fn parse(msg: &WaMessage) -> Parsed {
    let m = unwrap(msg);
    let content = |kind, text: Option<&String>, ci: Option<&ContextInfo>| Parsed::Content {
        kind,
        text: non_empty(text),
        quoted_id: quoted(ci),
    };

    if let Some(p) = m.protocol_message.as_deref() {
        let target_id = p
            .key
            .as_ref()
            .and_then(|k| k.id.clone())
            .unwrap_or_default();
        return match p
            .r#type
            .and_then(|t| protocol_message::Type::try_from(t).ok())
        {
            Some(protocol_message::Type::Revoke) if !target_id.is_empty() => {
                Parsed::Revoke { target_id }
            }
            Some(protocol_message::Type::MessageEdit) if !target_id.is_empty() => {
                let text = p.edited_message.as_deref().map(|e| match parse(e) {
                    Parsed::Content { text, .. } => text,
                    _ => None,
                });
                Parsed::Edit {
                    target_id,
                    text: text.flatten(),
                }
            }
            _ => Parsed::Plumbing,
        };
    }
    if let Some(t) = m.conversation.as_ref() {
        return content(Kind::Text, Some(t), None);
    }
    if let Some(e) = m.extended_text_message.as_deref() {
        return content(Kind::Text, e.text.as_ref(), e.context_info.as_deref());
    }
    if let Some(i) = m.image_message.as_deref() {
        return content(Kind::Image, i.caption.as_ref(), i.context_info.as_deref());
    }
    if let Some(v) = m.video_message.as_deref() {
        return content(Kind::Video, v.caption.as_ref(), v.context_info.as_deref());
    }
    if let Some(a) = m.audio_message.as_deref() {
        let kind = if a.ptt.unwrap_or(false) {
            Kind::Voice
        } else {
            Kind::Audio
        };
        return content(kind, None, a.context_info.as_deref());
    }
    if let Some(d) = m.document_message.as_deref() {
        return content(
            Kind::Document,
            d.caption.as_ref().or(d.file_name.as_ref()),
            d.context_info.as_deref(),
        );
    }
    if let Some(s) = m.sticker_message.as_deref() {
        return content(Kind::Sticker, None, s.context_info.as_deref());
    }
    if let Some(r) = m.reaction_message.as_ref() {
        return match r.key.as_ref().and_then(|k| k.id.clone()) {
            Some(target_id) => Parsed::Reaction {
                target_id,
                emoji: r.text.clone().unwrap_or_default(),
            },
            None => Parsed::Plumbing,
        };
    }
    if let Some(p) = m
        .poll_creation_message
        .as_deref()
        .or(m.poll_creation_message_v2.as_deref())
        .or(m.poll_creation_message_v3.as_deref())
    {
        return content(Kind::Poll, p.name.as_ref(), None);
    }
    if m.poll_update_message.is_some() {
        return content(Kind::PollVote, None, None);
    }
    if let Some(l) = m.location_message.as_deref() {
        return content(Kind::Location, l.name.as_ref().or(l.address.as_ref()), None);
    }
    if let Some(c) = m.contact_message.as_deref() {
        return content(Kind::Contact, c.display_name.as_ref(), None);
    }
    if let Some(a) = m.album_message.as_ref() {
        let n = a.expected_image_count.unwrap_or(0) + a.expected_video_count.unwrap_or(0);
        return content(
            Kind::Album,
            Some(&format!("album de {n} éléments")),
            a.context_info.as_deref(),
        );
    }
    if let Some(e) = m.event_message.as_ref() {
        let text = [e.name.as_deref(), e.description.as_deref()]
            .into_iter()
            .flatten()
            .filter(|t| !t.is_empty())
            .collect::<Vec<_>>()
            .join(" : ");
        return content(Kind::Event, Some(&text), e.context_info.as_deref());
    }
    if let Some(g) = m.group_invite_message.as_ref() {
        let text = [g.group_name.as_deref(), g.caption.as_deref()]
            .into_iter()
            .flatten()
            .filter(|t| !t.is_empty())
            .collect::<Vec<_>>()
            .join(" : ");
        return content(Kind::GroupInvite, Some(&text), g.context_info.as_deref());
    }
    if let Some(r) = m.template_button_reply_message.as_ref() {
        return content(
            Kind::Business,
            r.selected_display_text.as_ref(),
            r.context_info.as_deref(),
        );
    }
    if let Some(b) = m.buttons_message.as_ref() {
        return content(
            Kind::Business,
            b.content_text.as_ref(),
            b.context_info.as_deref(),
        );
    }
    if let Some(i) = m.interactive_message.as_ref() {
        return content(
            Kind::Business,
            i.body.as_ref().and_then(|b| b.text.as_ref()),
            i.context_info.as_deref(),
        );
    }
    if let Some(t) = m.template_message.as_ref() {
        return content(
            Kind::Business,
            t.hydrated_template
                .as_ref()
                .and_then(|h| h.hydrated_content_text.as_ref()),
            t.context_info.as_deref(),
        );
    }
    if is_plumbing_only(m) {
        return Parsed::Plumbing;
    }
    content(Kind::Other, None, None)
}

/// Média téléchargeable porté par un message déjà déballé (voir [`unwrap`]).
pub struct MediaRef {
    pub kind: Kind,
    pub mime: Option<String>,
    pub file_name: Option<String>,
    /// Clé de chiffrement du média : nécessaire pour redemander un média expiré.
    pub media_key: Option<Vec<u8>>,
}

pub fn media(m: &WaMessage) -> Option<MediaRef> {
    let r =
        |kind, mime: &Option<String>, file_name: Option<&String>, key: &Option<Vec<u8>>| MediaRef {
            kind,
            mime: mime.clone(),
            file_name: file_name.cloned(),
            media_key: key.clone(),
        };
    if let Some(i) = m.image_message.as_deref() {
        return Some(r(Kind::Image, &i.mimetype, None, &i.media_key));
    }
    if let Some(v) = m.video_message.as_deref() {
        return Some(r(Kind::Video, &v.mimetype, None, &v.media_key));
    }
    if let Some(a) = m.audio_message.as_deref() {
        let kind = if a.ptt.unwrap_or(false) {
            Kind::Voice
        } else {
            Kind::Audio
        };
        return Some(r(kind, &a.mimetype, None, &a.media_key));
    }
    if let Some(d) = m.document_message.as_deref() {
        return Some(r(
            Kind::Document,
            &d.mimetype,
            d.file_name.as_ref(),
            &d.media_key,
        ));
    }
    if let Some(s) = m.sticker_message.as_deref() {
        return Some(r(Kind::Sticker, &s.mimetype, None, &s.media_key));
    }
    None
}

/// Remplace l'emplacement du média d'un message (enveloppes comprises) par le
/// chemin que le téléphone vient de renvoyer. Faux si le message n'a pas de média.
pub fn set_direct_path(msg: &mut WaMessage, path: &str) -> bool {
    // Le chemin des enveloppes est relevé en lecture, puis parcouru en écriture :
    // le vérificateur d'emprunts refuse un retour conditionnel d'emprunt mutable.
    let mut steps = Vec::new();
    let mut cur = &*msg;
    for _ in 0..8 {
        let Some((step, next)) = wrapper(cur) else {
            break;
        };
        steps.push(step);
        cur = next;
    }
    let mut m = msg;
    for step in steps {
        m = match wrapper_mut(m, step) {
            Some(next) => next,
            None => return false,
        };
    }
    let (url, direct) = if let Some(x) = m.image_message.as_deref_mut() {
        (&mut x.url, &mut x.direct_path)
    } else if let Some(x) = m.video_message.as_deref_mut() {
        (&mut x.url, &mut x.direct_path)
    } else if let Some(x) = m.audio_message.as_deref_mut() {
        (&mut x.url, &mut x.direct_path)
    } else if let Some(x) = m.document_message.as_deref_mut() {
        (&mut x.url, &mut x.direct_path)
    } else if let Some(x) = m.sticker_message.as_deref_mut() {
        (&mut x.url, &mut x.direct_path)
    } else {
        return false;
    };
    // Sans URL, whatsmeow télécharge par le chemin, sur l'hôte média courant.
    *url = None;
    *direct = Some(path.to_owned());
    true
}

#[derive(Clone, Copy)]
enum Wrapper {
    DeviceSent,
    Ephemeral,
    ViewOnce,
    ViewOnceV2,
    DocumentWithCaption,
    Edited,
    AssociatedChild,
    Comment,
}

type FutureProof = Option<Box<wa_proto::wa_web_protobufs_e2e::FutureProofMessage>>;

fn fp(f: &FutureProof) -> Option<&WaMessage> {
    f.as_ref().and_then(|f| f.message.as_deref())
}

fn fp_mut(f: &mut FutureProof) -> Option<&mut WaMessage> {
    f.as_mut().and_then(|f| f.message.as_deref_mut())
}

fn wrapper(m: &WaMessage) -> Option<(Wrapper, &WaMessage)> {
    use Wrapper::*;
    m.device_sent_message
        .as_ref()
        .and_then(|d| d.message.as_deref())
        .map(|i| (DeviceSent, i))
        .or_else(|| fp(&m.ephemeral_message).map(|i| (Ephemeral, i)))
        .or_else(|| fp(&m.view_once_message).map(|i| (ViewOnce, i)))
        .or_else(|| fp(&m.view_once_message_v2).map(|i| (ViewOnceV2, i)))
        .or_else(|| fp(&m.document_with_caption_message).map(|i| (DocumentWithCaption, i)))
        .or_else(|| fp(&m.edited_message).map(|i| (Edited, i)))
        .or_else(|| fp(&m.associated_child_message).map(|i| (AssociatedChild, i)))
        .or_else(|| {
            m.comment_message
                .as_ref()
                .and_then(|c| c.message.as_deref())
                .map(|i| (Comment, i))
        })
}

fn wrapper_mut(m: &mut WaMessage, w: Wrapper) -> Option<&mut WaMessage> {
    let fp = fp_mut;
    match w {
        Wrapper::DeviceSent => m
            .device_sent_message
            .as_mut()
            .and_then(|d| d.message.as_deref_mut()),
        Wrapper::Ephemeral => fp(&mut m.ephemeral_message),
        Wrapper::ViewOnce => fp(&mut m.view_once_message),
        Wrapper::ViewOnceV2 => fp(&mut m.view_once_message_v2),
        Wrapper::DocumentWithCaption => fp(&mut m.document_with_caption_message),
        Wrapper::Edited => fp(&mut m.edited_message),
        Wrapper::AssociatedChild => fp(&mut m.associated_child_message),
        Wrapper::Comment => m
            .comment_message
            .as_mut()
            .and_then(|c| c.message.as_deref_mut()),
    }
}

/// Message qui ne porte que de la signalisation : distribution de clés de groupe,
/// métadonnées, épinglage et conservation (non gérés avant P4), réactions chiffrées
/// (communautés, non gérées), avis d'historique et emplacements réservés.
fn is_plumbing_only(m: &WaMessage) -> bool {
    let stripped = WaMessage {
        sender_key_distribution_message: None,
        message_context_info: None,
        keep_in_chat_message: None,
        pin_in_chat_message: None,
        enc_reaction_message: None,
        message_history_notice: None,
        message_history_bundle: None,
        placeholder_message: None,
        // Clone intégral, mais seulement pour un message qu'aucune branche n'a reconnu.
        ..m.clone()
    };
    stripped == WaMessage::default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use wa_proto::wa_common::MessageKey;
    use wa_proto::wa_web_protobufs_e2e::{
        AudioMessage, ExtendedTextMessage, FutureProofMessage, ProtocolMessage, ReactionMessage,
    };

    fn text(t: &str) -> WaMessage {
        WaMessage {
            conversation: Some(t.into()),
            ..Default::default()
        }
    }

    fn key(id: &str) -> Option<MessageKey> {
        Some(MessageKey {
            id: Some(id.into()),
            ..Default::default()
        })
    }

    #[test]
    fn text_roundtrip_through_raw_bytes() {
        let back = decode(&text("salut").encode_to_vec()).map(|m| parse(&m));
        assert_eq!(
            back.ok(),
            Some(Parsed::Content {
                kind: Kind::Text,
                text: Some("salut".into()),
                quoted_id: None
            })
        );
    }

    #[test]
    fn reply_keeps_quoted_id() {
        let m = WaMessage {
            extended_text_message: Some(Box::new(ExtendedTextMessage {
                text: Some("oui".into()),
                context_info: Some(Box::new(ContextInfo {
                    stanza_id: Some("ABC".into()),
                    ..Default::default()
                })),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert!(matches!(parse(&m), Parsed::Content { quoted_id: Some(q), .. } if q == "ABC"));
    }

    #[test]
    fn ephemeral_voice_is_unwrapped() {
        let inner = WaMessage {
            audio_message: Some(Box::new(AudioMessage {
                ptt: Some(true),
                ..Default::default()
            })),
            ..Default::default()
        };
        let m = WaMessage {
            ephemeral_message: Some(Box::new(FutureProofMessage {
                message: Some(Box::new(inner)),
            })),
            ..Default::default()
        };
        assert!(matches!(
            parse(&m),
            Parsed::Content {
                kind: Kind::Voice,
                ..
            }
        ));
    }

    #[test]
    fn edit_revoke_reaction() {
        let edit = WaMessage {
            protocol_message: Some(Box::new(ProtocolMessage {
                key: key("M1"),
                r#type: Some(protocol_message::Type::MessageEdit as i32),
                edited_message: Some(Box::new(text("corrigé"))),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(
            parse(&edit),
            Parsed::Edit {
                target_id: "M1".into(),
                text: Some("corrigé".into())
            }
        );

        let revoke = WaMessage {
            protocol_message: Some(Box::new(ProtocolMessage {
                key: key("M2"),
                r#type: Some(protocol_message::Type::Revoke as i32),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(
            parse(&revoke),
            Parsed::Revoke {
                target_id: "M2".into()
            }
        );

        let reaction = WaMessage {
            reaction_message: Some(ReactionMessage {
                key: key("M3"),
                text: Some("👍".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            parse(&reaction),
            Parsed::Reaction {
                target_id: "M3".into(),
                emoji: "👍".into()
            }
        );
    }

    #[test]
    fn direct_path_is_replaced_through_wrappers() {
        let inner = WaMessage {
            audio_message: Some(Box::new(AudioMessage {
                url: Some("https://old".into()),
                direct_path: Some("/old".into()),
                ..Default::default()
            })),
            ..Default::default()
        };
        let mut m = WaMessage {
            ephemeral_message: Some(Box::new(FutureProofMessage {
                message: Some(Box::new(inner)),
            })),
            ..Default::default()
        };
        assert!(set_direct_path(&mut m, "/new"));
        let a = unwrap(&m).audio_message.as_deref().expect("audio");
        assert_eq!(
            (a.url.as_deref(), a.direct_path.as_deref()),
            (None, Some("/new"))
        );
        assert!(!set_direct_path(&mut text("x"), "/p"));
    }

    #[test]
    fn history_notification_is_plumbing() {
        let m = WaMessage {
            protocol_message: Some(Box::new(ProtocolMessage {
                r#type: Some(protocol_message::Type::HistorySyncNotification as i32),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(parse(&m), Parsed::Plumbing);
        assert_eq!(parse(&WaMessage::default()), Parsed::Plumbing);
    }
}

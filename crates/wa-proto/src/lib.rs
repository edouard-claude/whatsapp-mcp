//! Types protobuf générés par `prost` :
//!
//! - [`wabridge::v1`] : le contrat entre `wa-mcp` et `wa-bridge` (`proto/bridge.proto`) ;
//! - les paquets whatsmeow ([`wa_web_protobufs_e2e`] et ses dépendances) : le message
//!   WhatsApp brut transmis dans `IncomingMessage.raw`.

// Code généré : sa forme et sa documentation viennent des .proto, hors de notre contrôle.
#![allow(clippy::all, clippy::pedantic, rustdoc::all)]

include!(concat!(env!("OUT_DIR"), "/_includes.rs"));

/// Message WhatsApp brut (`waE2E.Message`).
pub use wa_web_protobufs_e2e::Message as WaMessage;

//! Génère les types Rust du contrat du bridge et des messages WhatsApp.
//!
//! Les `.proto` de whatsmeow sont vendorisés sous `proto/whatsmeow/`, au commit
//! indiqué dans `proto/whatsmeow/VERSION`, qui doit rester celui de `bridge/go.mod`.

use std::path::PathBuf;

fn main() -> std::io::Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let whatsmeow = root.join("whatsmeow");
    let files = [
        root.join("bridge.proto"),
        whatsmeow.join("waE2E/WAWebProtobufsE2E.proto"),
    ];
    for f in &files {
        println!("cargo:rerun-if-changed={}", f.display());
    }
    println!("cargo:rerun-if-changed={}", whatsmeow.display());
    prost_build::Config::new()
        .include_file("_includes.rs")
        .compile_protos(&files, &[&root, &whatsmeow])
}

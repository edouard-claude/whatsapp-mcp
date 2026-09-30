# Changelog

## [0.1.0] - 2026-09-30

First public release.

### Added

- **Bridge** (Go, whatsmeow): one process for any number of accounts, driven over
  stdin/stdout with length-prefixed protobuf frames (`proto/bridge.proto`). Pairing by
  QR code or 8-character phone code, three-level reconnection (whatsmeow, backoff,
  watchdog), at-least-once delivery (`SynchronousAck`, decrypted event buffer).
- **Ingestion**: live messages and history sync into one SQLite database per account,
  raw protobuf kept for every message, phone-number / LID identity unification with
  retroactive merge, replies, reactions, edits, deletions for everyone, receipts,
  contacts, groups and participants, chat settings synced from other devices,
  FTS5 accent-insensitive search.
- **MCP server** (Rust, rmcp, stdio): 39 tools (reading, search, contacts, media,
  sending, message actions, chat settings, groups and communities, profile, blocklist,
  privacy, multi-account pairing), output schemas and risk annotations on every tool,
  resources and URI templates, 5 prompts, argument completion, live resource
  notifications (`resources/subscribe` and `subscriptions/listen`).
- **Media**: raw files returned to the host model (inline image or audio when small),
  expired media re-requested from the phone, on-demand older history.
- **Safety**: per-account lock, 20 sends per minute rate limit, edit and delete windows
  enforced, `--read-only` mode.
- **Tooling**: `wa-mcp doctor`, `install.sh`, prebuilt binaries for macOS and Linux
  (arm64, x86_64).

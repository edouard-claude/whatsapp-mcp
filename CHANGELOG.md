# Changelog

## [Unreleased]

### Added

- `wa-mcp service`: one process holds the WhatsApp session (bridge, locks, database,
  send limit) and serves it to several MCP hosts at once over Streamable HTTP, one
  MCP session per host. It listens on `127.0.0.1` only (`--port`, default `47311`),
  requires a bearer token (`service/token`, `0600`; `service/token-read-only` only
  exposes read tools), accepts loopback `Host` headers only and rejects any request
  carrying an `Origin`.
- `wa-mcp service install|uninstall|status`: installs it as a supervised user service
  (launchd on macOS, `systemd --user` on Linux).
- `wa-mcp serve` relays stdio to the service when it answers, and otherwise runs the
  session in-process as before: existing installations are unchanged.
- `wa-mcp doctor` shows whether the service runs, on which port, and the URL and header
  to paste into an HTTP host.

### Fixed

- The `resources/subscribe` notification task outlived its MCP session and waited
  for the next chat update; it now stops with the session.
- The send limit (20/min per account) is shared by every MCP session of an instance,
  and an account paired from one session is seen by all of them.

## [0.1.3] - 2026-11-09

### Fixed

- Initial history could be lost when pairing a large account: the bridge's own
  SQLite connections to `session.db` contended (`database is locked`), and messages
  whose decryption failed this way were dropped. Transactions now take the write
  lock at `BEGIN` (`_txlock=immediate`) and wait their turn instead of failing.

## [0.1.2] - 2026-11-09

### Fixed

- Messages sent into a chat with disappearing messages were permanent: recipients
  saw "This message won't disappear, the sender may be on an old version of
  WhatsApp". The disappearing timer of each chat is now tracked (history sync, live
  messages, group info; existing databases are backfilled from the last message)
  and set on every outgoing text, media, location and poll.

### Changed

- whatsmeow bumped to `79dc2a8c33df` (2026-11-07), vendored `.proto` files resynced;
  Go and Rust dependencies updated.
- Weekly automated whatsmeow update PR (`.github/workflows/whatsmeow.yml`) and
  Dependabot for the other Go, Cargo, Actions and Docker dependencies.

## [0.1.1] - 2026-09-30

### Fixed

- Output schemas declared as required some fields that are omitted when empty or
  false (`edited`, `deleted`, `has_media`, `reactions` on messages, `archived` and
  `pinned` on chats, `groups` on contacts, `truncated` on `list_recent`). Strict MCP
  hosts rejected the output as not matching its schema. These fields are now
  optional in the schema; responses are unchanged.

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

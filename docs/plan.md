# whatsapp-mcp : analyse et plan d'implémentation

Document de conception initial (2026-09-30), conservé tel quel pour l'historique des décisions. Les phases P0 à P6 sont livrées en v0.1.0 ; l'état actuel est décrit dans le README.

## 1. Analyse de l'existant

### lharries/whatsapp-mcp (référence historique, gelé depuis juillet 2025)

- Bridge Go mono-fichier (1 348 lignes), 2 endpoints REST (`/api/send`, `/api/download`), 4 événements gérés (Message, HistorySync, Connected, LoggedOut).
- Serveur MCP Python (FastMCP) qui lit le SQLite du bridge en direct : couplage par le schéma, pas par un contrat.
- 12 tools, aucune resource, aucun prompt.
- REST sur `:8080` toutes interfaces, sans auth : n'importe qui sur le LAN envoie des messages à ta place.
- Cassé contre whatsmeow actuel (ajout de `context.Context`), pas de gestion LID.

### rodrigopg/whatsapp-mcp (fork communautaire, actif, dernier commit 2026-09-23)

- Même architecture, bridge passé à 4 260 lignes dans un seul `main.go`, ~30 endpoints REST, 25 tools.
- Apports réels : loopback + bearer token, migration LID vers PN, table `senders`, media retry, page `/qr`, groupes (create/leave/participants), react/edit/revoke, presence, `IsOnWhatsApp`.
- Transcription : script Python lancé toutes les 5 min par le bridge (whisper.cpp ou API OpenAI-compatible), récupération des audios expirés par scraping du log du bridge.
- Limites constatées dans le code :
  - Toujours 100 % tools : pas de resources, prompts, subscriptions, elicitation, annotations, `outputSchema`.
  - Appairage QR uniquement (`GetQRChannel`), `PairPhone` explicitement exclu.
  - `extractTextContent` ne garde que texte et légendes : réactions, sondages, localisations, contacts, stickers, citations, éditions et suppressions entrantes ne sont pas stockés. Le proto brut est perdu, donc rien n'est rattrapable sans resync.
  - Recherche `LIKE` + `unaccent()`, pas de FTS.
  - Pas de description d'images, pas d'extraction de documents.
  - 3 runtimes à installer (Go + CGO, Python/uv, ffmpeg), transcription écrasable par un resync.
  - Leur propre gap analysis liste encore 11 familles absentes (sondages, invitations, réglages de groupe, profil, blocklist, presence globale, business, éphémères, newsletters, privacy, receipts réels).

### whatsmeow (v0.0.0-20260928, MPL-2.0)

- Surface très large et non exploitée : ~25 méthodes de groupe/communauté, 14 newsletters, sondages, privacy, blocklist, app state (pin/mute/archive/star/labels), ~70 types d'événements.
- `PairPhone(ctx, phone, showPushNotification, clientType, displayName) (code, error)` : il faut être connecté et avoir reçu `events.QR` avant l'appel, numéro au format international sans `0` ni `+`, code `XXXX-XXXX` valable ~160 s.
- `AddEventHandlerWithSuccessStatus` : permet de ne pas acquitter un événement tant qu'il n'est pas persisté (à valider en P0).

### Spec MCP : ce qui change le design

La spec stable est `2026-07-28`, implémentée par `rmcp` 3.x (3.5.0), compatible `2025-11-25`.

- Protocole sans état : plus de handshake `initialize`, plus de session HTTP, `server/discover` obligatoire.
- `subscriptions/listen` remplace `resources/subscribe` : un flux unique pour `resourceSubscriptions` et les `listChanged`.
- MRTR : elicitation via `resultType: "input_required"` + retry, corrélation par `requestState` (feature `request-state` de rmcp).
- Tasks : extension officielle `io.modelcontextprotocol/tasks`, polling `tasks/get`.
- `ttlMs` + `cacheScope` obligatoires sur les listes et `resources/read`.
- Sampling, Roots et Logging sont dépréciés : on ne construit rien dessus.

## 2. Architecture cible

Principe (recadrage du 2026-09-30) : le serveur est **agnostique et autonome**. Il parle WhatsApp et MCP, rien d'autre. Aucune IA dedans : pas de transcription, pas de vision, pas de résumé. Il rend les messages et les médias tels qu'ils arrivent, l'agent hôte fait le reste avec ses propres modèles.

```
   agent hôte (n'importe quel client MCP)
        | stdio
        v
   wa-mcp (Rust, tokio, un seul process)
    |- mcp      : rmcp 3.x, tools/resources/prompts/listen
    |- store    : SQLite + FTS5 par compte, 1 tâche écrivain
    |- ingest   : événements -> store, ack après commit
    |- bridge   : lance et supervise le process enfant
        |
        | pipes stdin/stdout, trames protobuf préfixées par leur longueur
        v
   wa-bridge (Go, whatsmeow, N clients = N comptes)
    |- sqlstore whatsmeow (sessions, clés, lid_map)
    v
  WhatsApp
```

Décisions structurantes :

1. **Mono-process stdio, pas de daemon.** L'hôte lance `wa-mcp`, qui lance le bridge. Aucun service à installer, aucune dépendance externe à l'exécution. La session WhatsApp vit tant que l'hôte garde le serveur ouvert, et le rattrapage hors-ligne de WhatsApp couvre les redémarrages. Un verrou de fichier par compte refuse une seconde instance (sinon `StreamReplaced`).
2. **Bridge en process enfant sur pipes, pas de socket.** Aucun port, aucune auth, et le bridge meurt avec son parent (EOF sur stdin). Ça marche aussi sous un hôte sandboxé qui ferme les sockets Unix. Framing longueur + protobuf, `prost` seul (pas de gRPC).
3. **Bridge Go volontairement bête.** Il expose whatsmeow et rien d'autre : pas de SQL métier, pas de HTTP, pas de logique. Il transmet l'enveloppe (`MessageInfo`) et le `waE2E.Message` en octets bruts, logs sur stderr.
4. **Le proto brut est stocké.** Rust le décode avec `prost` à partir des `.proto` de whatsmeow vendorisés au même commit que `go.mod`. Tout type de message non géré aujourd'hui reste rattrapable demain sans resync.
5. **Contrat protobuf = frontière remplaçable.** Si `whatsapp-rust` (0.7.0, port pur Rust) devient assez mûr, on supprime Go sans toucher au reste.
6. **Multi-comptes dès le départ.** Un seul bridge, N clients whatsmeow (le `sqlstore.Container` gère plusieurs devices nativement), chaque trame porte un `account`. Côté Rust, un répertoire par compte (`accounts/<alias>/store.db` + `media/`) : isolation stricte, suppression triviale, pas de colonne `account` partout.
7. **Maintien de session.** Un appareil lié reste valide tant qu'il se connecte régulièrement et que le téléphone principal se connecte au moins tous les ~14 jours. Le bridge empile trois étages : keepalive et reconnexion auto de whatsmeow (y compris à la première connexion), reconnexion avec backoff (2 s à 5 min) sur les échecs non réessayables, chien de garde toutes les 2 min. Seuls `LOGGED_OUT` et `STREAM_REPLACED` arrêtent les tentatives. L'état et la date de dernière connexion sont stockés (`meta`) pour être exposés en P2 (`session_status`), le téléphone inactif ne pouvant être que signalé.
8. **Tout sous un seul répertoire de données.** `WA_DATA_DIR` est la seule racine en écriture (bases, sessions, médias téléchargés), et la seule configuration obligatoire.

### Layout du dépôt

```
proto/bridge.proto            contrat Rust <-> Go
proto/whatsmeow/              .proto vendorisés (script de sync épinglé)
bridge/                       module Go (whatsmeow), lit stdin, écrit stdout
crates/wa-proto/              code généré prost (build.rs)
crates/wa-mcp/                binaire unique : serveur stdio (défaut) | pair | doctor
  src/{domain,bridge,store,ingest,mcp,cli}/
```

Deux crates seulement : `wa-proto` isole la génération de code, tout le reste est en modules (pas de découpage cosmétique).

### Modèle de données (SQLite, WAL, FTS5)

- `chats`, `contacts` (PN + LID unifiés dès l'écriture), `groups`, `group_participants`
- `messages` : clé `(chat_jid, id)`, `kind` typé, `text`, `quoted_id`, `edited_at`, `revoked_at`, `raw_proto BLOB`
- `reactions`, `receipts`, `polls`, `poll_votes`
- `media` : clés de déchiffrement, `direct_path`, état (`remote | local | expired | retry_pending`), chemin local
- `messages_fts` : FTS5 `unicode61 remove_diacritics 2` sur texte + légendes
- Option ultérieure : table `annotations` alimentée par l'agent (voir `annotate_message`), indexée dans le FTS et jamais écrasée par un resync.

Types Rust : newtypes `Jid`, `MessageId`, `ChatId`, enums pour `MessageKind`, `MediaState`, `PairingState`, `ConnectionState`. Un écrivain unique alimenté par un canal borné, lectures sur un pool.

## 3. Surface MCP

### Tools (~40, `outputSchema` + annotations sur tous)

| Domaine | Tools |
|---|---|
| Comptes | `list_accounts`, `session_status`, `session_pair` (qr ou phone, crée l'alias), `session_logout` |
| Lecture | `list_chats`, `get_chat`, `list_messages`, `get_message_context`, `search_messages`, `get_unread` |
| Contacts | `search_contacts`, `get_contact` (about, photo, business, groupes communs), `check_whatsapp` |
| Envoi | `send_message` (reply, mentions), `send_media`, `send_voice_note`, `send_location`, `send_contact`, `send_poll`, `vote_poll`, `forward_message` |
| Actions message | `react`, `edit_message`, `delete_message`, `star_message`, `mark_read`, `send_presence` |
| Chat | `update_chat` (archive, pin, mute, non lu, timer éphémère) |
| Groupes | `create_group`, `get_group`, `update_group` (nom, sujet, photo, annonce, verrou, approbation), `update_group_participants`, `group_invite` (get, reset, preview, join), `handle_join_requests`, `leave_group`, `link_subgroup` |
| Compte | `blocklist`, `privacy_settings`, `set_profile` |
| Médias | `get_media` (télécharge et rend le fichier brut : chemin local + lien resource, contenu inline `AudioContent` / `ImageContent` sous un plafond de taille), `request_history` (task) |
| Plus tard | newsletters, statuts, labels business, `annotate_message` (l'agent range sa transcription ou sa description pour la rendre cherchable) |

Règles : ordre déterministe, `readOnlyHint` / `destructiveHint` / `idempotentHint` exacts, erreurs typées exploitables par le modèle, pagination par curseur opaque. Paramètre `account` facultatif avec un seul compte, obligatoire dès qu'il y en a plusieurs (même convention que mcp-insta).

### Resources

- Fixes : `whatsapp://accounts`
- Templates : `whatsapp://{account}/status`, `whatsapp://{account}/chats`, `whatsapp://{account}/pairing/qr` (PNG), `whatsapp://{account}/chat/{jid}`, `whatsapp://{account}/chat/{jid}/messages{?before,limit}`, `whatsapp://{account}/message/{chat}/{id}`, `whatsapp://{account}/media/{chat}/{id}`, `whatsapp://{account}/contact/{jid}`, `whatsapp://{account}/group/{jid}`
- Completions sur `{jid}` par nom de contact ou de groupe.
- `subscriptions/listen` : `resources/updated` sur un chat à chaque nouveau message, `resourcesListChanged` à l'apparition d'un chat.
- `ttlMs` court, `cacheScope: private` partout.

### Prompts

`catch_up` (rattrapage des non lus), `summarize_chat`, `draft_reply` (dans mon style, à partir de mes messages passés), `daily_digest` (groupes), `extract_actions` (tâches, dates, rendez-vous).

### Elicitation (MRTR) et garde-fous d'écriture

- Par défaut, la confirmation des écritures est laissée à l'hôte (c'est son rôle, une elicitation en plus ferait double validation). Les annotations de risque doivent donc être exactes.
- L'elicitation sert à ce que l'hôte ne peut pas deviner : désambiguïsation du destinataire, saisie du numéro pour `PairPhone`.
- `WA_CONFIRM=elicit` active la confirmation côté serveur (envoi, suppression, sortie de groupe, participants) pour un hôte sans politique d'approbation.

### Tasks et progression

`request_history` et les gros téléchargements renvoient un handle de task, avec `notifications/progress`.

## 4. Ce que le serveur fait lui-même (sans IA)

- **Rendu de conversation compact** : noms résolus, citations, réactions en ligne, médias en marqueurs adressables `[vocal 0:42, id=...]`, `[image, id=...]`. L'agent appelle `get_media` sur l'id quand il veut le contenu.
- **Médias rendus bruts** : le vocal sort en OGG Opus tel que reçu, l'image telle que reçue. Transcription et description sont le travail de l'agent.
- **Envoi de vocaux** : l'agent fournit un fichier OGG Opus, le serveur calcule durée et waveform.
- **Médias expirés** : media retry piloté par événement (`events.MediaRetry`).
- **Recherche** : FTS5 + filtres (chat, expéditeur, période, type, avec média).

## 5. Sécurité

- Trifecta létale : le contenu des messages est une entrée non fiable. Il est délimité et marqué comme tel dans les sorties, jamais interprété.
- Modes `read-only`, allowlist / denylist de chats, limite de débit sur les envois (risque de ban, API non officielle).
- Sandbox des chemins médias (limités au `data_dir` et aux `roots` déclarés par l'hôte).
- Pas de persistance des `view once`. Chiffrement au repos (SQLCipher) en option.
- Logs sur stderr, OpenTelemetry optionnel, aucun contenu de message dans les logs.

## 6. Phases

| Phase | Contenu | Critère de sortie |
|---|---|---|
| P0 Spike | `bridge.proto`, bridge Go minimal sur pipes (connect, QR, `PairPhone`, flux d'événements, envoi texte, 2 comptes), `wa-mcp` qui supervise et affiche les événements | Appairage par code, message reçu décodé par prost, ack après commit validé |
| P1 Ingestion | Schéma, history sync, LID/PN, contacts, groupes, réactions, éditions, suppressions, receipts, FTS | Resync complet idempotent, proto brut relisible |
| P2 MCP lecture | Tools de lecture, `get_media`, resources, templates, completions, prompts | Utilisable par un agent en lecture seule, vocal récupérable en fichier |
| P3 MCP écriture | Envois, actions message, mark read, presence, elicitation | Annotations de risque correctes sur chaque tool |
| P4 Groupes et compte | Gestion complète des groupes, invitations, communautés, profil, blocklist, privacy, app state | Parité avec le gap analysis de rodrigopg, et au-delà |
| P5 Live | `subscriptions/listen`, media retry, tasks (`request_history`) | Notification live sur un chat, backfill suivi par task |
| P6 Packaging | Archive 2 binaires (cargo-dist), `wa-mcp doctor`, CI Go + Rust | Installation en une commande, sans Python, sans service système |

Tests : faux bridge en Rust (mêmes trames) pour l'intégration (rejoue des événements enregistrés), snapshots `insta` sur le rendu, `proptest` sur le parsing des JID et curseurs, gauntlet `fmt` / `clippy -D warnings` / `nextest` / `cargo deny`.

## 7. Risques

- Dérive des `.proto` whatsmeow entre Go et Rust : sync épinglé au commit de `go.mod`, test de CI qui compare.
- Ban ou changement de protocole WhatsApp : bridge mis à jour indépendamment du reste grâce au contrat protobuf.
- Support client inégal de `2026-07-28` (listen, MRTR, tasks) : chaque fonction a un repli en tool simple.
- Distribution d'un binaire Go à côté du binaire Rust : traité en P6, l'alternative FFI (c-archive) est écartée (runtime Go dans le process, signaux, cross-compilation).
- Un hôte qui ferme le serveur quand il est inactif coupe la session WhatsApp : l'hôte doit le garder ouvert (dans Pénélope, `lazy_start = false`).

## 8. Décisions

| Sujet | Décision |
|---|---|
| Périmètre | Serveur agnostique : WhatsApp + MCP, aucune IA embarquée |
| Transcription, vision, résumé | Hors serveur, faits par l'agent à partir des médias bruts |
| Process | Mono-process stdio, lance lui-même le bridge, verrou par compte |
| IPC bridge | Pipes stdin/stdout, trames protobuf |
| Comptes | Multi-comptes dès P0, un répertoire par compte |
| Confirmations | Politique de l'hôte par défaut, elicitation pour désambiguïser et appairer |
| Transport HTTP, daemon, launchd | Abandonnés |
| Newsletters, statuts, `annotate_message` | Après P5 |

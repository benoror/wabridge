# wabridge vs wacli

[wacli](https://github.com/openclaw/wacli) (MIT, Go; originally `steipete/wacli`, now
`openclaw/wacli`) is the closest sibling: also a whatsmeow linked-device client written in Go. The
two solve different problems. wacli is a full WhatsApp CLI with a searchable local store; wabridge
is a narrow, durable inbox/outbox loop for an agent that is not always awake.

Sources (read 2026-11-13): wacli [README](https://github.com/openclaw/wacli/blob/main/README.md),
[docs/sync.md](https://github.com/openclaw/wacli/blob/main/docs/sync.md),
[docs/integrations.md](https://github.com/openclaw/wacli/blob/main/docs/integrations.md),
[docs/doctor.md](https://github.com/openclaw/wacli/blob/main/docs/doctor.md),
[CHANGELOG.md](https://github.com/openclaw/wacli/blob/main/CHANGELOG.md), and the source tree at
`main` (latest release v0.26.0, 2026-11-05).

## Feature table

| | wabridge (this repo) | wacli |
|---|---|---|
| Protocol library | whatsmeow | whatsmeow |
| Build | pure Go (`modernc.org/sqlite`), builds with `CGO_ENABLED=0` | cgo + C compiler (`go-sqlite3`, `sqlite_fts5` tag) |
| Distribution | build from source | Homebrew, release archives (macOS/Linux/Windows), Docker |
| Accounts | one process per number (`--profile`) | named accounts, each an isolated store (`--account`) |
| Message storage | append-only `inbox.jsonl` per profile | SQLite store (`wacli.db`) with full-text search |
| History backfill / search | no | yes (`history`, `messages search`) |
| Agent hand-off | claims with expiring leases, `done`/`release`, `last_handled` cursor | read via CLI `--json` or read-only SQLite; no claim/lease concept |
| Webhook | content-free wake ping (count, timestamp, chat ids), Bearer auth | message payload (`sync --webhook`), optional `X-Wacli-Signature` HMAC-SHA256 |
| Webhook delivery | debounced, 60 s cooldown, deferred not dropped, retried while lines are pending | best-effort; "retries/backoff are intentionally out of scope" (docs/sync.md) |
| Dedupe | persisted chat+id set (`seen.log`) | store-level (upserts into `wacli.db`) |
| Host suspend / resume | frozen-gap detection, ~1 s reconnect on resume, hard deadlines, wedge exit | not a stated goal; has `--max-reconnect`, `--stale-threshold` on sync |
| Outbound | outbox files with stable ids and id-reusing retries; text, reply, forward, image, document, voice note (local TTS), typing | `send text` / `send file` (up to 2 GiB with `--max-size-mb`), delegation to a running `sync` |
| Send pacing | none (see issue #1) | `--send-spacing DURATION|MIN-MAX` on sync |
| Voice transcription | local whisper.cpp | no |
| Read-only mode | no (proposed) | `--read-only` / `WACLI_READONLY=1` |
| Health / doctor | `status.json` heartbeat, `ensure.sh --check` | `wacli doctor [--connect] [--json]` |
| Explicit errors for outdated client / temporary ban | no (proposed) | handled in `internal/app/session_state.go`, `internal/wa/client.go` |
| Dependency policy | none written (proposed) | 48-hour cooldown before taking dependency updates (CHANGELOG entries) |
| MCP | no (issue #2) | none built in; docs describe a read-only wrapper pattern |
| Tests | unit tests + replay soak suite over synthetic fixtures | large unit/regression suite, CI, CodeQL |

## Gaps in wabridge this table points to

- Explicit handling for outdated client, temporary ban and connect failure.
- A read-only `doctor` command and a read-only mode.
- A written dependency-update delay (wacli uses 48 h).
- Send pacing / warm-up for new numbers (issue #1).
- Release binaries (issue #9).

## Where wabridge is deliberately different

- The webhook never carries message content, and it is a wake hint, not the delivery channel.
- Delivery is at-least-once with dedupe and leases, so a crashed or paused consumer loses nothing.
- Recovery from host pause/resume is a first-class feature with regression tests.

## Credits

- [whatsmeow](https://github.com/tulir/whatsmeow) by Tulir Asokan and contributors implements the
  WhatsApp Web multi-device protocol; wabridge is a thin loop on top of it.
- [wacli](https://github.com/openclaw/wacli), created by @steipete, inspired the error-handling,
  read-only, doctor and dependency-delay items on our roadmap.

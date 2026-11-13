# Architecture

wabridge is one Go binary (`package main`, built on [whatsmeow](https://github.com/tulir/whatsmeow))
that links to one WhatsApp number as a linked device. One process per number (`wabridge run
--profile NAME`); all state for a profile lives in `~/.whatsapp-bridge/NAME/` (mode 700).

The design splits into a **generic core** (useful to any agent platform, or any program that wants
a durable WhatsApp inbox) and a thin **Grok Bot-specific edge** (how the agent gets woken up).

```
WhatsApp ──ws──▶ wabridge run ──append──▶ inbox.jsonl ◀──claim/done── agent
                    │   ▲                                               │
                    │   └──────────── outbox/<id>.json ◀──send──────────┘
                    └──POST (no text, Bearer)──▶ agent platform webhook ──▶ starts an agent turn
```

## Generic core

| Part | Files | What it does |
|---|---|---|
| Inbound pipeline | `pipeline.go`, `edits.go`, `media.go`, `polls.go`, `transcribe.go`, `extras.go` | Turns whatsmeow `events.Message` into one JSON line per message. Media is downloaded *before* the line is written. Edits, deletes, reactions (including encrypted), poll votes, contacts and locations get real `type`s; unknown kinds become `"type":"unsupported"` with a `subtype` (never content). Voice notes get a later `transcript_update` line from local whisper.cpp. |
| Dedupe | `dedupe.go` | `SeenSet`: bounded (5000 entries, 24 h) set keyed by normalized chat JID + message id, persisted to `seen.log`, marked only *after* the inbox write. Absorbs reconnect replays, retry receipts and offline-sync duplicates. |
| Offline / delayed | `replay.go` | Messages from offline sync are flagged `offline` / `delayed` but are never dropped: they are new, claimable and announced like anything else. |
| Inbox claims and leases | `inbox.go`, `inbox_cmd.go`, `groups.go` | `wabridge inbox claim` atomically (flock on `claims/state.lock`) takes every complete line after the claimed byte offset and returns a lease (`lease_id`, deterministic `msg_id` for the ack, `expires`, `messages`, `groups`). `done` finishes it; `release` or TTL expiry re-offers the whole range. A second cursor, `last_handled` (`{"id","ts"}`), lets simple bots advance without claiming; the later of the two wins. `inbox lock/unlock` is an optional single-run lock (`claims/run.json`). |
| Outbox | `outbox.go`, `compose.go`, `attach.go`, `voice.go`, `resolve.go`, `presence.go` | Producers drop `outbox/<id>.json` atomically; the running bridge polls every 500 ms, checks health (and reconnects) before sending, retries reusing the same WhatsApp message id, and moves the request to `outbox/sent/` or `outbox/failed/` with the result. Supports quoted replies, forwards, images, documents, voice notes (local TTS with a fallback chain), typing presence, and phone number → JID resolution. `wabridge send` uses the outbox when `run` holds `run.lock`, otherwise connects directly. |
| Notice scheduling | `outbox.go` (`Notifier`, `Settler`), `pipeline.go` | Decides *when* "something is pending" is true and fires a notice: debounce 10 s, cooldown 60 s (deferred, never dropped), settle check 15 s after every connect / 3 s after offline sync, a notified watermark (`claims/notified.json`) so reconnects never re-announce, expired leases announced once per lease id (periodic check every 30 s, at most 3 times per range), failed POSTs retried up to 5 times while lines are pending. |
| Health | `status.go` | `status.json` rewritten every 30 s: heartbeat `ts`, `state` (starting, connected, disconnected, stream_replaced, logged_out), `last_frame_at`, `last_event_at`, `last_message_at`, `reconnects`, `last_reconnect`, `last_gap`. |
| Watchdog and pause recovery | `watchdog.go`, `sockdrop.go`, `transport.go`, `replaced.go` | 1 s tick. Forces a reconnect after 2+ keepalive timeouts, 60 s disconnected, or 90 s of transport silence. Detects a frozen process by wall-clock gaps between ticks (host paused) and reconnects at once on resume. Every dial / connect / disconnect has a hard deadline; a stalled graceful close is cut by hard-dropping the TCP socket after 150 ms. If the whatsmeow socket lock is wedged, or reconnecting fails for 5 min, the process exits for its supervisor. `stream_replaced` retries with a 60 s → 30 min backoff. |
| Supervision | `scripts/supervise.sh`, `scripts/ensure.sh`, `scripts/timer.sh` | Restart-on-exit loop per profile; idempotent check-and-start that never restarts a running supervisor; an optional timer that runs ensure and writes `timer.check`. |
| Tests | `*_test.go`, `testdata/replay/`, `scripts/soak.sh` | Unit tests plus a replay suite that drives synthetic whatsmeow events through the real inbound path with a fake clock and a fake webhook. `scripts/soak.sh` (vet + replay + all tests) must print `soak: OK` before a live restart. |

Nothing in the core knows about any particular agent platform. A consumer only needs the files and
the `wabridge inbox` / `wabridge send` CLI.

## Grok Bot-specific edge

| Part | Where | Notes |
|---|---|---|
| Wake ping transport | `Notifier.Post` in `outbox.go`, `loadWebhook` / `loadWebhookKey` | One HTTP POST to `<profile>/webhook_url` with `Authorization: Bearer <webhook_key>` — the shape Grok Bot routine webhooks accept. Up to 3 attempts; 401/403 are not retried. URL and key are never logged. |
| Ping body | `Notifier.flush` | `{"profile","count","latest_ts","chats":[...],"inbox_path"}`. No message text, captions, media or sender names. It **does** include the chat JIDs that have pending lines and the local inbox path (see the wake contract for the plan to make these optional). |
| Routine hand-off | outside this repo | The webhook starts an agent turn; the agent then runs `wabridge inbox claim`, replies via `wabridge send` (with the lease's `msg_id` for idempotent acks), and calls `inbox done`. `ensure.sh` is meant to be the first step of each run. |
| Alerts | `scripts/timer.sh` | Optional: posts `{"alert":"bridge-timer",...}` (no message content) to the webhook of `$ALERT_PROFILE`. |

Swapping in another platform means replacing only the POST target/auth; see
[WAKE-CONTRACT.md](WAKE-CONTRACT.md).

## Non-goals (today)

No searchable message history, no multi-account in one process, no HTTP listener, no MCP server.
See [COMPARISON.md](COMPARISON.md) and the issue tracker.

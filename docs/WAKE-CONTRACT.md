# Wake contract

A small, platform-neutral contract between wabridge (the **bridge**) and whatever runs the agent
(the **host**). The bridge never pushes message content to the host; it only says "something is
waiting". The agent then pulls and acknowledges through local files / CLI.

Status legend: **[built]** works today; **[proposed]** not implemented yet.

## 1. Wake ping (bridge → host)

**[built]** An HTTP `POST` to the URL in `~/.whatsapp-bridge/<profile>/webhook_url`.

- Headers: `Content-Type: application/json`; `Authorization: Bearer <key>` if
  `<profile>/webhook_key` exists. Both files are mode 600 and never logged.
- Body:

  ```json
  {"profile": "alice", "count": 2, "latest_ts": "2026-01-01T12:00:00Z",
   "chats": ["111@s.whatsapp.net"], "inbox_path": "/home/user/.whatsapp-bridge/alice/inbox.jsonl"}
  ```

  - No message text, captions, emoji, media, media keys or sender names. Ever.
  - `chats` (JIDs with pending lines) and `inbox_path` **are** included today.
- Timing: debounced 10 s after an inbound line; at least 60 s between pings (a ping due inside the
  window is deferred, never dropped); a settle check after each (re)connect and offline sync; a
  periodic 30 s check for expired leases.
- Delivery: up to 3 attempts per ping; 2xx = delivered; 401/403 = stop (not retried); other
  failures are retried after the cooldown up to 5 times while lines are still pending.
- Idempotent by design: a duplicate or spurious ping is harmless because the host always asks the
  inbox what is actually pending. A ping with `count: 0` only comes from `wabridge notify-test`
  (`"test": true`).
- **[proposed]** A "minimal" mode that sends only `{"profile","count","latest_ts"}` (drop `chats`
  and `inbox_path`), so the ping carries no identifiers at all. Pairs with the content-free test
  guard in issue #7.
- **[proposed]** Pluggable auth (HMAC signature as an alternative to Bearer) for hosts that expect it.

Host obligations: answer 2xx quickly (do the work asynchronously), treat the ping as a hint only,
and start one agent turn.

## 2. Read and claim (agent → bridge), via CLI

**[built]** All commands print one JSON object.

| Step | Command | Result |
|---|---|---|
| Claim | `wabridge inbox claim --profile P [--ttl 10m]` | `{"count":0,"lease_id":""}` when nothing is pending; otherwise `{"count","lease_id","msg_id","expires","messages":[...],"groups":[...]}`. Takes every complete line after the claimed offset, or re-offers a released/expired lease whole. |
| Reply | `wabridge send --profile P --to JID --text ... --msg-id <lease msg_id> [--chunk N] [--reply-to ID]` | Reusing the lease's `msg_id` makes a retried ack land once. |
| Finish | `wabridge inbox done --profile P --lease ID` | Lines are handled. |
| Give back | `wabridge inbox release --profile P --lease ID` | Lines are re-offered to the next claim (not re-announced). |
| Optional run lock | `wabridge inbox lock --profile P --owner NAME [--ttl]` / `unlock --token T` | Exit code 3 = another run holds it. |

If the agent crashes before `done`, the lease expires (TTL) and the bridge announces it again
(once per lease id, at most 3 times per range), so nothing is lost.

## 3. Read without claiming (simple bots)

**[built]** Read `inbox.jsonl` directly and write `<profile>/last_handled` = `{"id","ts"}` of the
newest line handled. The bridge counts only lines after the later of the claim offset and
`last_handled` as pending.

## 4. Inbox line format

**[built]** One JSON object per line, append-only. Common fields: `id`, `chat`, `sender`,
`push_name`, `from_me`, `is_group`, `ts`, `type`, `text`, plus type-specific fields (`media_path`,
`edited_id`, `deleted_id`, `reacted_id`, `emoji`, `transcript`, `offline`, `delayed`, ...). Update
lines (`media_update`, `transcript_update`) patch an earlier id and are never pending on their own.
Full reference: [USAGE.md](USAGE.md).

## 5. Health (host → bridge, read-only)

**[built]** `<profile>/status.json`; a heartbeat `ts` older than ~2 minutes means the process is not
running or the host was paused. `scripts/ensure.sh --check` reports per profile without starting
anything.

**[proposed]** `wabridge doctor` (read-only, all profiles, JSON) — see issue drafts.

## 6. Proposed: transport adapters

**[proposed]** Keep the CLI/file protocol as the source of truth and add thin adapters on top:
an MCP server over claim / send / done (issue #2) and a generic webhook adapter config
(`webhook.json`: url, auth scheme, body mode) so non-Grok hosts can plug in without code changes.

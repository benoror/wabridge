# wabridge usage reference

Minimal WhatsApp linked-device bridge (Go + whatsmeow). One profile = one number.
State lives in `~/.whatsapp-bridge/<profile>/` (chmod 700): `session.db` (keys, never commit), `inbox.jsonl`, media, logs, and profile files `webhook_url` / `webhook_key` (names only in docs; never print values).

## Build / run / restore

- **Go:** 1.26+ (see go.mod). `go build -o wabridge .` (binary is gitignored). Supervisors exec `./wabridge` beside these sources.
- **Soak before any live restart:** `scripts/soak.sh` must print `soak: OK`.
- **Ensure / supervise:** `scripts/ensure.sh` (from `~/.whatsapp-bridge/autostart`); `scripts/supervise.sh <profile>` keeps one profile alive.
- **Re-pair:** needs the phone that owns the number. `./wabridge pair --profile <p> --phone <international digits>` prints an 8-character code.
- **Examples below use synthetic numbers only** (`5218112345678`, `528112345678`). Never put real phone numbers in tracked files.

---

## Build
    go build -o wabridge .

Before restarting any live profile, run `scripts/soak.sh` (go vet + the replay suite in
`testdata/replay/*.json` + all tests); restart only if it ends with `soak: OK`. Replay fixtures
are synthetic (fake JIDs `111@s.whatsapp.net`, `120363000000000000@g.us`, placeholder text; the
suite rejects other JIDs); the format is documented at the top of `replaysoak_test.go`.

## Pair (one-time per profile)
    ./wabridge pair --profile alice
Open `alice-qr.png` (current directory, or `--qr PATH`) and scan from WhatsApp > Linked devices.
The PNG is rewritten each time the code rotates (first ~60s, then ~20s each); it is deleted on success/timeout.
Re-run if it times out.

No second screen? Link with a phone number instead:

    ./wabridge pair --profile alice --phone <international digits>

It prints an 8-character code; on the phone open WhatsApp > Linked devices > Link a device >
"Link with phone number instead" and type it (WhatsApp also shows a notification). Re-run if it expires.

## Run (receive)
    ./wabridge run --profile alice
Appends each inbound message as one JSON line to `~/.whatsapp-bridge/alice/inbox.jsonl`. Auto-reconnects.
Exits if the device is logged out (re-pair).

### Edits and deletes
An edited message is appended as its own line `{"id": <edit's id>, "type": "edit", "edited_id": <original id>,
"text": <full new text or caption>, "chat", "sender", "from_me", "ts", ...}`: a correction that replaces the earlier
message `edited_id` (it is claimed and notified like a normal inbound message). A delete-for-everyone is
`{"type": "delete", "deleted_id": <original id>, ...}` (no text): treat the earlier message as withdrawn.
Other WhatsApp protocol messages (key shares, history-sync notices, disappearing-message settings) and
edits with no text are not written (logged as `inbox: skipped ...`). Lines before 2026-10-17
show edits/deletes as empty `"type":"text"` lines.

### Reactions
A reaction is appended as `{"id": <reaction's id>, "type": "reaction", "reacted_id": <id of the message
reacted to>, "reacted_from_me": <bool: the reacted message was ours>, "reacted_sender": <its sender, groups
only>, "emoji": "...", "chat", "sender", "push_name", "from_me", "is_group", "ts", ...}` (no `text`). An empty
`emoji` means the sender removed their reaction and the line also carries `"removed": true`; a newer reaction
from the same sender to the same message replaces the older one. Reactions with no target id are not written
(log `inbox: skipped reaction without target id`).
Reactions from someone else wake the routine (2026-10-25, reversing the 2026-10-19 recorded-only rule: a
reaction can be an acknowledgement or more, and the bot decides): they are pending/unhandled, claimable and
announced to the webhook like any message, so a reaction alone starts a run. They are never grouped by
`inbox claim` and never a `--reply-to` target (the bot reads them in the lease's `messages`, like poll votes).
Your own reactions (`from_me`) stay recorded-only. Encrypted reactions (`encReactionMessage`) are decrypted
with whatsmeow's `DecryptReaction` and written as plain reactions; if decryption fails the line is written as
`"type":"unsupported","subtype":"encReactionMessage"` (still wakes the routine, no emoji).
Messages that WhatsApp labels `text` but carry no text and no media (contact, location, pin, event, ...) are
written with a real type (`contact`, `location`, `poll`, ...) or `"type":"unsupported"` plus a `subtype` field
name (never content).

**Poll votes (exception, 2026-10-20):** a bot can mark one of its own sent messages as a poll, either at
send time with `wabridge send --poll ...` (bridge must be running) or afterwards with
`wabridge poll --profile NAME --message-id ID`. Marks live in `<profile dir>/polls.txt` (one id per line, re-read on
every reaction, so no restart). A reaction from someone else to a marked message is written with
`"type": "poll_vote"` instead of `"reaction"` (same fields, removals included with `"removed": true`), and a
poll_vote is a normal handleable line: pending, claimable and announced, so it wakes the routine. The bot's own
reactions stay recorded-only; other reactions wake the routine as above. A poll_vote is never a `--reply-to`
target; quote the poll message itself. Fixture: `testdata/replay/07_poll_vote_wakes.json`.
### Media
Image, document (incl. PDF), video, audio and sticker messages (inbound and from_me) are downloaded
*before* their inbox line is written (and before the webhook counts them) to
`~/.whatsapp-bridge/<profile>/media/<msgid>.<ext>` (dir 700, files 600). The raw message proto is kept
as `media/<msgid>.msg.pb` so media can be re-downloaded later. Extra inbox fields:
`media_path`, `mime`, `caption`, `file_name` (documents), `download_error` (only on failure).
If the download fails with 404/410 the bridge asks the phone to re-upload (media retry receipt);
when the phone answers, a line `{"id","chat","type":"media_update","ts","media_path"|"download_error",...}`
is appended for the same message id. Media received before this feature is not recoverable
(whatsmeow doesn't store message protos/media keys).

## Send (only when your sending policy allows it)
    ./wabridge send --profile alice --to 5218112345678 --text "hi"
`--to` accepts a phone number (digits, optional leading `+`) or a full JID (`...@s.whatsapp.net`, `...@g.us`, `...@lid`).
A bare number is resolved by the bridge at send time with an IsOnWhatsApp query to its canonical JID
(e.g. MX `52444...` may become `521444...@s.whatsapp.net`); unregistered numbers fail with "number not on WhatsApp".
Full JIDs are used unchanged.
- If `run` is active (it holds an flock on `<profile>/run.lock`, pid inside), `send` drops the message
  into the outbox and waits up to 30s for the result; prints the result JSON, exits 1 on failure/timeout.
- If not running, `send` connects directly (old behavior).
Every successful send is appended to `inbox.jsonl` with `from_me: true` and `via: outbox|direct`.

### Images and documents (need the bridge running; go through the outbox)
    ./wabridge send --profile bob --to 528112345678 --image /path/ticket.jpg --caption "texto"
    ./wabridge send --profile bob --to 528112345678 --document /path/CSF.pdf --file-name "CSF.pdf" [--caption "texto"]
- `--image`: JPEG/PNG up to 16 MB, uploaded byte-for-byte (never recompressed) as a photo, with
  width/height and a small JPEG thumbnail when decodable. Other types (or bigger files): use `--document`.
- `--document`: any file up to 100 MB; `--file-name` is what the recipient sees (default: basename),
  mimetype by extension (falls back to content sniffing, then application/octet-stream), PDF page count best-effort.
- `--caption` (or `--text`, which becomes the caption when an attachment is given). Works with
  `--reply-to` and `--msg-id`/`--chunk` dedupe. One attachment per message. Paths are made absolute
  and read by the running bridge at send time, so keep the file in place until the result prints.
- Files are validated (exists, readable, regular, non-empty, size cap) both by `send` and by the bridge;
  an invalid outbox item goes to `outbox/failed/` with the error. The inbox.jsonl echo is
  `type: "image"|"document"`, `media_path`, `file_name`, `text` = caption.

### Outbox protocol (for bots)
Write `~/.whatsapp-bridge/<profile>/outbox/<id>.json` = `{"id","to","text"}` atomically
(write a dotfile / non-`.json` tmp, then rename). `run` polls every 500ms, sends via its live
connection, and moves the request to `outbox/sent/<id>.json` or `outbox/failed/<id>.json` as
`{id,to,text,ok,message_id,error,ts}`.
Optional fields: `msg_id`, `reply_to`, `voice` (prepared OGG/Opus; use `send --voice`), and attachments:
`{"id":"f1","to":"528112345678","image":"/abs/path/ticket.jpg","caption":"texto","msg_id":"..."}`,
`{"id":"f2","to":"528112345678","document":"/abs/path/CSF.pdf","file_name":"CSF.pdf","caption":"texto"}`.

## Notify (webhook)
Put a URL in `~/.whatsapp-bridge/<profile>/webhook_url` (chmod 600; never logged). Read at `run` start
(restart to change). On inbound non-from_me messages, `run` debounces 10s and POSTs
`{"profile","count","latest_ts","chats":[...],"inbox_path"}` (no message text), up to 3 attempts.
The bot then reads `inbox.jsonl` itself. No file = no notifications.

Notices are at least 60s apart; a notice that falls inside that window is deferred to when it
opens, never dropped. A failed POST (5xx/network) is retried after the cooldown (up to 5 times)
while unhandled lines are still pending.

Offline / delayed delivery: messages that arrived while the bridge or box was down are delivered
on reconnect. Their lines carry `"offline": true` (inside OfflineSyncPreview..Completed) and/or
`"delayed": true` (older than connect-60s). Both are informational only: such messages are new,
claimable by `inbox claim` and notified like any other. Offline-sync lines don't notify one by one;
the bridge checks once the sync settles (3s after the last offline line / OfflineSyncCompleted).
After every connect (including process start) it also checks after 15s: if `inbox claim` would
return anything (lines after the claimed offset, or released/expired leases), it fires a notice
(log: `webhook: N pending after ...; notice scheduled in ...`, then `webhook: notice sent`).
"Unhandled" honours both cursors: the claim offset and `last_handled` (`{"id","ts"}`, the newest
message a bot handled without claiming; the later of the two wins, for `claim` too). The settle
check (and the failed-notice retry) also only counts lines past the notified watermark
(`claims/notified.json`, moved to the end of the inbox after every successful notice), so a
reconnect never re-announces lines a notice already covered. Expired leases (a run claimed and died
before `done`) are the exception: each expired lease is announced once by lease id, by the
settle check or by a quiet periodic check every 30s (no reconnect needed; log `webhook: N
line(s) in expired lease(s)`), so a re-claimed range that dies again is announced again, at most
3 times per range (a message that kills every run stops waking the routine but stays claimable).
Released leases (`inbox release`) are not re-announced. 0 = no notice (log:
`settle check: N pending (U unhandled, already announced A)`).
The only things ever skipped: true duplicates (same chat+id already written, `seen.log`, marked
after the inbox write) and inbox lines already claimed. Lines written before 2026-10-17 may carry the
old name `"replay": true`; treat it like `delayed`.

Auth: if `~/.whatsapp-bridge/<profile>/webhook_key` exists (chmod 600; never logged), every POST
carries `Authorization: Bearer <key>` (e.g. what Grok Bot routine webhooks expect; 401 otherwise).
401/403 are not retried.

Test: `wabridge notify-test --profile <p>` sends one `{"profile","test":true,"count":0,"chats":[],"inbox_path"}`
POST (wakes the routine once) and prints the status. Doesn't touch WhatsApp; safe while `run` is active.

## Run as service
nohup:
    nohup ./wabridge run --profile alice >> ~/.whatsapp-bridge/alice/bridge.log 2>&1 &

systemd --user (`~/.config/systemd/user/wabridge@.service`):
    [Unit]
    Description=wabridge %i
    [Service]
    ExecStart=/path/to/wabridge run --profile %i
    Restart=always
    RestartSec=10
    [Install]
    WantedBy=default.target
Then `systemctl --user daemon-reload && systemctl --user enable --now wabridge@alice`.

## Caveats
Unofficial client: violates WhatsApp ToS; the number can be banned. Avoid bulk/automated sending.

## Watchdog and supervisor

`run` watches the socket: after 2+ consecutive keepalive timeouts, or >60s
disconnected, it forces Disconnect()+Connect() with backoff (5s doubling to 60s)
and logs one `watchdog: reconnected (...)` line. If reconnecting fails for
>5 min it exits 1. SIGTERM/SIGINT exit within ~5s even if the websocket close hangs.

Without systemd, use the shell supervisor (restarts 5s after any exit,
appends to `~/.whatsapp-bridge/<profile>/bridge.log`):

    setsid nohup scripts/supervise.sh alice >/dev/null 2>&1 &   # start
    kill "$(cat ~/.whatsapp-bridge/alice/supervise.pid)"         # stop (stops bridge too)
    pkill -f 'wabridge run --profile alice'                      # restart bridge only

### Health (`status.json`)

`state` alone is not health: a dead process or a paused host leaves `connected`
behind. The running bridge rewrites `status.json` every 30s (`ts` = heartbeat) with
`started_at`, `connected_at`, `last_frame_at` (last inbound frame, keepalive pongs
included), `last_event_at` (any whatsmeow event), `last_message_at` (last message;
absent = none since start), `reconnects`, `last_reconnect` ("<time> <reason>") and
`last_gap`. States: starting, connected, disconnected, stream_replaced, logged_out.

`ts` older than ~2 min means the bridge is not running or the host was paused.
The host gets paused when idle. Every bridge on it freezes at the same moment, and on
resume the watchdog (1s tick) logs `process was frozen for X` and, if no frame has
arrived since before the pause, reconnects at once with `resumed after X pause,
transport silent for Y`. The old socket is dead by then, so if whatsmeow's graceful
websocket close has not finished within 150ms the watchdog hard-closes the TCP socket
(`graceful close stalled; dropped the socket`) instead of waiting out the 5s close
timeout; resume-to-reconnected is ~1s. Messages sent during a pause arrive in the
offline sync after resume, marked `offline`/`delayed`.

### After a host restart

Nothing restarts the supervisors (no systemd, no cron). `scripts/ensure.sh` is the
idempotent check-and-start. It reads profiles from `~/.whatsapp-bridge/autostart`
(one per line) or from its arguments, never touches a running supervisor, refuses to
start next to an unsupervised `wabridge run`, and prints ok|started|stale|down|blocked
per profile (exit 0 = all ok/started). `--check` only reports.

    scripts/ensure.sh            # call first in each agent run
    scripts/ensure.sh --check

## Non-downloadable "media"

Messages whatsmeow types as media but with nothing to download get a
descriptive `type`: `album` (parent of a photo group; `album_images`,
`album_videos`), `contact`, `contacts`, `location` (`lat`,`lng`),
`live_location`, `poll`; otherwise `media` with `subtype` (proto field name).
Round video notes (ptv) are downloaded like videos.

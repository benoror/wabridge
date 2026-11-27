# Examples

Two small, assistant-neutral programs that implement both sides of the
[wake contract](../docs/WAKE-CONTRACT.md). Standard-library Python only; adapt them to whatever
runs your agent.

| File | Side | What it does |
|---|---|---|
| [`wake_handler.py`](wake_handler.py) | host | Receives the content-free wake ping (`POST /wake`, optional Bearer key), answers `204` at once, and starts one agent run. Pings that arrive during a run trigger exactly one more run afterwards. |
| [`inbox_routine.py`](inbox_routine.py) | agent | One run: `inbox claim`, answer each (chat, sender) group with the group's `ack_msg_id`, then `inbox done`; on any error `inbox release`, so the next run gets the same lines. |

## Try it

```sh
go build -o wabridge .
./wabridge pair --profile work --phone <international digits>

# Host side: start the handler and point the bridge at it
echo "http://127.0.0.1:8787/wake" > ~/.whatsapp-bridge/work/webhook_url
openssl rand -hex 16 > ~/.whatsapp-bridge/work/webhook_key
chmod 600 ~/.whatsapp-bridge/work/webhook_*
WAKE_KEY=$(cat ~/.whatsapp-bridge/work/webhook_key) \
AGENT_CMD="python3 examples/inbox_routine.py --profile work --dry-run" \
  python3 examples/wake_handler.py --port 8787 &

# Bridge
./wabridge run --profile work &
./wabridge notify-test --profile work   # the handler logs a test ping
```

Drop `--dry-run` once `answer()` in `inbox_routine.py` calls your agent. Messages never pass
through the wake handler: the routine reads them from the local inbox through the CLI.

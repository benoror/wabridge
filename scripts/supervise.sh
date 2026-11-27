#!/usr/bin/env bash
# Keep `wabridge run --profile <p>` alive: restart 5s after any exit.
# Start: setsid nohup scripts/supervise.sh <p> >/dev/null 2>&1 &
# Stop:  kill "$(cat ~/.whatsapp-bridge/<p>/supervise.pid)"   (child gets SIGTERM, exits <=5s)
set -u
p=${1:?usage: supervise.sh <profile>}
bin="$(cd "$(dirname "$0")/.." && pwd)/wabridge"
dir="$HOME/.whatsapp-bridge/$p"
log="$dir/bridge.log"
echo $$ > "$dir/supervise.pid"
child=
trap '[ -n "$child" ] && kill -TERM "$child" 2>/dev/null; wait "$child" 2>/dev/null; rm -f "$dir/supervise.pid"; exit 0' TERM INT
while :; do
  "$bin" run --profile "$p" >>"$log" 2>&1 &
  child=$!
  wait "$child"; rc=$?
  echo "$(date '+%F %T') supervise: wabridge exited rc=$rc; restarting in 5s" >>"$log"
  sleep 5
done

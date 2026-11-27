#!/usr/bin/env bash
# Keep `wabridge run --profile <p>` alive.
# - Normal exit/crash: restart with exponential backoff 5s -> 10s -> ... -> 5 min cap; back to 5s after a
#   run that lasted more than 10 min.
# - Logged out (status.json state=logged_out, or the child said "not paired"): a restart cannot fix it, so
#   PARK: write <dir>/supervise.parked (JSON: reason, since, session.db mtime) and stop launching wabridge.
#   While parked, check every minute, without launching wabridge, whether session.db changed (a re-pair
#   writes it) or the marker was removed; either one resumes normal supervision.
#   Re-pair flow: `wabridge pair --profile <p> [--phone <digits>]` -> the parked supervisor
#   relaunches the bridge within ~1 min. If it does not (or no supervisor is running), `rm <dir>/supervise.parked`
#   and/or run scripts/ensure.sh <p>.
# Start: setsid nohup scripts/supervise.sh <p> >/dev/null 2>&1 &
# Stop:  kill "$(cat ~/.whatsapp-bridge/<p>/supervise.pid)"   (child gets SIGTERM; sleeps are interruptible)
set -u
p=${1:?usage: supervise.sh <profile>}
bin="$(cd "$(dirname "$0")/.." && pwd)/wabridge"
dir="$HOME/.whatsapp-bridge/$p"
log="$dir/bridge.log"
park="$dir/supervise.parked"
min=${SUP_BACKOFF_MIN:-5}; max=${SUP_BACKOFF_MAX:-300}; reset_after=${SUP_RESET_AFTER:-600}; poll=${SUP_PARK_POLL:-60}
echo $$ > "$dir/supervise.pid"
child=
trap '[ -n "$child" ] && kill -TERM "$child" 2>/dev/null; wait "$child" 2>/dev/null; rm -f "$dir/supervise.pid"; exit 0' TERM INT
say() { echo "$(date '+%F %T') supervise: $*" >>"$log"; }
nap() { sleep "$1" & child=$!; wait "$child"; child=; }   # interruptible by TERM
smtime() { stat -c %Y "$dir/session.db" "$dir/session.db-wal" 2>/dev/null | sort -n | tail -1 | grep . || echo 0; }
state() { python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("state",""))' "$dir/status.json" 2>/dev/null; }
marker_mtime() { python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("session_mtime",0))' "$park" 2>/dev/null || echo 0; }
park_until_repaired() { # $1 = reason
  if [ ! -f "$park" ]; then
    python3 -c 'import json,sys,datetime as d; json.dump({"reason":sys.argv[1],"since":d.datetime.now().astimezone().isoformat(timespec="seconds"),"pid":int(sys.argv[2]),"session_mtime":int(sys.argv[3])},open(sys.argv[4],"w"))' \
      "$1" "$$" "$(smtime)" "$park"
    say "parked ($1): not restarting; re-pair with: wabridge pair --profile $p"
  fi
  local m0; m0=$(marker_mtime)
  while [ -f "$park" ] && [ "$(smtime)" = "$m0" ]; do nap "$poll"; done
  [ -f "$park" ] && say "session.db changed (re-paired?); resuming" || say "park marker removed; resuming"
  rm -f "$park"
}
delay=$min
# A parked marker from an earlier supervisor (e.g. after a box restart) still holds until session.db changes.
[ -f "$park" ] && park_until_repaired "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("reason","logged_out"))' "$park" 2>/dev/null || echo logged_out)"
while :; do
  t0=$(date +%s)
  "$bin" run --profile "$p" >>"$log" 2>&1 &
  child=$!
  wait "$child"; rc=$?; child=
  ran=$(( $(date +%s) - t0 ))
  # Park when the child said "not paired" (dies before touching status.json) or wrote state=logged_out
  # during this run (LoggedOut event). A stale logged_out from before this launch does not count.
  st_new=0; [ "$(stat -c %Y "$dir/status.json" 2>/dev/null || echo 0)" -ge "$t0" ] && st_new=1
  if tail -n 2 "$log" 2>/dev/null | grep -q '^not paired' || { [ $st_new -eq 1 ] && [ "$(state)" = logged_out ]; }; then
    say "wabridge exited rc=$rc after ${ran}s: logged out / not paired"
    park_until_repaired logged_out; delay=$min; continue
  fi
  [ "$ran" -gt "$reset_after" ] && delay=$min
  say "wabridge exited rc=$rc after ${ran}s; restarting in ${delay}s"
  nap "$delay"
  delay=$(( delay * 2 )); [ "$delay" -gt "$max" ] && delay=$max
done

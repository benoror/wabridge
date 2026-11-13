#!/usr/bin/env bash
# Idempotent "is the bridge up? if not, start it" check, for use after a box restart
# (for hosts without systemd/cron, where nothing restarts supervise.sh on boot).
# Safe to call often (e.g. first step of every agent run); it never stops
# or restarts a running supervisor.
#
#   scripts/ensure.sh [--check] [profile ...]
#
# Profiles default to the lines of ~/.whatsapp-bridge/autostart. --check reports only.
# Per profile prints one line: ok | started | stale | down | blocked, plus status fields.
# Exit 0 when every profile is ok/started, 1 otherwise.
set -u
root="$HOME/.whatsapp-bridge"
repo="$(cd "$(dirname "$0")/.." && pwd)"
check=0
[ "${1:-}" = "--check" ] && { check=1; shift; }
profiles=("$@")
if [ ${#profiles[@]} -eq 0 ] && [ -f "$root/autostart" ]; then
  mapfile -t profiles < <(grep -vE '^\s*(#|$)' "$root/autostart")
fi
[ ${#profiles[@]} -gt 0 ] || { echo "usage: ensure.sh [--check] profile... (or list them in $root/autostart)" >&2; exit 2; }

sup_alive() { # pid profile
  [ -n "$1" ] && kill -0 "$1" 2>/dev/null &&
    tr '\0' ' ' <"/proc/$1/cmdline" 2>/dev/null | grep -q "supervise.sh $2"
}
run_pid() { pgrep -f "wabridge run --profile $1( |\$)" | head -1; }
status_line() { # profile -> "state=.. age=..s last_frame=.. last_msg=.."
  python3 - "$root/$1/status.json" <<'PY' 2>/dev/null || echo "status=unreadable"
import json,sys,datetime as d
s=json.load(open(sys.argv[1]))
def age(k):
    v=s.get(k)
    if not v: return "-"
    t=d.datetime.fromisoformat(v.replace("Z","+00:00"))
    return "%ds" % (d.datetime.now(d.timezone.utc)-t).total_seconds()
print("state=%s hb_age=%s frame_age=%s msg_age=%s reconnects=%s" % (s.get("state"), age("ts"), age("last_frame_at"), age("last_message_at"), s.get("reconnects",0)))
PY
}
hb_age() {
  python3 -c 'import json,sys,datetime as d
s=json.load(open(sys.argv[1])); t=d.datetime.fromisoformat(s["ts"].replace("Z","+00:00"))
print(int((d.datetime.now(d.timezone.utc)-t).total_seconds()))' "$root/$1/status.json" 2>/dev/null || echo 999999
}

rc=0
for p in "${profiles[@]}"; do
  dir="$root/$p"
  if [ ! -f "$dir/session.db" ]; then echo "$p: blocked (no session.db; not paired)"; rc=1; continue; fi
  exec 9>"$dir/ensure.lock"
  if ! flock -n 9; then echo "$p: blocked (another ensure.sh is running)"; rc=1; continue; fi
  pid=$(cat "$dir/supervise.pid" 2>/dev/null || true)
  if sup_alive "$pid" "$p"; then
    age=$(hb_age "$p")
    st_now=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("state",""))' "$dir/status.json" 2>/dev/null)
    if [ "$st_now" = stream_replaced ] || [ "$st_now" = logged_out ]; then
      # Alert only: the bridge retries stream_replaced itself; never restart it from here.
      echo "$p: not ok ($st_now; supervisor $pid alive, no restart) $(status_line "$p")"; rc=1
    elif [ "$age" -gt 180 ]; then
      # Heartbeat is every 30s; old means the child is not running or the box just resumed.
      echo "$p: stale (supervisor $pid alive, status heartbeat ${age}s old) $(status_line "$p")"; rc=1
    else
      echo "$p: ok (supervisor $pid) $(status_line "$p")"
    fi
    flock -u 9; continue
  fi
  stray=$(run_pid "$p")
  if [ -n "$stray" ]; then
    echo "$p: blocked (wabridge run pid $stray is running without a supervisor; not starting a second one)"; rc=1
    flock -u 9; continue
  fi
  if [ $check -eq 1 ]; then echo "$p: down (no supervisor)"; rc=1; flock -u 9; continue; fi
  # exec + closed fds: the supervisor must not inherit this script's stdout pipe (a $(...) caller would
  # never see EOF) nor the ensure/timer lock descriptors (they would keep those locks held forever).
  ( cd "$repo" && exec setsid nohup scripts/supervise.sh "$p" >/dev/null 2>&1 </dev/null 8>&- 9>&- & )
  ok=0
  for _ in $(seq 1 40); do
    sleep 0.5
    st=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("state",""))' "$dir/status.json" 2>/dev/null)
    np=$(cat "$dir/supervise.pid" 2>/dev/null || true)
    if [ "$st" = connected ] && [ "$(hb_age "$p")" -lt 60 ] && sup_alive "$np" "$p"; then ok=1; break; fi
  done
  if [ $ok -eq 1 ]; then echo "$p: started (supervisor $np) $(status_line "$p")"
  else echo "$p: started but not connected within 20s (supervisor ${np:-?}) $(status_line "$p")"; rc=1; fi
  flock -u 9
done
exit $rc

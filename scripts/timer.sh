#!/usr/bin/env bash
# Bridge-only timer. Every INTERVAL seconds (default 300) runs ensure.sh for the given profiles
# (default: the lines of ~/.whatsapp-bridge/autostart; idempotent: starts a down bridge, never
# restarts a running one), writes ~/.whatsapp-bridge/timer.check (JSON, rewritten every pass = the
# timer's own heartbeat), and alerts only when a bridge is not ok/started: one line in
# timer-alerts.log plus, if ALERT_PROFILE is set, a text-free POST to that profile's webhook
# ({"alert":"bridge-timer",...}), at most once per 30 min per bad state.
# Safe to start more than once: a second copy exits at once if the lock is held.
#   Start: ALERT_PROFILE=alice setsid nohup scripts/timer.sh [profile ...] >/dev/null 2>&1 &
#   Stop:  kill "$(cat ~/.whatsapp-bridge/timer.pid)"
#   Once:  scripts/timer.sh --once [profile ...]   (one pass, no alert POST)
set -u
root="$HOME/.whatsapp-bridge"; here="$(cd "$(dirname "$0")" && pwd)"
interval="${INTERVAL:-300}"; alert_profile="${ALERT_PROFILE:-}"
once=0; [ "${1:-}" = "--once" ] && { once=1; shift; }
profiles=("$@")
if [ $once -eq 0 ]; then
  exec 8>"$root/timer.run.lock"
  flock -n 8 || { echo "timer already running (pid $(cat "$root/timer.pid" 2>/dev/null))"; exit 0; }
  echo $$ >"$root/timer.pid"
  trap 'rm -f "$root/timer.pid"; exit 0' TERM INT
fi
last_sig=""; last_alert=0
while :; do
  out=$("$here/ensure.sh" ${profiles[@]+"${profiles[@]}"} 2>&1 8>&- </dev/null); rc=$?
  bad=$(printf '%s\n' "$out" | grep -vE '^[a-z]+: (ok|started) ' | grep -E '^[a-z]+: ' | cut -d: -f1 | sort | tr '\n' ' ' | sed 's/ $//')
  python3 - "$root/timer.check" "$rc" "$bad" "$$" "$interval" <<PY
import json,sys,os,datetime as d
path,rc,bad,pid,iv=sys.argv[1:6]
lines=[l for l in """$out""".splitlines() if l.strip()]
res={l.split(":",1)[0]:l.split(":",1)[1].strip() for l in lines if ":" in l}
doc={"ts":d.datetime.now().astimezone().isoformat(timespec="seconds"),"ok":rc=="0" and not bad,
     "bad":bad.split() if bad else [],"pid":int(pid),"interval_s":int(iv),"results":res}
tmp=path+".tmp"; json.dump(doc,open(tmp,"w"),indent=1); os.replace(tmp,path)
PY
  if [ -n "$bad" ]; then
    now=$(date +%s)
    if [ "$bad" != "$last_sig" ] || [ $((now-last_alert)) -ge 1800 ]; then
      echo "$(date '+%F %T') not ok: $bad" >>"$root/timer-alerts.log"
      if [ $once -eq 0 ] && [ -n "$alert_profile" ]; then
        body=$(python3 -c 'import json,sys; print(json.dumps({"profile":sys.argv[3],"alert":"bridge-timer","bad":sys.argv[1].split(),"count":0,"chats":[],"check_path":sys.argv[2]}))' "$bad" "$root/timer.check" "$alert_profile")
        url=$(cat "$root/$alert_profile/webhook_url" 2>/dev/null); key=$(cat "$root/$alert_profile/webhook_key" 2>/dev/null)
        [ -n "$url" ] && curl -s -o /dev/null -m 20 -X POST -H 'Content-Type: application/json' \
          ${key:+-H "Authorization: Bearer $key"} --data "$body" "$url" || echo "$(date '+%F %T') alert POST failed" >>"$root/timer-alerts.log"
      fi
      last_sig="$bad"; last_alert=$now
    fi
  else last_sig=""; fi
  [ $once -eq 1 ] && exit $rc
  sleep "$interval"
done

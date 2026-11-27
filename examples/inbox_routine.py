#!/usr/bin/env python3
"""Example inbox routine for wabridge (standard library only).

One run: claim everything pending, answer each (chat, sender) group once, then mark the lease
done. If anything fails, release the lease so the next run gets the same lines again.

    python3 examples/inbox_routine.py --profile work [--wabridge ./wabridge] [--dry-run]

Replace `answer()` with your agent. Replies reuse the group's `ack_msg_id`, so a retried run
that answers the same group again is deduplicated instead of sending twice.
"""
import argparse
import json
import subprocess
import sys


def wb(sub, profile, binary, *args):
    """Run `wabridge <sub...> --profile P <args...>` and parse its one-line JSON output."""
    out = subprocess.run([binary, *sub, "--profile", profile, *args],
                         check=True, capture_output=True, text=True).stdout
    return json.loads(out) if out.strip().startswith("{") else {}


def answer(group, messages):
    """Your agent goes here. Return the reply text, or None to stay silent."""
    texts = [m.get("text") or m.get("transcript") or "" for m in messages]
    if not any(t.strip() for t in texts):
        return None
    return "Got your message (%d new)." % len(messages)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--wabridge", default="./wabridge")
    ap.add_argument("--ttl", default="10m")
    ap.add_argument("--dry-run", action="store_true", help="print replies instead of sending")
    a = ap.parse_args()

    lease = wb(["inbox", "claim"], a.profile, a.wabridge, "--ttl", a.ttl)
    if not lease.get("lease_id"):
        print("nothing pending", file=sys.stderr)
        return
    lid = lease["lease_id"]
    by_id = {m.get("id"): m for m in lease.get("messages", [])}
    try:
        for g in lease.get("groups", []):
            msgs = [by_id[i] for i in g.get("message_ids", []) if i in by_id]
            reply = answer(g, msgs)
            if reply is None:
                continue
            if a.dry_run:
                print(json.dumps({"to": g["chat"], "text": reply, "msg_id": g["ack_msg_id"]}))
                continue
            wb(["send"], a.profile, a.wabridge, "--to", g["chat"], "--text", reply,
               "--msg-id", g["ack_msg_id"], "--reply-to", g["last_id"])
    except Exception:
        wb(["inbox", "release"], a.profile, a.wabridge, "--lease", lid)
        raise
    wb(["inbox", "done"], a.profile, a.wabridge, "--lease", lid)


if __name__ == "__main__":
    main()

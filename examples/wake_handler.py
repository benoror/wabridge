#!/usr/bin/env python3
"""Generic wake handler for wabridge (standard library only).

Receives wabridge's content-free wake ping and starts one agent run. It never sees message
content: the ping only says how many lines are pending. The agent pulls the messages itself
with `wabridge inbox claim` (see inbox_routine.py).

    WAKE_KEY=<same value as ~/.whatsapp-bridge/<profile>/webhook_key> \
    AGENT_CMD="python3 examples/inbox_routine.py --profile work" \
    python3 examples/wake_handler.py --port 8787

Then put http://127.0.0.1:8787/wake in ~/.whatsapp-bridge/<profile>/webhook_url.

Contract (docs/WAKE-CONTRACT.md): answer 2xx fast, treat the ping as a hint, start at most one
run at a time. A ping that arrives while a run is going sets a flag, and one more run starts
after it, so nothing waits for the next ping.
"""
import argparse
import hmac
import json
import os
import shlex
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

KEY = os.environ.get("WAKE_KEY", "")
CMD = shlex.split(os.environ.get("AGENT_CMD", "python3 examples/inbox_routine.py --profile work"))

_lock = threading.Lock()
_running = False
_again = False


def _run_loop():
    global _running, _again
    while True:
        subprocess.run(CMD, check=False)
        with _lock:
            if not _again:
                _running = False
                return
            _again = False


def wake():
    global _running, _again
    with _lock:
        if _running:
            _again = True
            return
        _running = True
    threading.Thread(target=_run_loop, daemon=True).start()


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/wake":
            return self._reply(404)
        if KEY:
            got = self.headers.get("Authorization", "")
            if not hmac.compare_digest(got, "Bearer " + KEY):
                return self._reply(401)
        n = int(self.headers.get("Content-Length") or 0)
        try:
            ping = json.loads(self.rfile.read(n) or b"{}")
        except ValueError:
            return self._reply(400)
        self._reply(204)
        if ping.get("test"):
            print("wake: test ping for profile %s" % ping.get("profile"), file=sys.stderr)
            return
        if int(ping.get("count") or 0) > 0:
            wake()

    def _reply(self, code):
        self.send_response(code)
        self.end_headers()

    def log_message(self, fmt, *args):  # keep logs to status codes only
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8787)
    a = ap.parse_args()
    print("wake handler on http://%s:%d/wake" % (a.host, a.port), file=sys.stderr)
    ThreadingHTTPServer((a.host, a.port), Handler).serve_forever()


if __name__ == "__main__":
    main()

#!/usr/bin/env bash
# Pre-restart soak: go vet + the replay suite (testdata/replay/*.json, sanitized fixtures driven
# through the real inbound path) + the full test suite. Touches no live profile, no network
# besides loopback test servers. Run it before restarting any live profile:
#   scripts/soak.sh && go build -o wabridge.new . && mv wabridge.new wabridge
set -euo pipefail
cd "$(dirname "$0")/.."
echo "== go vet"
go vet ./...
echo "== replay suite"
go test -count=1 -run '^TestReplaySoak$' -v . | grep -E '^(\s*--- |\s+replaysoak_test\.go|PASS|FAIL|ok\s)'
echo "== full tests"
go test -count=1 ./...
echo "soak: OK"

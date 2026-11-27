package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// cursorReader is what a cursor-tracking bot does (docs/WAKE-CONTRACT.md section 3): inbound lines
// in inbox.jsonl after the line whose id is in last_handled.
func cursorReader(t *testing.T, ib *Inbox) []string {
	t.Helper()
	inbox := filepath.Join(ib.Dir, "inbox.jsonl")
	start := ib.lastHandledEnd(inbox)
	if start < 0 {
		start = 0
	}
	f, err := os.Open(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	f.Seek(start, 0)
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var m struct {
			ID     string `json:"id"`
			FromMe bool   `json:"from_me"`
			Type   string `json:"type"`
		}
		if json.Unmarshal(sc.Bytes(), &m) == nil && !m.FromMe && !isUpdateLine(m.Type) {
			out = append(out, m.ID)
		}
	}
	return out
}

// A cursor-tracking bot's last_handled lags behind while lines after it are claimed and marked
// done through the bridge. Without a single source of truth, the bot's next read replays them.
func TestClaimDoneMovesLegacyCursorNoReplay(t *testing.T) {
	dir := t.TempDir()
	appendLines(t, dir, 0, 5, false) // M0..M4
	os.WriteFile(filepath.Join(dir, "last_handled"), []byte(`{"id":"M0"}`), 0o600)
	ib := &Inbox{Dir: dir}

	l, err := ib.Claim(time.Minute) // mark the backlog handled through the bridge
	if err != nil || l == nil || fmt.Sprint(ids(l)) != "[M1 M2 M3 M4]" {
		t.Fatalf("claim: %v %v", l, err)
	}
	if err := ib.Done(l.ID); err != nil {
		t.Fatal(err)
	}
	if got := cursorReader(t, ib); len(got) != 0 {
		t.Fatalf("cursor reader replays handled lines %v", got)
	}
	appendLines(t, dir, 5, 1, false) // a new message
	if got := cursorReader(t, ib); fmt.Sprint(got) != "[M5]" {
		t.Fatalf("cursor reader after new line: %v", got)
	}
	if pi, _ := ib.Pending(); pi.Count != 1 {
		t.Fatalf("pending: want 1, got %+v", pi)
	}
	l, _ = ib.Claim(time.Minute)
	if l == nil || fmt.Sprint(ids(l)) != "[M5]" {
		t.Fatalf("claim re-emits handled lines: %v", l)
	}
	ib.Done(l.ID)
	if got := cursorReader(t, ib); len(got) != 0 {
		t.Fatalf("after second done: %v", got)
	}
	// Restart: no state survives in memory; a fresh Inbox must agree.
	ib2 := &Inbox{Dir: dir}
	if l, _ := ib2.Claim(time.Minute); l != nil {
		t.Fatalf("claim after restart re-emits: %v", ids(l))
	}
}

// last_handled pointing at a voice note whose transcript_update arrives later must not jump past
// the lines in between (update lines reuse the original id).
func TestLastHandledIgnoresUpdateLines(t *testing.T) {
	dir := t.TempDir()
	f, _ := os.Create(filepath.Join(dir, "inbox.jsonl"))
	for _, m := range []map[string]any{
		{"id": "V", "type": "voice"},
		{"id": "A", "type": "text"},
		{"id": "V", "type": "transcript_update"},
		{"id": "B", "type": "text"},
	} {
		b, _ := json.Marshal(m)
		f.Write(append(b, '\n'))
	}
	f.Close()
	os.WriteFile(filepath.Join(dir, "last_handled"), []byte(`{"id":"V"}`), 0o600)
	ib := &Inbox{Dir: dir}
	if got := cursorReader(t, ib); fmt.Sprint(got) != "[A B]" {
		t.Fatalf("want [A B] after V, got %v", got)
	}
	l, _ := ib.Claim(time.Minute)
	if l == nil || fmt.Sprint(ids(l)) != "[A V B]" {
		t.Fatalf("claim: %v", l)
	}
}

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Regression: last_handled held a bare id (no JSON) and claims/state.json was
// missing, so Pending started at EOF on every check and reported 0 pending while 10 lines sat
// past the notified watermark; offline-delivered DMs never woke the consumer.
func TestPendingBareLastHandledNoState(t *testing.T) {
	dir := t.TempDir()
	appendLines(t, dir, 0, 3, false) // M0..M2
	os.WriteFile(filepath.Join(dir, "last_handled"), []byte("M1\n"), 0o600)
	ib := &Inbox{Dir: dir}
	if pi, _ := ib.Pending(); pi.Count != 1 {
		t.Fatalf("bare id cursor: want 1 pending (M2), got %+v", pi)
	}
	appendLines(t, dir, 3, 2, false)
	if pi, _ := ib.Pending(); pi.Count != 3 {
		t.Fatalf("new lines after bare cursor: want 3, got %+v", pi)
	}
	l, _ := ib.Claim(time.Minute)
	if l == nil || fmt.Sprint(ids(l)) != "[M2 M3 M4]" {
		t.Fatalf("claim after bare cursor: %v", l)
	}
}

// Unresolvable cursor (id not in inbox) + no state.json: fall back to the notified watermark,
// not EOF, so lines that arrived after the last notice stay pending.
func TestPendingFallsBackToWatermarkNotEOF(t *testing.T) {
	dir := t.TempDir()
	appendLines(t, dir, 0, 2, false)
	ib := &Inbox{Dir: dir}
	ib.MarkNotified(ib.NotifyMark())
	os.WriteFile(filepath.Join(dir, "last_handled"), []byte("GONE"), 0o600)
	appendLines(t, dir, 2, 2, false)
	if pi, _ := ib.Pending(); pi.Count != 2 {
		t.Fatalf("want 2 pending past watermark, got %+v", pi)
	}
}

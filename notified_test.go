package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func inLine(id, ts string) string {
	return `{"id":"` + id + `","chat":"g@g.us","ts":"` + ts + `","type":"text"}`
}

// The last_handled case: a claim long ago, then the bot handled later messages directly and moved
// last_handled. Those must not be pending (they were re-announced on every reconnect).
func TestPendingRespectsLastHandledAfterClaim(t *testing.T) {
	dir := t.TempDir()
	ib := &Inbox{Dir: dir}
	ib.Claim(time.Minute) // state.json at EOF of the empty inbox
	writeInbox(t, dir, inLine("A", "2026-10-18T16:44:00Z"), inLine("B", "2026-10-18T16:46:00Z"),
		`{"id":"B","type":"transcript_update"}`, inLine("C", "2026-10-18T20:50:00Z"))
	if pi, _ := ib.Pending(); pi.Count != 3 {
		t.Fatalf("before last_handled: %+v", pi)
	}
	os.WriteFile(filepath.Join(dir, "last_handled"), []byte(`{"id":"C","ts":"2026-10-18T20:50:00Z"}`), 0o600)
	if pi, _ := ib.Pending(); pi.Count != 0 || pi.Unhandled != 0 {
		t.Fatalf("handled via last_handled still pending: %+v", pi)
	}
	if l, _ := ib.Claim(time.Minute); l != nil {
		t.Fatalf("claim re-offered lines handled via last_handled: %v", ids(l))
	}
	writeInbox(t, dir, inLine("D", "2026-10-18T21:00:00Z"))
	if pi, _ := ib.Pending(); pi.Count != 1 || pi.Latest != "2026-10-18T21:00:00Z" {
		t.Fatalf("new line after last_handled: %+v", pi)
	}
	// last_handled pointing behind the claim offset never moves it back.
	os.WriteFile(filepath.Join(dir, "last_handled"), []byte(`{"id":"A"}`), 0o600)
	if pi, _ := ib.Pending(); pi.Count != 1 {
		t.Fatalf("stale last_handled moved offset back: %+v", pi)
	}
}

// After a successful notice the watermark hides already-announced lines from later settle checks;
// only lines that arrive afterwards count, and with none there is no notice at all.
func TestNotifiedWatermarkStopsRepeatNotices(t *testing.T) {
	var posts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { posts.Add(1) }))
	defer srv.Close()
	dir := t.TempDir()
	ib := &Inbox{Dir: dir}
	ib.Claim(time.Minute)
	n := &Notifier{URL: srv.URL, Delay: 5 * time.Millisecond, Cooldown: 10 * time.Millisecond,
		Pending: ib.Pending, Snapshot: ib.NotifyMark, Mark: func(o NoticeMark) { ib.MarkNotified(o) }}
	settle := func() { pi, _ := ib.Pending(); n.Kick(pi, "reconnect") }
	waitPosts := func(want int32) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for posts.Load() < want && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		time.Sleep(1300 * time.Millisecond) // Kick schedules >=1s out; make sure nothing extra lands
		if posts.Load() != want {
			t.Fatalf("posts %d, want %d", posts.Load(), want)
		}
	}
	writeInbox(t, dir, inLine("A", "t1"), inLine("B", "t2"), inLine("C", "t3"))
	settle()
	waitPosts(1)
	pi, _ := ib.Pending()
	if pi.Count != 0 || pi.Unhandled != 3 {
		t.Fatalf("after notice: %+v", pi)
	}
	settle() // reconnect #2 with nothing new: silent
	settle()
	waitPosts(1)
	writeInbox(t, dir, inLine("D", "t4"))
	if pi, _ := ib.Pending(); pi.Count != 1 || pi.Unhandled != 4 || pi.Latest != "t4" {
		t.Fatalf("new line: %+v", pi)
	}
	settle()
	waitPosts(2)
	if err := ib.MarkNotified(NoticeMark{Offset: 1}); err != nil { // never moves backwards
		t.Fatal(err)
	}
	if pi, _ := ib.Pending(); pi.Count != 0 {
		t.Fatalf("watermark moved back: %+v", pi)
	}
}

// A notice whose POST failed does not advance the watermark, so the lines stay announceable.
func TestFailedNoticeKeepsWatermark(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(403) }))
	defer srv.Close()
	dir := t.TempDir()
	ib := &Inbox{Dir: dir}
	ib.Claim(time.Minute)
	writeInbox(t, dir, inLine("A", "t1"))
	n := &Notifier{URL: srv.URL, Snapshot: ib.NotifyMark, Mark: func(o NoticeMark) { ib.MarkNotified(o) }}
	n.Add("g@g.us", "t1")
	n.mu.Lock()
	n.timer.Stop()
	n.mu.Unlock()
	n.flush()
	if pi, _ := ib.Pending(); pi.Count != 1 {
		t.Fatalf("failed notice marked lines announced: %+v", pi)
	}
}

// Periodic lease checks back off while notices keep failing (no POST storm every 30s), and the
// loop runs the check on its interval until ctx ends.
func TestLeaseCheckBackoffAndLoop(t *testing.T) {
	var posts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { posts.Add(1) }))
	defer srv.Close()
	clk := newHClock()
	dir := t.TempDir()
	ib := &Inbox{Dir: dir, Now: clk.Now}
	ib.Claim(time.Minute)
	writeInbox(t, dir, inLine("A", "t1"))
	if l, _ := ib.Claim(time.Minute); l == nil {
		t.Fatal("no lease")
	}
	clk.Add(2 * time.Minute)
	n := &Notifier{URL: srv.URL, Retries: 1}
	n.failures = 2 // gave up
	leaseCheck(ib, n, func(string, ...any) {})
	time.Sleep(1200 * time.Millisecond)
	if posts.Load() != 0 {
		t.Fatal("lease check posted while the notifier had given up")
	}
	n.failures = 0
	ctx, cancel := context.WithCancel(context.Background())
	var ticks atomic.Int32
	go watchLeases(ctx, 20*time.Millisecond, func() { ticks.Add(1) })
	time.Sleep(110 * time.Millisecond)
	cancel()
	if ticks.Load() < 3 {
		t.Fatalf("ticks %d", ticks.Load())
	}
	leaseCheck(ib, n, func(string, ...any) {})
	time.Sleep(1300 * time.Millisecond)
	if posts.Load() != 1 {
		t.Fatalf("posts %d, want 1", posts.Load())
	}
}

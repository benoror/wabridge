package main

import (
	"strings"
	"sync"
	"testing"
	"time"
)

type hClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *hClock) Now() time.Time               { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *hClock) Add(d time.Duration)          { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }
func newHClock() *hClock                       { return &hClock{t: time.Date(2026, 10, 16, 18, 0, 0, 0, time.UTC)} }
func (c *hClock) rfcNow() string               { return rfc(c.Now()) }
func (c *hClock) since(d time.Duration) string { return rfc(c.Now().Add(-d)) }

func TestCheckGapDetectsFrozenProcess(t *testing.T) {
	clk := newHClock()
	c := &fakeConn{}
	c.up.Store(true)
	w := NewWatchdog(c)
	w.Now = clk.Now
	var logs []string
	w.Log = func(s string) { logs = append(logs, s) }
	w.TransportActivity()
	if g := w.checkGap(); g != 0 {
		t.Fatal("first tick is never a gap")
	}
	clk.Add(5 * time.Second)
	if g := w.checkGap(); g != 0 {
		t.Fatalf("normal tick reported gap %s", g)
	}
	clk.Add(55 * time.Minute)
	if g := w.checkGap(); g != 55*time.Minute {
		t.Fatalf("want 55m gap, got %s", g)
	}
	if len(logs) != 1 || !strings.Contains(logs[0], "frozen for 55m0s") {
		t.Fatalf("gap log: %q", logs)
	}
	// After a pause a reconnect fires on the same tick; the resume rule names the pause.
	if why := w.due(clk.Now()); !strings.HasPrefix(why, "resumed after 55m0s pause, transport silent") {
		t.Fatalf("want resume reconnect after pause, got %q", why)
	}
}

func TestHealthSnapshot(t *testing.T) {
	clk := newHClock()
	c := &fakeConn{}
	c.up.Store(true)
	w := NewWatchdog(c)
	w.Now = clk.Now
	w.Log = func(string) {}
	h := &Health{Dir: t.TempDir(), Now: clk.Now, WD: w}
	h.Set("starting", "")
	s := h.Snapshot()
	if s.State != "starting" || s.LastMessageAt != "" || s.Reconnects != 0 || s.StartedAt != clk.rfcNow() {
		t.Fatalf("fresh snapshot: %+v", s)
	}
	clk.Add(time.Second)
	h.Set("connected", "")
	w.TransportActivity()
	h.Event()
	clk.Add(2 * time.Second)
	w.AppActivity()
	clk.Add(time.Minute)
	if err := w.ForceReconnect(t.Context(), "test"); err != nil {
		t.Fatal(err)
	}
	clk.Add(10 * time.Minute)
	w.checkGap()
	w.checkGap() // establishes lastTick
	clk.Add(40 * time.Minute)
	w.checkGap()
	h.Write()
	got, err := readStatus(h.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "connected" || got.TS != clk.rfcNow() {
		t.Fatalf("state/ts: %+v", got)
	}
	if got.LastMessageAt != clk.since(51*time.Minute) {
		t.Fatalf("last_message_at %q", got.LastMessageAt)
	}
	if got.ConnectedAt != clk.since(51*time.Minute+2*time.Second) || got.LastEventAt != got.ConnectedAt {
		t.Fatalf("connected/event: %+v", got)
	}
	if got.Reconnects != 1 || !strings.HasSuffix(got.LastReconnect, " test") {
		t.Fatalf("reconnects: %+v", got)
	}
	if !strings.HasSuffix(got.LastGap, " 40m0s") {
		t.Fatalf("last_gap %q", got.LastGap)
	}
}

func TestHealthRunHeartbeat(t *testing.T) {
	h := &Health{Dir: t.TempDir()}
	h.Set("connected", "")
	done := make(chan struct{})
	go h.Run(done, 5*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	close(done)
	s, err := readStatus(h.Dir)
	if err != nil || s.State != "connected" {
		t.Fatalf("heartbeat lost state: %+v %v", s, err)
	}
}

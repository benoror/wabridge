package main

import (
	"testing"
	"time"
)

func TestReplayGate(t *testing.T) {
	t0 := time.Unix(10_000_000, 0)
	g := &ReplayGate{}
	if r, _ := g.Classify(t0.Add(-time.Hour), t0); r {
		t.Fatal("replay before first connect")
	}
	g = &ReplayGate{}
	g.Connected(t0)
	if r, _ := g.Classify(t0.Add(-30*time.Second), t0); r {
		t.Fatal("inside grace marked replay")
	}
	if r, _ := g.Classify(t0.Add(-5*time.Minute), t0.Add(time.Second)); !r {
		t.Fatal("old message not marked replay")
	}
	// Live traffic, then a reconnect: catch-up window lets the last 10m through for 2m.
	g.Classify(t0.Add(time.Hour), t0.Add(time.Hour))
	t1 := t0.Add(time.Hour + time.Minute)
	g.Connected(t1)
	if r, _ := g.Classify(t1.Add(-5*time.Minute), t1.Add(10*time.Second)); r {
		t.Fatal("catch-up window not applied after active reconnect")
	}
	if r, _ := g.Classify(t1.Add(-20*time.Minute), t1.Add(10*time.Second)); !r {
		t.Fatal("catch-up reaches too far back")
	}
	if r, _ := g.Classify(t1.Add(-5*time.Minute), t1.Add(3*time.Minute)); !r {
		t.Fatal("catch-up window did not expire")
	}
	// Quiet reconnect: no catch-up.
	t2 := t1.Add(time.Hour)
	g.Connected(t2)
	if r, _ := g.Classify(t2.Add(-5*time.Minute), t2.Add(time.Second)); !r {
		t.Fatal("catch-up granted after quiet period")
	}
	g.OfflineSync(true)
	if _, off := g.Classify(t2, t2); !off {
		t.Fatal("offline flag missing")
	}
	g.OfflineSync(false)
	if _, off := g.Classify(t2, t2); off {
		t.Fatal("offline flag stuck")
	}
}

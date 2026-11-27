package main

// Delivery classifier (informational only). After a (re)connect WhatsApp delivers messages that
// were queued while the bridge was offline. Lines older than connectedAt-Grace are marked
// "delayed": true. This never skips anything: such a message is usually brand new to us, so it
// is claimable and triggers the webhook notice like any other (until 2026-10-17 this was
// "replay": true and suppressed the webhook, which stranded real offline-time messages).
// Only true duplicates (seen.log) and already-claimed inbox offsets are skipped.
// If the reconnect happened during active traffic, a bounded catch-up window leaves slightly
// older messages unflagged. whatsmeow has no per-message offline flag, but brackets offline delivery with
// events.OfflineSyncPreview / OfflineSyncCompleted; lines in between get "offline": true.

import (
	"sync"
	"time"
)

type ReplayGate struct {
	Grace       time.Duration // default 60s
	Recent      time.Duration // inbound this recently before connect => catch-up allowed (default 2m)
	CatchupFor  time.Duration // how long after connect the catch-up applies (default 2m)
	CatchupBack time.Duration // how far back catch-up reaches before connect (default 10m)

	mu           sync.Mutex
	connectedAt  time.Time
	catchupUntil time.Time
	lastInbound  time.Time
	offline      bool
}

func (g *ReplayGate) defaults() {
	if g.Grace == 0 {
		g.Grace = 60 * time.Second
	}
	if g.Recent == 0 {
		g.Recent = 2 * time.Minute
	}
	if g.CatchupFor == 0 {
		g.CatchupFor = 2 * time.Minute
	}
	if g.CatchupBack == 0 {
		g.CatchupBack = 10 * time.Minute
	}
}

func (g *ReplayGate) Connected(now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.defaults()
	g.connectedAt = now
	g.catchupUntil = time.Time{}
	if !g.lastInbound.IsZero() && now.Sub(g.lastInbound) <= g.Recent {
		g.catchupUntil = now.Add(g.CatchupFor)
	}
}

func (g *ReplayGate) OfflineSync(active bool) { g.mu.Lock(); g.offline = active; g.mu.Unlock() }

// Classify returns whether a message with timestamp ts (received at now) is a replay,
// and whether it arrived during offline sync.
func (g *ReplayGate) Classify(ts, now time.Time) (replay, offline bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.defaults()
	offline = g.offline
	if g.connectedAt.IsZero() {
		return false, offline
	}
	cutoff := g.connectedAt.Add(-g.Grace)
	if now.Before(g.catchupUntil) {
		cutoff = g.connectedAt.Add(-g.CatchupBack)
	}
	replay = ts.Before(cutoff)
	if !replay {
		g.lastInbound = now
	}
	return replay, offline
}

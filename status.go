package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Status is <profile>/status.json, readable by supervise.sh, scripts/ensure.sh and humans.
// States: starting, connected, disconnected, stream_replaced (terminal: another client took
// the session; the process idles instead of exiting so the supervisor does not restart-loop
// into a fight), logged_out.
//
// "state" alone is not health: a frozen (paused box) or dead process leaves "connected"
// behind. TS is a heartbeat rewritten every StatusBeat by the running bridge, so a TS older
// than a couple of minutes means the process is not running (or the box was paused). The
// *_at fields are the real signals: last inbound transport frame (incl. keepalive pongs),
// last whatsmeow event of any kind, last application message, last Connected event.
type Status struct {
	State         string `json:"state"`
	Reason        string `json:"reason,omitempty"`
	PID           int    `json:"pid"`
	TS            string `json:"ts"`
	StartedAt     string `json:"started_at,omitempty"`
	ConnectedAt   string `json:"connected_at,omitempty"`
	LastFrameAt   string `json:"last_frame_at,omitempty"`
	LastEventAt   string `json:"last_event_at,omitempty"`
	LastMessageAt string `json:"last_message_at,omitempty"`
	Reconnects    int    `json:"reconnects,omitempty"`
	LastReconnect string `json:"last_reconnect,omitempty"` // "<RFC3339> <reason>"
	LastGap       string `json:"last_gap,omitempty"`       // "<RFC3339> <duration>": process was frozen (box paused)
}

// StatusBeat is how often a running bridge rewrites status.json.
const StatusBeat = 30 * time.Second

func rfc(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// Health owns status.json for a running bridge: state changes and the periodic heartbeat
// go through one writer so neither clobbers the other's fields.
type Health struct {
	Dir string
	Now func() time.Time
	WD  *Watchdog // optional: frame/app timestamps, reconnect and gap counters

	mu        sync.Mutex
	state     string
	reason    string
	started   time.Time
	connected time.Time
	lastEvent time.Time
}

func (h *Health) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

// Set records a state change and writes status.json immediately.
func (h *Health) Set(state, reason string) {
	h.mu.Lock()
	if h.started.IsZero() {
		h.started = h.now()
	}
	h.state, h.reason = state, reason
	if state == "connected" {
		h.connected = h.now()
	}
	h.mu.Unlock()
	h.Write()
}

// State returns the current state.
func (h *Health) State() string { h.mu.Lock(); defer h.mu.Unlock(); return h.state }

// Event records that whatsmeow dispatched an event (any type).
func (h *Health) Event() { h.mu.Lock(); h.lastEvent = h.now(); h.mu.Unlock() }

// Snapshot builds the current Status.
func (h *Health) Snapshot() Status {
	h.mu.Lock()
	s := Status{State: h.state, Reason: h.reason, PID: os.Getpid(), TS: rfc(h.now()),
		StartedAt: rfc(h.started), ConnectedAt: rfc(h.connected), LastEventAt: rfc(h.lastEvent)}
	h.mu.Unlock()
	if w := h.WD; w != nil {
		s.LastFrameAt = rfc(unixOrZero(w.lastTransport.Load()))
		if m := w.lastMsg.Load(); m != 0 {
			s.LastMessageAt = rfc(time.Unix(0, m))
		}
		s.Reconnects = int(w.reconnects.Load())
		w.mu.Lock()
		if !w.lastReconnAt.IsZero() {
			s.LastReconnect = rfc(w.lastReconnAt) + " " + w.lastReconnWhy
		}
		if !w.lastGapAt.IsZero() {
			s.LastGap = rfc(w.lastGapAt) + " " + w.lastGap.Round(time.Second).String()
		}
		w.mu.Unlock()
	}
	return s
}

func unixOrZero(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// Write rewrites status.json from the current snapshot.
func (h *Health) Write() {
	atomicWrite(filepath.Join(h.Dir, "status.json"), h.Snapshot())
}

// Run rewrites status.json every StatusBeat until done is closed.
func (h *Health) Run(done <-chan struct{}, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-t.C:
			h.Write()
		}
	}
}

func writeStatus(dir, state, reason string) {
	atomicWrite(filepath.Join(dir, "status.json"), Status{State: state, Reason: reason, PID: os.Getpid(),
		TS: time.Now().UTC().Format(time.RFC3339)})
}

func readStatus(dir string) (Status, error) {
	var s Status
	b, err := os.ReadFile(filepath.Join(dir, "status.json"))
	if err == nil {
		err = json.Unmarshal(b, &s)
	}
	return s, err
}

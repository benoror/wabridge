package main

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// lockedConn models whatsmeow after a resume: its own auto-reconnect sits in Connect holding
// socketLock, so IsConnected/Disconnect/Connect all block until the lock is released.
type lockedConn struct {
	mu       sync.RWMutex
	up       atomic.Bool
	connects atomic.Int32
}

func (c *lockedConn) IsConnected() bool { c.mu.RLock(); defer c.mu.RUnlock(); return c.up.Load() }
func (c *lockedConn) Disconnect()       { c.mu.Lock(); c.up.Store(false); c.mu.Unlock() }
func (c *lockedConn) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connects.Add(1)
	c.up.Store(true)
	return nil
}

func wedgeWD(c Conn) (*Watchdog, *logBuf) {
	w := fastWD(c)
	w.DownMax, w.GiveUp = time.Hour, time.Hour
	w.ProbeMax, w.WedgeDrop, w.WedgeMax = 20*time.Millisecond, 60*time.Millisecond, 200*time.Millisecond
	lb := &logBuf{}
	w.Log = lb.add
	return w, lb
}

type logBuf struct {
	mu sync.Mutex
	l  []string
}

func (b *logBuf) add(s string) { b.mu.Lock(); b.l = append(b.l, s); b.mu.Unlock() }
func (b *logBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Join(b.l, "\n")
}

// The stuck lock is broken by Drop (dial on a dead link fails once its socket is closed):
// the watchdog notices, drops, carries on, and status.json keeps being written meanwhile.
func TestWedgedClientDropFreesItAndHeartbeatContinues(t *testing.T) {
	c := &lockedConn{}
	c.up.Store(true)
	c.mu.Lock() // auto-reconnect stuck holding the lock
	w, lb := wedgeWD(c)
	var drops atomic.Int32
	w.Drop = func() {
		if drops.Add(1) == 1 {
			c.up.Store(false)
			c.mu.Unlock()
		}
	}
	h := &Health{Dir: t.TempDir(), WD: w}
	h.Set("connected", "")
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()
	time.Sleep(40 * time.Millisecond) // wedge detected, before WedgeDrop
	if !bounded(100*time.Millisecond, h.Write) {
		t.Fatal("status.json heartbeat blocked while the client lock is stuck")
	}
	if err := <-errc; err != nil {
		t.Fatalf("Run: %v (logs %s)", err, lb)
	}
	if drops.Load() != 1 {
		t.Fatalf("drops=%d", drops.Load())
	}
	for _, want := range []string{"client lock stuck", "dropped the socket", "client lock free again"} {
		if !strings.Contains(lb.String(), want) {
			t.Fatalf("missing %q in logs:\n%s", want, lb)
		}
	}
}

// Drop does not help: Run must give up with ErrWedged (supervisor restart) within WedgeMax,
// not hang forever with the process alive.
func TestWedgedClientExitsAfterWedgeMax(t *testing.T) {
	c := &lockedConn{}
	c.mu.Lock()
	defer c.mu.Unlock()
	w, lb := wedgeWD(c)
	var drops atomic.Int32
	w.Drop = func() { drops.Add(1) }
	start := time.Now()
	err := runFor(w, 2*time.Second)
	if !errors.Is(err, ErrWedged) {
		t.Fatalf("want ErrWedged, got %v (logs %s)", err, lb)
	}
	if el := time.Since(start); el > time.Second {
		t.Fatalf("took %s to give up", el)
	}
	if drops.Load() != 1 {
		t.Fatalf("drops=%d, want 1 before giving up", drops.Load())
	}
}

// A halted watchdog (stream replaced) must never exit-and-restart into a session fight.
func TestWedgedIgnoredWhenHalted(t *testing.T) {
	c := &lockedConn{}
	c.mu.Lock()
	defer c.mu.Unlock()
	w, _ := wedgeWD(c)
	w.Halt()
	if err := runFor(w, 400*time.Millisecond); err != nil {
		t.Fatalf("halted Run returned %v", err)
	}
}

func TestConnectHungReturnsWedged(t *testing.T) {
	c := &lockedConn{}
	c.mu.Lock()
	defer c.mu.Unlock()
	w, _ := wedgeWD(c)
	w.ConnectMax, w.CallMax, w.DropAfter = 30*time.Millisecond, 30*time.Millisecond, 5*time.Millisecond
	w.Drop = func() {}
	start := time.Now()
	err := w.ForceReconnect(context.Background(), "test")
	if !errors.Is(err, ErrWedged) {
		t.Fatalf("want ErrWedged, got %v", err)
	}
	if el := time.Since(start); el > 500*time.Millisecond {
		t.Fatalf("bounded connect took %s", el)
	}
}

// After a resume the bridge must be connected with a fresh frame within ResumeMax, or the
// watchdog forces a reconnect itself (instead of waiting DownMax or forever).
func TestResumeDeadline(t *testing.T) {
	clk := newHClock()
	c := &fakeConn{}
	c.up.Store(true)
	w := NewWatchdog(c)
	w.Now, w.Log = clk.Now, func(string) {}
	w.TransportActivity()
	w.checkGap()
	clk.Add(10 * time.Minute)
	c.up.Store(false) // EOF on resume; whatsmeow's auto-reconnect owns it
	w.checkGap()
	if why := w.due(clk.Now()); why != "" {
		t.Fatalf("immediately after resume: %q", why)
	}
	clk.Add(10 * time.Second)
	if why := w.due(clk.Now()); why != "" {
		t.Fatalf("10s after resume: %q", why)
	}
	clk.Add(11 * time.Second)
	if why := w.due(clk.Now()); !strings.HasPrefix(why, "not recovered 21s after resume (connected=false)") {
		t.Fatalf("21s after resume: %q", why)
	}
	if why := w.due(clk.Now()); why != "" {
		t.Fatalf("deadline must fire once: %q", why)
	}

	// Recovered in time (connected + a frame after the resume): no forced reconnect.
	w.checkGap()
	clk.Add(10 * time.Minute)
	w.checkGap()
	clk.Add(3 * time.Second)
	c.up.Store(true)
	w.TransportActivity()
	w.due(clk.Now())
	clk.Add(30 * time.Second)
	w.TransportActivity()
	if why := w.due(clk.Now()); why != "" {
		t.Fatalf("recovered bridge reconnected: %q", why)
	}
}

// The websocket dial is bounded: a peer that accepts TCP but never answers the upgrade
// (request lost on a dead link) must fail after wsDialTimeout, not hang.
func TestWebsocketDialBounded(t *testing.T) {
	old := wsDialTimeout
	wsDialTimeout = 150 * time.Millisecond
	defer func() { wsDialTimeout = old }()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close() // hold it open, say nothing
		}
	}()
	k := newSockTracker()
	start := time.Now()
	_, _, err = websocket.Dial(context.Background(), "ws://"+ln.Addr().String()+"/", &websocket.DialOptions{HTTPClient: k.Client()})
	if err == nil {
		t.Fatal("dial to a silent peer succeeded")
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("dial took %s", el)
	}
}

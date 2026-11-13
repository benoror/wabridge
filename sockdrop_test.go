package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// stallConn: Disconnect blocks (like a graceful websocket close on a dead link) until Drop.
type stallConn struct {
	up       atomic.Bool
	connects atomic.Int32
	release  chan struct{}
}

func (s *stallConn) IsConnected() bool { return s.up.Load() }
func (s *stallConn) Connect() error    { s.connects.Add(1); s.up.Store(true); return nil }
func (s *stallConn) Disconnect() {
	select {
	case <-s.release:
	case <-time.After(5 * time.Second): // coder/websocket's hardcoded close timeout
	}
	s.up.Store(false)
}

func TestReconnectDropsStalledClose(t *testing.T) {
	c := &stallConn{release: make(chan struct{})}
	c.up.Store(true)
	w := NewWatchdog(c)
	var logs []string
	w.Log = func(s string) { logs = append(logs, s) }
	var drops atomic.Int32
	w.Drop = func() {
		if drops.Add(1) == 1 {
			close(c.release)
		}
	}
	start := time.Now()
	if err := w.ForceReconnect(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el > time.Second {
		t.Fatalf("reconnect took %s; drop should cut the stalled close short", el)
	}
	if drops.Load() != 1 || c.connects.Load() != 1 {
		t.Fatalf("drops=%d connects=%d", drops.Load(), c.connects.Load())
	}
	if !strings.Contains(strings.Join(logs, "\n"), "dropped the socket") {
		t.Fatalf("logs: %q", logs)
	}
}

func TestReconnectNoDropWhenCloseIsQuick(t *testing.T) {
	c := &fakeConn{}
	c.up.Store(true)
	w := fastWD(c)
	w.DropAfter = 200 * time.Millisecond
	var drops atomic.Int32
	w.Drop = func() { drops.Add(1) }
	if err := w.ForceReconnect(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	if drops.Load() != 0 || c.connects.Load() != 1 {
		t.Fatalf("drops=%d connects=%d", drops.Load(), c.connects.Load())
	}
}

func TestResumeTriggersImmediateReconnect(t *testing.T) {
	clk := newHClock()
	c := &fakeConn{}
	c.up.Store(true)
	w := NewWatchdog(c)
	w.Now, w.Log = clk.Now, func(string) {}
	w.TransportActivity()
	w.checkGap()              // establishes lastTick
	clk.Add(45 * time.Second) // paused 45s: below TransportMax (90s), so the old rule waited
	w.checkGap()
	why := w.due(clk.Now())
	if !strings.HasPrefix(why, "resumed after 45s pause") {
		t.Fatalf("due after resume = %q", why)
	}
	if w.due(clk.Now()) != "" {
		t.Fatal("resume flag must be consumed by one check")
	}

	// A frame already arrived after the resume: the socket survived, leave it alone.
	w.checkGap()
	clk.Add(45 * time.Second)
	w.TransportActivity()
	w.checkGap()
	if why := w.due(clk.Now()); why != "" {
		t.Fatalf("live socket after resume reconnected: %q", why)
	}

	// Socket already down (whatsmeow auto-reconnect owns it): the resume rule stays out.
	clk.Add(45 * time.Second)
	c.up.Store(false)
	w.checkGap()
	if why := w.due(clk.Now()); why != "" {
		t.Fatalf("down socket hit resume rule: %q", why)
	}
}

func TestResumeIgnoredWhenHalted(t *testing.T) {
	clk := newHClock()
	c := &fakeConn{}
	c.up.Store(true)
	w := NewWatchdog(c)
	w.Now, w.Log = clk.Now, func(string) {}
	w.checkGap()
	clk.Add(10 * time.Minute)
	w.checkGap()
	w.Halt()
	if why := w.due(clk.Now()); why != "" {
		t.Fatalf("halted watchdog wants reconnect: %q", why)
	}
}

// Real websocket: the peer never answers the close frame, so a graceful Close waits for the
// full handshake timeout. DropAll must make it return right away.
func TestSockTrackerDropUnblocksWebsocketClose(t *testing.T) {
	hold := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(rw, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		<-hold // never read: the client's close frame is never acknowledged
	}))
	defer srv.Close()
	defer close(hold)

	k := newSockTracker()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), &websocket.DialOptions{HTTPClient: k.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if k.Live() != 1 {
		t.Fatalf("tracked %d conns, want 1", k.Live())
	}
	done := make(chan struct{})
	start := time.Now()
	go func() { conn.Close(websocket.StatusNormalClosure, ""); close(done) }()
	select {
	case <-done:
		t.Fatal("close returned without a handshake; test can't show the stall")
	case <-time.After(300 * time.Millisecond):
	}
	if n := k.DropAll(); n != 1 {
		t.Fatalf("dropped %d", n)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("close still blocked after DropAll")
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("close took %s", el)
	}
	if k.Live() != 0 {
		t.Fatalf("%d conns still tracked after drop", k.Live())
	}
}

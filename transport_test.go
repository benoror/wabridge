package main

import (
	"context"
	"sync"
	"testing"
	"time"

	waLog "go.mau.fi/whatsmeow/util/log"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func clockWD(c Conn) (*Watchdog, *fakeClock) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	w := fastWD(c)
	w.Now = clk.Now
	w.TransportMax, w.TransportShort, w.AppWindow = 90*time.Second, 45*time.Second, 5*time.Minute
	w.TransportActivity()
	w.AppActivity()
	return w, clk
}

func upConn() *fakeConn { c := &fakeConn{}; c.up.Store(true); return c }

func TestTransportActiveAppQuietNoReconnect(t *testing.T) {
	w, clk := clockWD(upConn())
	for i := 0; i < 120; i++ { // 1h of app silence, frames every 30s
		clk.Add(30 * time.Second)
		w.TransportActivity()
		if why := w.due(clk.Now()); why != "" {
			t.Fatalf("reconnect at %d despite active transport: %s", i, why)
		}
	}
	if !w.Healthy() {
		t.Fatal("should be healthy")
	}
}

func TestTransportSilentPastWindowReconnects(t *testing.T) {
	c := upConn()
	w, clk := clockWD(c)
	clk.Add(89 * time.Second)
	w.AppActivity() // app traffic must not mask a dead transport, nor be needed
	if why := w.due(clk.Now()); why != "" {
		t.Fatalf("too early: %s", why)
	}
	clk.Add(2 * time.Second)
	if w.due(clk.Now()) == "" {
		t.Fatal("expected reconnect after 91s transport silence")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)
	deadline := time.Now().Add(time.Second)
	for c.connects.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if c.connects.Load() == 0 {
		t.Fatal("Run did not reconnect")
	}
}

func TestShorterWindowOnceAfterReconnect(t *testing.T) {
	c := upConn()
	w, clk := clockWD(c)
	if err := w.ForceReconnect(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	clk.Add(46 * time.Second)
	if w.due(clk.Now()) == "" {
		t.Fatal("expected short (45s) window right after reconnect")
	}
	w.ForceReconnect(context.Background(), "test")
	clk.Add(10 * time.Second)
	w.TransportActivity() // first frame after reconnect => back to normal window
	clk.Add(60 * time.Second)
	if why := w.due(clk.Now()); why != "" {
		t.Fatalf("normal window should apply after first frame: %s", why)
	}
	clk.Add(31 * time.Second)
	if w.due(clk.Now()) == "" {
		t.Fatal("expected reconnect past normal window")
	}
}

func TestAppQuietPlusStaleTransportRule(t *testing.T) {
	w, clk := clockWD(upConn())
	for i := 0; i < 40; i++ { // 20 min of app silence, transport fresh
		clk.Add(30 * time.Second)
		w.TransportActivity()
	}
	clk.Add(50 * time.Second) // 4x app window exceeded, transport 50s (> short, < max)
	if w.due(clk.Now()) == "" {
		t.Fatal("app quiet >4x window + transport stale should reconnect")
	}
	w.AppActivity()
	if why := w.due(clk.Now()); why != "" {
		t.Fatalf("recent app msg: transport 50s < 90s should not reconnect: %s", why)
	}
}

func TestEnsureReconnectsOnStaleTransport(t *testing.T) {
	c := upConn()
	w, clk := clockWD(c)
	clk.Add(30 * time.Second)
	if err := w.Ensure(context.Background(), func() bool { return true }, time.Second); err != nil || c.connects.Load() != 0 {
		t.Fatalf("fresh transport should not reconnect: %v %d", err, c.connects.Load())
	}
	clk.Add(91 * time.Second)
	if w.Healthy() {
		t.Fatal("stale transport must be unhealthy")
	}
	if err := w.Ensure(context.Background(), func() bool { return true }, time.Second); err != nil || c.connects.Load() != 1 {
		t.Fatalf("expected one reconnect: %v %d", err, c.connects.Load())
	}
}

func TestRecvTapBumpsTransport(t *testing.T) {
	w, clk := clockWD(upConn())
	f := w.TransportActivity
	frameHook.Store(&f)
	defer frameHook.Store(nil)
	clk.Add(80 * time.Second)
	tapLogger{nopLog{}}.Sub("Recv").Debugf("%s", "<node/>")
	clk.Add(80 * time.Second)
	if why := w.due(clk.Now()); why != "" {
		t.Fatalf("frame via Recv logger should count: %s", why)
	}
}

type nopLog struct{}

func (nopLog) Debugf(string, ...any)     {}
func (nopLog) Infof(string, ...any)      {}
func (nopLog) Warnf(string, ...any)      {}
func (nopLog) Errorf(string, ...any)     {}
func (n nopLog) Sub(string) waLog.Logger { return n }

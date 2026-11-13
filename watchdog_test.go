package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type fakeConn struct {
	up                atomic.Bool
	connects, discs   atomic.Int32
	failConnect, hang bool
}

func (f *fakeConn) IsConnected() bool { return f.up.Load() }
func (f *fakeConn) Disconnect() {
	f.discs.Add(1)
	f.up.Store(false)
	if f.hang {
		select {}
	}
}
func (f *fakeConn) Connect() error {
	f.connects.Add(1)
	if f.failConnect {
		return errors.New("nope")
	}
	f.up.Store(true)
	return nil
}

func fastWD(c Conn) *Watchdog {
	w := NewWatchdog(c)
	w.Tick, w.DownMax, w.GiveUp, w.CallMax, w.Backoff0 = 5*time.Millisecond, 30*time.Millisecond, 80*time.Millisecond, 10*time.Millisecond, 5*time.Millisecond
	w.Log = func(string) {}
	return w
}

func runFor(w *Watchdog, d time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return w.Run(ctx)
}

func TestWatchdogKeepaliveFailures(t *testing.T) {
	c := &fakeConn{}
	c.up.Store(true)
	w := fastWD(c)
	w.KeepAliveFailed()
	runFor(w, 30*time.Millisecond)
	if c.connects.Load() != 0 {
		t.Fatal("one timeout must not reconnect")
	}
	w.KeepAliveFailed()
	w.KeepAliveFailed()
	runFor(w, 30*time.Millisecond)
	if c.connects.Load() != 1 || c.discs.Load() != 1 {
		t.Fatalf("want 1 forced reconnect, got c=%d d=%d", c.connects.Load(), c.discs.Load())
	}
	w.KeepAliveFailed()
	w.KeepAliveOK()
	w.KeepAliveFailed()
	runFor(w, 30*time.Millisecond)
	if c.connects.Load() != 1 {
		t.Fatal("restored keepalive must reset the count")
	}
}

func TestWatchdogDownTooLongWithHungDisconnect(t *testing.T) {
	c := &fakeConn{hang: true}
	w := fastWD(c)
	if err := runFor(w, 200*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if c.connects.Load() < 1 {
		t.Fatal("expected reconnect after being down > DownMax")
	}
}

func TestWatchdogGivesUp(t *testing.T) {
	c := &fakeConn{failConnect: true}
	w := fastWD(c)
	if err := runFor(w, 2*time.Second); err == nil {
		t.Fatal("expected give-up error")
	}
}

func TestEnsureReconnectsAfterKeepaliveTimeout(t *testing.T) {
	c := &fakeConn{}
	c.up.Store(true)
	w := fastWD(c)
	if err := w.Ensure(context.Background(), func() bool { return true }, time.Second); err != nil || c.connects.Load() != 0 {
		t.Fatalf("healthy should not reconnect: %v %d", err, c.connects.Load())
	}
	w.KeepAliveFailed()
	if err := w.Ensure(context.Background(), func() bool { return true }, time.Second); err != nil || c.connects.Load() != 1 {
		t.Fatalf("expected one reconnect: %v %d", err, c.connects.Load())
	}
	w.AddPending(1)
	w.KeepAliveFailed()
	if w.due(time.Now()) == "" {
		t.Fatal("one keepalive timeout with pending send should trigger reconnect")
	}
}

func TestHaltStopsReconnects(t *testing.T) {
	c := &fakeConn{}
	w := fastWD(c)
	w.Halt()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := w.Run(ctx); err != nil {
		t.Fatalf("halted watchdog gave up instead of idling: %v", err)
	}
	if c.connects.Load() != 0 {
		t.Fatalf("halted watchdog reconnected %d times", c.connects.Load())
	}
	if err := w.ForceReconnect(context.Background(), "pre-send"); !errors.Is(err, ErrHalted) {
		t.Fatalf("ForceReconnect after halt: %v", err)
	}
	if w.Healthy() {
		t.Fatal("halted conn reported healthy")
	}
}

func TestStatusFile(t *testing.T) {
	d := t.TempDir()
	writeStatus(d, "stream_replaced", "x")
	s, err := readStatus(d)
	if err != nil || s.State != "stream_replaced" || s.PID == 0 {
		t.Fatalf("status %+v %v", s, err)
	}
}

func TestJitter(t *testing.T) {
	d := 4 * time.Second
	if jitter(d, 0) != 3*time.Second || jitter(d, 0.5) != d || jitter(d, 0.999) >= 5*time.Second {
		t.Fatal("jitter out of +/-25% band")
	}
}

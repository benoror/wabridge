package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.mau.fi/whatsmeow"
)

// Conn is the slice of *whatsmeow.Client the watchdog drives (fakeable in tests).
type Conn interface {
	IsConnected() bool
	Connect() error
	Disconnect()
}

// Watchdog forces a reconnect when keepalives keep failing or the socket stays
// down, and gives up (returns an error) once reconnecting has failed for GiveUp.
type Watchdog struct {
	C       Conn
	Tick    time.Duration // check interval
	DownMax time.Duration // max time !IsConnected before forcing reconnect
	GiveUp  time.Duration // reconnect failing this long => Run returns error
	CallMax time.Duration // bound on a single Disconnect call
	// Drop hard-closes the underlying TCP socket(s) without a websocket close handshake. A
	// reconnect only happens when the socket is already judged broken, so if Disconnect has
	// not finished after DropAfter (a graceful close on a dead link stalls ~5s), Drop is called
	// to make it return at once. nil = old behaviour (wait up to CallMax).
	Drop      func()
	DropAfter time.Duration
	Backoff0  time.Duration
	Log       func(string)

	mu        sync.Mutex
	kaFails   int
	downSince time.Time
	reconnMu  sync.Mutex   // one reconnect at a time (watchdog vs outbox)
	pending   atomic.Int32 // outbox items in flight => react to the first keepalive timeout

	// Transport-activity watchdog (OpenClaw-style): raw inbound frames are tracked
	// separately from application messages. Silence on the transport means a
	// half-dead socket; silence on the app side alone is normal and never restarts.
	Now            func() time.Time
	TransportMax   time.Duration // transport silent this long => reconnect (> keepalive interval)
	TransportShort time.Duration // window used once right after a reconnect, and for the app+transport rule
	AppWindow      time.Duration // "normal" app quiet window; 4x of it plus stale transport => reconnect
	lastTransport  atomic.Int64  // unix nanos of last inbound frame
	lastApp        atomic.Int64  // unix nanos of last application message
	justReconn     atomic.Bool   // use TransportShort until the first frame after a reconnect
	halted         atomic.Bool   // terminal state (StreamReplaced): never reconnect again

	// Health counters for status.json (see Health).
	lastMsg       atomic.Int64 // unix nanos of last application message (0 = none since start)
	reconnects    atomic.Int32
	lastReconnAt  time.Time // under mu
	lastReconnWhy string    // under mu
	// Gap detection: the box can be paused (whole VM frozen) for minutes to hours; on resume
	// the wall clock jumps. A tick arriving GapMin+ after the previous one (wall clock) means
	// the process was frozen; it is logged and recorded so "transport silent for 7h" reconnects
	// read as resume-after-pause rather than a half-dead socket.
	GapMin    time.Duration
	lastTick  int64     // unix nanos (wall) of previous tick; only touched by Run
	lastGapAt time.Time // under mu
	lastGap   time.Duration
	resumed   time.Duration // gap seen on the current tick (consumed by due)
	resumeAt  time.Time     // under mu: last resume not yet followed by a healthy socket

	// Hard deadlines. whatsmeow's IsConnected/Connect/Disconnect all take the client's
	// socketLock; after a resume its own auto-reconnect can sit inside Connect (websocket dial)
	// holding that lock indefinitely. Every call into C therefore goes through a bounded probe,
	// w.mu is never held across one (status.json's heartbeat reads w.mu), and a lock that stays
	// stuck is broken by dropping the sockets and, failing that, by exiting for the supervisor.
	ProbeMax   time.Duration // bound on one IsConnected call
	ConnectMax time.Duration // bound on one Connect call (then Drop, then CallMax more)
	ResumeMax  time.Duration // after a resume, not connected with a fresh frame by then => force reconnect
	WedgeDrop  time.Duration // client lock stuck this long => Drop() the sockets
	WedgeMax   time.Duration // still stuck this long => Run returns ErrWedged (supervisor restarts us)
	probeBusy  atomic.Bool
	wedgedAt   time.Time // only Run
	wedgeDrop  bool      // only Run
}

// ErrHalted is returned by reconnects after Halt (another client owns the session).
var ErrHalted = errors.New("connection halted: session replaced by another client")

// ErrWedged: the whatsmeow client's socket lock is held by a goroutine that never returns.
// Nothing in-process can recover that, so Run gives up and the supervisor restarts the bridge.
var ErrWedged = errors.New("client wedged: socket lock held by a stuck call")

// connected is a bounded IsConnected: ok=false means the call did not return within ProbeMax
// (the client lock is held by a stuck goroutine). At most one probe is in flight; while it is
// stuck, later probes report ok=false at once instead of piling up goroutines.
func (w *Watchdog) connected() (up, ok bool) {
	if !w.probeBusy.CompareAndSwap(false, true) {
		return false, false
	}
	ch := make(chan bool, 1)
	go func() {
		r := w.C.IsConnected()
		w.probeBusy.Store(false)
		ch <- r
	}()
	select {
	case r := <-ch:
		return r, true
	case <-time.After(w.ProbeMax):
		return false, false
	}
}

// Halt stops all reconnects for good (StreamReplaced is terminal; fighting it loops forever).
func (w *Watchdog) Halt()        { w.halted.Store(true) }
func (w *Watchdog) Halted() bool { return w.halted.Load() }

// TransportActivity records an inbound frame (called from the Recv logger hook).
func (w *Watchdog) TransportActivity() {
	w.lastTransport.Store(w.Now().UnixNano())
	w.justReconn.Store(false)
}

// AppActivity records an application-level message event.
func (w *Watchdog) AppActivity() {
	n := w.Now().UnixNano()
	w.lastApp.Store(n)
	w.lastMsg.Store(n)
}

func (w *Watchdog) transportWindow() time.Duration {
	if w.justReconn.Load() {
		return w.TransportShort
	}
	return w.TransportMax
}

func (w *Watchdog) sinceTransport(now time.Time) time.Duration {
	return now.Sub(time.Unix(0, w.lastTransport.Load()))
}

// transportStale: connected socket but no inbound frame within the active window.
func (w *Watchdog) transportStale(now time.Time) bool {
	return w.sinceTransport(now) > w.transportWindow()
}

// AddPending adjusts the in-flight outbox counter.
func (w *Watchdog) AddPending(d int) { w.pending.Add(int32(d)) }

// Healthy: socket up and no unanswered keepalive.
func (w *Watchdog) Healthy() bool {
	up, ok := w.connected() // never under w.mu: it may block on the client lock
	w.mu.Lock()
	defer w.mu.Unlock()
	return ok && up && !w.halted.Load() && w.kaFails == 0 && !w.transportStale(w.Now())
}

// Ensure makes the connection usable before a send: if it is down or a keepalive
// has timed out, force a reconnect; then wait (up to max) until ready() holds.
func (w *Watchdog) Ensure(ctx context.Context, ready func() bool, max time.Duration) error {
	if !w.Healthy() {
		if err := w.ForceReconnect(ctx, "pre-send health check"); err != nil {
			return err
		}
	}
	deadline := time.Now().Add(max)
	for {
		if up, _ := w.connected(); up && ready() {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("not connected after %s", max)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return nil
}

// ForceReconnect drops the socket and reconnects (serialized with the watchdog loop).
func (w *Watchdog) ForceReconnect(ctx context.Context, why string) error {
	return w.reconnect(ctx, why)
}

func NewWatchdog(c Conn) *Watchdog {
	// Tick is 1s so a resume is noticed within ~1s (the checks are a few atomic loads).
	w := &Watchdog{C: c, Tick: 1 * time.Second, DropAfter: 150 * time.Millisecond, DownMax: 60 * time.Second, GiveUp: 5 * time.Minute,
		CallMax: 10 * time.Second, Backoff0: 5 * time.Second,
		ProbeMax: 2 * time.Second, ConnectMax: 45 * time.Second, ResumeMax: 20 * time.Second,
		WedgeDrop: 5 * time.Second, WedgeMax: 20 * time.Second,
		Now: time.Now, TransportMax: 90 * time.Second, TransportShort: 45 * time.Second, AppWindow: 5 * time.Minute,
		GapMin: 30 * time.Second,
		Log:    func(s string) { fmt.Fprintln(os.Stderr, time.Now().Format("15:04:05"), "watchdog:", s) }}
	n := w.Now().UnixNano()
	w.lastTransport.Store(n)
	w.lastApp.Store(n)
	return w
}

func (w *Watchdog) KeepAliveFailed() { w.mu.Lock(); w.kaFails++; w.mu.Unlock() }
func (w *Watchdog) KeepAliveOK()     { w.mu.Lock(); w.kaFails = 0; w.mu.Unlock() }

// due reports whether a forced reconnect is needed and why ("" also when the probe is stuck;
// Run handles that case separately).
func (w *Watchdog) due(now time.Time) string {
	if w.halted.Load() {
		return ""
	}
	up, ok := w.connected()
	if !ok {
		return ""
	}
	return w.dueWith(now, up)
}

func (w *Watchdog) dueWith(now time.Time, up bool) string {
	if w.halted.Load() {
		return ""
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.resumeAt.IsZero() && up && w.lastTransport.Load() > w.resumeAt.UnixNano() {
		w.resumeAt = time.Time{} // recovered: connected and a frame arrived after the resume
	}
	if up {
		w.downSince = time.Time{}
	} else if w.downSince.IsZero() {
		w.downSince = now
	}
	thr := 2
	if w.pending.Load() > 0 {
		thr = 1
	}
	resumed := w.resumed
	w.resumed = 0
	switch {
	case w.kaFails >= thr:
		return fmt.Sprintf("%d keepalive timeouts in a row", w.kaFails)
	case !w.resumeAt.IsZero() && w.ResumeMax > 0 && now.Sub(w.resumeAt) > w.ResumeMax:
		// Hard deadline after a resume: whatever whatsmeow's own auto-reconnect is doing, it
		// has not produced a live socket in time. Drop it and reconnect ourselves.
		since := now.Sub(w.resumeAt).Round(time.Second)
		w.resumeAt = time.Time{}
		return fmt.Sprintf("not recovered %s after resume (connected=%v)", since, up)
	case !w.downSince.IsZero() && now.Sub(w.downSince) > w.DownMax:
		return fmt.Sprintf("disconnected for %s", now.Sub(w.downSince).Round(time.Second))
	case w.downSince.IsZero() && resumed > 0 && w.sinceTransport(now) >= w.GapMin:
		// Just resumed from a pause and no frame since before it: the socket is almost surely
		// dead (server drops us after ~1 min without pings). Don't wait for TransportMax.
		return fmt.Sprintf("resumed after %s pause, transport silent for %s",
			resumed.Round(time.Second), w.sinceTransport(now).Round(time.Second))
	case w.downSince.IsZero() && w.transportStale(now):
		return fmt.Sprintf("transport silent for %s", w.sinceTransport(now).Round(time.Second))
	case w.downSince.IsZero() && now.Sub(time.Unix(0, w.lastApp.Load())) > 4*w.AppWindow &&
		w.sinceTransport(now) > w.TransportShort:
		return fmt.Sprintf("app quiet %s and transport silent %s",
			now.Sub(time.Unix(0, w.lastApp.Load())).Round(time.Second), w.sinceTransport(now).Round(time.Second))
	}
	return ""
}

// bounded runs f but stops waiting after d (a hung websocket close must not block us).
func bounded(d time.Duration, f func()) bool {
	done := make(chan struct{})
	go func() { f(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// disconnect tears the old socket down: Disconnect first (it marks the disconnect as expected,
// so whatsmeow won't auto-reconnect behind our back), and if it is still stuck in the graceful
// close after DropAfter, hard-close the TCP socket so it returns immediately.
func (w *Watchdog) disconnect() {
	done := make(chan struct{})
	go func() { w.C.Disconnect(); close(done) }()
	if w.Drop != nil {
		select {
		case <-done:
			return
		case <-time.After(w.DropAfter):
		}
		w.Drop()
		w.Log("graceful close stalled; dropped the socket")
	}
	select {
	case <-done:
	case <-time.After(w.CallMax):
		w.Log("disconnect hung; continuing")
	}
}

// connect is a bounded Connect: after ConnectMax the sockets are dropped (unblocks a dial or
// handshake stuck on a dead link); if it still has not returned after CallMax more, the client
// lock is held by a stuck call and ErrWedged is returned.
func (w *Watchdog) connect() error {
	ch := make(chan error, 1)
	go func() { ch <- w.C.Connect() }()
	select {
	case err := <-ch:
		return err
	case <-time.After(w.ConnectMax):
	}
	if w.Drop != nil {
		w.Drop()
		w.Log(fmt.Sprintf("connect hung %s; dropped the socket", w.ConnectMax))
	}
	select {
	case err := <-ch:
		if err == nil {
			return nil
		}
		return fmt.Errorf("connect hung %s, then: %w", w.ConnectMax, err)
	case <-time.After(w.CallMax):
		return fmt.Errorf("%w (Connect did not return in %s)", ErrWedged, w.ConnectMax+w.CallMax)
	}
}

func (w *Watchdog) reconnect(ctx context.Context, why string) error {
	w.reconnMu.Lock()
	defer w.reconnMu.Unlock()
	if w.halted.Load() {
		return ErrHalted
	}
	start, wait := w.Now(), w.Backoff0
	for attempt := 1; ; attempt++ {
		w.disconnect()
		err := w.connect()
		if errors.Is(err, ErrWedged) {
			w.Log(fmt.Sprintf("reconnect attempt %d (reason: %s): %v", attempt, why, err))
			return err
		}
		if errors.Is(err, whatsmeow.ErrAlreadyConnected) {
			err = nil
		}
		if err == nil {
			w.Log(fmt.Sprintf("reconnected (reason: %s, attempt %d)", why, attempt))
			w.reconnects.Add(1)
			w.mu.Lock()
			w.kaFails, w.downSince = 0, time.Time{}
			w.lastReconnAt, w.lastReconnWhy = w.Now(), why
			w.mu.Unlock()
			w.lastTransport.Store(w.Now().UnixNano())
			w.justReconn.Store(true)
			return nil
		}
		w.Log(fmt.Sprintf("reconnect attempt %d failed (reason: %s): %v", attempt, why, err))
		if w.Now().Sub(start) > w.GiveUp {
			return fmt.Errorf("reconnect failing for %s", w.Now().Sub(start).Round(time.Second))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(jitter(wait, rand.Float64())):
		}
		if wait *= 2; wait > time.Minute {
			wait = time.Minute
		}
	}
}

// Run blocks until ctx ends (nil) or reconnecting has failed for GiveUp (error).
func (w *Watchdog) Run(ctx context.Context) error {
	t := time.NewTicker(w.Tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			w.checkGap()
			now := w.Now()
			up, ok := w.connected()
			if !ok && !w.halted.Load() {
				if err := w.wedged(now); err != nil {
					return err
				}
				continue
			}
			if !w.wedgedAt.IsZero() {
				w.Log(fmt.Sprintf("client lock free again after %s", now.Sub(w.wedgedAt).Round(time.Second)))
				w.wedgedAt, w.wedgeDrop = time.Time{}, false
			}
			if !ok {
				continue
			}
			if why := w.dueWith(now, up); why != "" {
				if err := w.reconnect(ctx, why); err != nil {
					return err
				}
				w.lastTick = w.Now().UnixNano() // a slow reconnect is not a frozen process
			}
		}
	}
}

// wedged handles a stuck IsConnected probe: drop the sockets after WedgeDrop (a dial,
// handshake or close blocked on a dead link returns at once), give up after WedgeMax.
func (w *Watchdog) wedged(now time.Time) error {
	if w.wedgedAt.IsZero() {
		w.wedgedAt = now
		w.Log(fmt.Sprintf("client lock stuck (IsConnected blocked > %s); dropping sockets in %s, restarting in %s", w.ProbeMax, w.WedgeDrop, w.WedgeMax))
	}
	stuck := now.Sub(w.wedgedAt)
	if stuck >= w.WedgeDrop && !w.wedgeDrop {
		w.wedgeDrop = true
		if w.Drop != nil {
			w.Drop()
			w.Log("client lock stuck; dropped the socket")
		}
	}
	if stuck >= w.WedgeMax {
		w.Log(fmt.Sprintf("client lock still stuck after %s; exiting for the supervisor", stuck.Round(time.Second)))
		return fmt.Errorf("%w for %s", ErrWedged, stuck.Round(time.Second))
	}
	return nil
}

// jitter spreads d by +/-25% (r in [0,1)) so several profiles don't reconnect in lockstep.
func jitter(d time.Duration, r float64) time.Duration {
	return time.Duration(float64(d) * (0.75 + 0.5*r))
}

// checkGap compares wall-clock time between ticks (the monotonic clock may not advance while
// the VM is paused, so wall nanos are used) and records a frozen-process gap.
func (w *Watchdog) checkGap() time.Duration {
	n := w.Now().UnixNano()
	prev := w.lastTick
	w.lastTick = n
	if prev == 0 || w.GapMin <= 0 {
		return 0
	}
	gap := time.Duration(n - prev)
	if gap < w.GapMin {
		return 0
	}
	w.mu.Lock()
	w.lastGapAt, w.lastGap, w.resumed, w.resumeAt = w.Now(), gap, gap, w.Now()
	w.mu.Unlock()
	w.Log(fmt.Sprintf("process was frozen for %s (box paused/resumed?); transport check follows", gap.Round(time.Second)))
	return gap
}

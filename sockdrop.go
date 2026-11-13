package main

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"
)

// sockTracker hands whatsmeow an HTTP client for the websocket whose dialer remembers every
// raw TCP connection it opens, so the watchdog can hard-close them.
//
// Why: after a box pause/resume the socket is dead but whatsmeow's Disconnect still tries the
// graceful websocket close (coder/websocket writes a close frame with a hardcoded 5s ctx and
// then waits for the peer's close). On a dead link that always burns the full 5s ("failed to
// acquire lock: context deadline exceeded") before we can reconnect. Closing the underlying
// net.Conn makes every pending read/write and the close handshake fail at once.
type sockTracker struct {
	mu    sync.Mutex
	conns map[*trackedConn]struct{}
}

func newSockTracker() *sockTracker { return &sockTracker{conns: map[*trackedConn]struct{}{}} }

// Client returns an http.Client (DefaultTransport clone, so env proxies still apply) whose
// dials are tracked.
func (k *sockTracker) Client() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	base := t.DialContext
	if base == nil {
		base = (&net.Dialer{}).DialContext
	}
	// Bound the websocket upgrade: with no deadline a request lost on a link that died while the
	// box was paused waits forever inside whatsmeow's Connect, holding the client's socket lock
	// (IsConnected, Disconnect and the watchdog all block behind it). coder/websocket turns
	// Client.Timeout into a deadline on the whole dial (TCP+TLS+upgrade) and clears it after.
	t.ResponseHeaderTimeout = wsDialTimeout
	t.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := base(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return k.track(c), nil
	}
	return &http.Client{Transport: t, Timeout: wsDialTimeout}
}

func (k *sockTracker) track(c net.Conn) net.Conn {
	tc := &trackedConn{Conn: c, k: k}
	k.mu.Lock()
	k.conns[tc] = struct{}{}
	k.mu.Unlock()
	return tc
}

// Live is the number of tracked connections not yet closed.
func (k *sockTracker) Live() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.conns)
}

// DropAll hard-closes every live connection (no close frame) and returns how many it closed.
func (k *sockTracker) DropAll() int {
	k.mu.Lock()
	cs := make([]*trackedConn, 0, len(k.conns))
	for c := range k.conns {
		cs = append(cs, c)
	}
	k.mu.Unlock()
	for _, c := range cs {
		c.Close()
	}
	return len(cs)
}

type trackedConn struct {
	net.Conn
	k    *sockTracker
	once sync.Once
}

func (c *trackedConn) Close() error {
	c.once.Do(func() {
		c.k.mu.Lock()
		delete(c.k.conns, c)
		c.k.mu.Unlock()
	})
	return c.Conn.Close()
}

// wsDialTimeout bounds one websocket dial (TCP+TLS+upgrade); a var so tests can shorten it.
var wsDialTimeout = 20 * time.Second

var wsConns = newSockTracker()

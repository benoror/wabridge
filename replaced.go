package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// replacedRetry re-connects after a stream_replaced with a growing delay.
var replacedRetry = &replacedBackoff{}

const (
	replacedFirst = 60 * time.Second
	replacedCap   = 30 * time.Minute
)

// nextReplacedDelay doubles d up to the cap (0 starts at the first delay).
func nextReplacedDelay(d time.Duration) time.Duration {
	if d <= 0 {
		return replacedFirst
	}
	if d*2 > replacedCap {
		return replacedCap
	}
	return d * 2
}

type replacedBackoff struct {
	mu      sync.Mutex
	running bool
	delay   time.Duration
	tries   int
	since   time.Time
}

// Reset clears the backoff and replaced.json once we are connected again.
func (b *replacedBackoff) Reset(dir string) {
	b.mu.Lock()
	b.delay, b.tries, b.since = 0, 0, time.Time{}
	b.mu.Unlock()
	os.Remove(filepath.Join(dir, "replaced.json"))
}

// Start launches the retry loop once; connected() stops it, try() reconnects.
func (b *replacedBackoff) Start(ctx context.Context, dir string, connected func() bool, try func()) {
	b.mu.Lock()
	if b.running {
		b.mu.Unlock()
		return
	}
	b.running = true
	if b.since.IsZero() {
		b.since = time.Now()
	}
	b.mu.Unlock()
	go func() {
		defer func() { b.mu.Lock(); b.running = false; b.mu.Unlock() }()
		for {
			b.mu.Lock()
			b.delay = nextReplacedDelay(b.delay)
			d, n, since := b.delay, b.tries, b.since
			b.mu.Unlock()
			st, _ := json.Marshal(map[string]any{"since": since.UTC().Format(time.RFC3339), "tries": n, "next_retry_in_s": int(d.Seconds())})
			_ = os.WriteFile(filepath.Join(dir, "replaced.json"), st, 0o600)
			select {
			case <-ctx.Done():
				return
			case <-time.After(d):
			}
			if connected() {
				return
			}
			b.mu.Lock()
			b.tries++
			b.mu.Unlock()
			try()
			select {
			case <-ctx.Done():
				return
			case <-time.After(20 * time.Second):
			}
			if connected() {
				return
			}
		}
	}()
}

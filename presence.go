package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

// PresenceConfig is <profile>/presence.json. Every field is optional.
//
//	auto:    show typing/recording before outbox sends (default true)
//	max_ms:  cap for the automatic typing/recording delay (default 3000)
//	online:  "around_sends" (default): available just before a presence/send, unavailable
//	         a few seconds after the last one, so the paired phone keeps getting push
//	         notifications; "always": available on connect; "never": no online presence.
//	push_name: display name; if set and different from the session's, it is pushed to WhatsApp
//	         (app-state setting_pushName) once per process after connecting, so contacts see it
type PresenceConfig struct {
	Auto   *bool  `json:"auto,omitempty"`
	MaxMs  int    `json:"max_ms,omitempty"`
	Online string `json:"online,omitempty"`
	// PushName is pushed to WhatsApp by PushNameSyncer, and also used locally if the session
	// has none yet (whatsmeow refuses online presence without one).
	PushName string `json:"push_name,omitempty"`
}

func (c PresenceConfig) auto() bool { return c.Auto == nil || *c.Auto }
func (c PresenceConfig) max() time.Duration {
	if c.MaxMs <= 0 {
		return 3 * time.Second
	}
	return time.Duration(c.MaxMs) * time.Millisecond
}
func (c PresenceConfig) online() string {
	switch c.Online {
	case "always", "never":
		return c.Online
	}
	return "around_sends"
}

func loadPresenceConfig(dir string) (PresenceConfig, error) {
	var c PresenceConfig
	b, err := os.ReadFile(filepath.Join(dir, "presence.json"))
	if os.IsNotExist(err) {
		return c, nil
	}
	if err == nil {
		err = json.Unmarshal(b, &c)
	}
	return c, err
}

// Chat presence states.
const (
	PresComposing = "composing"
	PresRecording = "recording"
	PresPaused    = "paused"
)

// Presencer shows chat presence. Everything is best-effort: errors are logged, never returned.
type Presencer struct {
	Cfg     PresenceConfig
	Chat    func(ctx context.Context, to, state string) error // state: composing|recording|paused
	Online  func(ctx context.Context, available bool) error
	Refresh time.Duration // re-send interval for long holds (WhatsApp expires typing ~25s); default 10s
	Linger  time.Duration // around_sends: go unavailable this long after the last activity; default 8s

	mu     sync.Mutex
	holds  map[string]context.CancelFunc
	busy   int
	online bool
	offT   *time.Timer
	callTO time.Duration
}

func (p *Presencer) logErr(what string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s presence: %s: %v\n", time.Now().Format("15:04:05"), what, err)
	}
}

func (p *Presencer) call(ctx context.Context, f func(context.Context) error) error {
	to := p.callTO
	if to == 0 {
		to = 3 * time.Second
	}
	c, cancel := context.WithTimeout(ctx, to)
	defer cancel()
	return f(c)
}

func (p *Presencer) setOnline(ctx context.Context, v bool) {
	if p.Online == nil {
		return
	}
	p.logErr("online", p.call(ctx, func(c context.Context) error { return p.Online(c, v) }))
}

// Connected is called after each (re)connect.
func (p *Presencer) Connected(ctx context.Context) {
	switch p.Cfg.online() {
	case "always":
		p.setOnline(ctx, true)
	case "around_sends":
		p.mu.Lock()
		p.online = false
		idle := p.busy == 0
		p.mu.Unlock()
		if idle {
			p.setOnline(ctx, false) // make sure the phone keeps push notifications
		}
	}
}

func (p *Presencer) begin(ctx context.Context) {
	p.mu.Lock()
	p.busy++
	if p.offT != nil {
		p.offT.Stop()
		p.offT = nil
	}
	need := p.Cfg.online() == "around_sends" && !p.online
	p.online = p.online || need
	p.mu.Unlock()
	if need {
		p.setOnline(ctx, true)
	}
}

func (p *Presencer) end() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.busy > 0 {
		p.busy--
	}
	if p.busy > 0 || p.Cfg.online() != "around_sends" || !p.online {
		return
	}
	l := p.Linger
	if l == 0 {
		l = 8 * time.Second
	}
	if p.offT != nil {
		p.offT.Stop()
	}
	p.offT = time.AfterFunc(l, func() {
		p.mu.Lock()
		if p.busy > 0 || !p.online {
			p.mu.Unlock()
			return
		}
		p.online, p.offT = false, nil
		p.mu.Unlock()
		p.setOnline(context.Background(), false)
	})
}

func (p *Presencer) chat(ctx context.Context, to, state string) {
	if p.Chat == nil {
		return
	}
	p.logErr("chat "+state, p.call(ctx, func(c context.Context) error { return p.Chat(c, to, state) }))
}

// Hold shows state in chat `to` for d (refreshing), then paused. Non-blocking; a new hold
// for the same chat replaces the old one. state paused clears immediately.
func (p *Presencer) Hold(ctx context.Context, to, state string, d time.Duration) {
	p.mu.Lock()
	if p.holds == nil {
		p.holds = map[string]context.CancelFunc{}
	}
	if c := p.holds[to]; c != nil {
		c()
	}
	hctx, cancel := context.WithCancel(ctx)
	p.holds[to] = cancel
	p.mu.Unlock()
	p.begin(ctx)
	go func() {
		defer func() {
			p.mu.Lock()
			if p.holds[to] != nil && hctx.Err() == nil {
				delete(p.holds, to)
			}
			p.mu.Unlock()
			cancel()
			p.end()
		}()
		if state == PresPaused || d <= 0 {
			p.chat(ctx, to, PresPaused)
			return
		}
		ref := p.Refresh
		if ref == 0 {
			ref = 10 * time.Second
		}
		deadline := time.NewTimer(d)
		defer deadline.Stop()
		tick := time.NewTicker(ref)
		defer tick.Stop()
		p.chat(ctx, to, state)
		for {
			select {
			case <-hctx.Done():
				return // replaced by a newer hold/send (it sets its own state) or shutting down
			case <-tick.C:
				p.chat(ctx, to, state)
			case <-deadline.C:
				p.chat(ctx, to, PresPaused)
				return
			}
		}
	}()
}

// typingDelay is a realistic typing time for n characters, capped.
func typingDelay(n int, max time.Duration) time.Duration {
	d := 600*time.Millisecond + time.Duration(n)*40*time.Millisecond
	if d > max {
		d = max
	}
	return d
}

// recordingDelay: the note is already recorded; show "recording" briefly.
func recordingDelay(secs uint32, max time.Duration) time.Duration {
	d := time.Second + time.Duration(secs)*250*time.Millisecond
	if d > max {
		d = max
	}
	return d
}

// BeforeSend shows composing/recording for a short time before an outbox send (once per item,
// never inside retries). Returns when it is time to send. Forwards get no presence.
func (p *Presencer) BeforeSend(ctx context.Context, m OutMsg) {
	if p == nil || !p.Cfg.auto() || m.FwdID != "" {
		return
	}
	p.mu.Lock()
	if c := p.holds[m.To]; c != nil { // the send supersedes a manual hold
		c()
	}
	p.mu.Unlock()
	state, d := PresComposing, typingDelay(len([]rune(m.Text)), p.Cfg.max())
	if m.Voice != "" {
		state, d = PresRecording, recordingDelay(m.VoiceSecs, p.Cfg.max())
	}
	p.begin(ctx)
	p.chat(ctx, m.To, state)
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// AfterSend clears the chat presence and starts the online linger.
func (p *Presencer) AfterSend(ctx context.Context, m OutMsg) {
	if p == nil || !p.Cfg.auto() || m.FwdID != "" {
		return
	}
	p.chat(ctx, m.To, PresPaused)
	p.end()
}

// PresReq is a <profile>/presence/*.json request from `wabridge typing`.
type PresReq struct {
	To    string `json:"to"`
	State string `json:"state"`
	ForMs int64  `json:"for_ms"`
}

func validPresState(s string) bool {
	return s == PresComposing || s == PresRecording || s == PresPaused
}

func presenceDir(dir string) string {
	d := filepath.Join(dir, "presence")
	os.MkdirAll(d, 0o700)
	return d
}

// WatchRequests applies typing requests dropped by the CLI (separate from the outbox so a slow
// send never delays them).
func (p *Presencer) WatchRequests(ctx context.Context, dir string) {
	t := time.NewTicker(300 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.drain(ctx, dir)
		}
	}
}

func (p *Presencer) drain(ctx context.Context, dir string) {
	names, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	sort.Strings(names)
	for _, n := range names {
		var r PresReq
		err := readJSON(n, &r)
		os.Remove(n)
		if err != nil || r.To == "" || !validPresState(r.State) {
			p.logErr("bad request", fmt.Errorf("%s", filepath.Base(n)))
			continue
		}
		p.Hold(ctx, r.To, r.State, time.Duration(r.ForMs)*time.Millisecond)
	}
}

// requestPresence is the CLI side: drop a request and wait (briefly) for the bridge to take it.
func requestPresence(dir string, r PresReq, wait time.Duration) error {
	pd := presenceDir(dir)
	p := filepath.Join(pd, strconv.FormatInt(time.Now().UnixNano(), 36)+".json")
	if err := atomicWrite(p, r); err != nil {
		return err
	}
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(p); os.IsNotExist(err) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	os.Remove(p)
	return fmt.Errorf("bridge did not pick up the request")
}

// PushNameSyncer pushes the presence.json push_name to WhatsApp (app-state setting_pushName)
// so contacts see it, once per process: it retries on later connects until one succeeds.
// With an empty Want it does nothing.
type PushNameSyncer struct {
	Want string
	Have func() string                                // current session push name
	Send func(ctx context.Context, name string) error // app-state patch; on success caller stores it
	Log  func(format string, a ...any)

	mu   sync.Mutex
	done bool
}

// Sync is called after each (re)connect. Returns true if it sent the name in this call.
func (s *PushNameSyncer) Sync(ctx context.Context) bool {
	if s == nil || s.Want == "" || s.Send == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return false
	}
	if s.Have != nil && s.Have() == s.Want {
		s.done = true
		return false
	}
	if err := s.Send(ctx, s.Want); err != nil {
		if s.Log != nil {
			s.Log("push_name: set failed: %v", err)
		}
		return false
	}
	s.done = true
	if s.Log != nil {
		s.Log("push_name: set")
	}
	return true
}

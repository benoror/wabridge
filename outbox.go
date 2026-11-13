package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// OutMsg is an outbox request; Result is written back next to it.
type OutMsg struct {
	ID    string `json:"id"`
	To    string `json:"to"`
	Text  string `json:"text"`
	MsgID string `json:"msg_id,omitempty"` // WhatsApp message id; fixed before the first attempt and reused on retries

	ReplyTo     string `json:"reply_to,omitempty"`     // quoted message id
	ReplyChat   string `json:"reply_chat,omitempty"`   // chat of the quoted message (default: To)
	ReplySender string `json:"reply_sender,omitempty"` // author of the quoted message (default: from inbox)
	FwdChat     string `json:"fwd_chat,omitempty"`     // forward: source chat
	FwdID       string `json:"fwd_id,omitempty"`       // forward: source message id
	Voice       string `json:"voice,omitempty"`        // voice note: path to OGG/Opus mono
	VoiceSecs   uint32 `json:"voice_secs,omitempty"`
	Waveform    []byte `json:"waveform,omitempty"`  // 64 samples 0..100
	Image       string `json:"image,omitempty"`     // image: path to JPEG/PNG (<=16 MB), sent as-is
	Document    string `json:"document,omitempty"`  // document: path to any file (<=100 MB)
	FileName    string `json:"file_name,omitempty"` // document: name shown to the recipient (default: basename)
	Caption     string `json:"caption,omitempty"`   // image/document caption
}

// valid reports whether the item has something to send.
func (m OutMsg) valid() error {
	if m.To == "" || (m.Text == "" && m.FwdID == "" && m.Voice == "" && m.Image == "" && m.Document == "") {
		return fmt.Errorf("to and one of text/voice/image/document/forward required")
	}
	return m.validateMedia()
}

type Result struct {
	OutMsg
	OK        bool     `json:"ok"`
	MessageID string   `json:"message_id,omitempty"`
	Error     string   `json:"error,omitempty"`
	Attempts  int      `json:"attempts"`
	Errors    []string `json:"attempt_errors,omitempty"`
	Deduped   bool     `json:"deduped,omitempty"`
	TS        string   `json:"ts"`
}

// Sender sends text with the given WhatsApp message id and returns the server-acked id.
// Resending the same id is deduplicated by WhatsApp, so retries are safe.
type Sender func(ctx context.Context, m OutMsg) (string, error)

// Outbox drains <dir>/outbox with health checks and same-id retries.
type Outbox struct {
	Dir       string
	Send      Sender
	NewID     func() string
	Ensure    func(ctx context.Context) error             // make sure the socket is healthy before an attempt
	Reconnect func(ctx context.Context, why string) error // force reconnect after a failed attempt
	Attempts  int                                         // total attempts (default 4 = 1 + 3 retries)
	Backoff   time.Duration                               // first retry delay, doubled each time
	OnSent    func(Result)
	Pending   func(delta int) // optional: tells the watchdog an item is in flight
	Presence  *Presencer      // optional, best-effort typing/recording around each item (not per retry)
}

// lockRun takes an exclusive flock on <dir>/run.lock; returns nil if another run holds it.
func lockRun(dir string) *os.File {
	f, err := os.OpenFile(filepath.Join(dir, "run.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		die("lock: %v", err)
	}
	if syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		f.Close()
		return nil
	}
	f.Truncate(0)
	fmt.Fprintf(f, "%d\n", os.Getpid())
	return f
}

func runActive(dir string) bool {
	f := lockRun(dir)
	if f == nil {
		return true
	}
	f.Close() // releases lock
	return false
}

func atomicWrite(path string, v any) error {
	b, _ := json.Marshal(v)
	tmp := filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".tmp")
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// processOutbox handles every *.json in outbox once (oldest name first).
func (o *Outbox) processOutbox(ctx context.Context) {
	ob := o.Dir
	names, _ := filepath.Glob(filepath.Join(ob, "*.json"))
	sort.Strings(names)
	for _, p := range names {
		if ctx.Err() != nil {
			return
		}
		r := Result{}
		b, err := os.ReadFile(p)
		if err == nil {
			err = json.Unmarshal(b, &r.OutMsg)
		}
		if err == nil {
			err = r.valid()
		}
		if err == nil && r.MsgID != "" && o.alreadySent(r.MsgID) {
			os.Remove(p) // duplicate of a sent msg_id: never send twice
			continue
		}
		if err == nil && r.MsgID == "" && o.NewID != nil {
			// Persist the id before the first attempt so a crash/restart retries with the same id.
			r.MsgID = o.NewID()
			err = atomicWrite(p, r.OutMsg)
		}
		if err == nil {
			o.Presence.BeforeSend(ctx, r.OutMsg)
			r.MessageID, r.Attempts, r.Errors, err = o.sendWithRetry(ctx, r.OutMsg)
			o.Presence.AfterSend(ctx, r.OutMsg)
		}
		if err != nil && ctx.Err() != nil {
			return // shutting down: leave the item queued, it keeps its msg_id
		}
		r.OK = err == nil
		if err != nil {
			r.Error = err.Error()
		}
		if r.MessageID == "" {
			r.MessageID = r.MsgID
		}
		r.TS = time.Now().UTC().Format(time.RFC3339)
		sub := "sent"
		if !r.OK {
			sub = "failed"
		}
		atomicWrite(filepath.Join(ob, sub, filepath.Base(p)), r)
		os.Remove(p)
		if r.OK && o.OnSent != nil {
			o.OnSent(r)
		}
	}
}

// alreadySent reports whether outbox/sent holds a successful result with this msg_id.
func (o *Outbox) alreadySent(msgID string) bool {
	if _, err := os.Stat(filepath.Join(o.Dir, "sent", "m-"+safeName(msgID)+".json")); err == nil {
		return true
	}
	names, _ := filepath.Glob(filepath.Join(o.Dir, "sent", "*.json"))
	for _, p := range names {
		var r Result
		if readJSON(p, &r) == nil && r.OK && r.MsgID == msgID {
			return true
		}
	}
	return false
}

func safeName(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, s)
}

func (o *Outbox) sendWithRetry(ctx context.Context, m OutMsg) (string, int, []string, error) {
	n, wait := o.Attempts, o.Backoff
	if n <= 0 {
		n = 4
	}
	if o.Pending != nil {
		o.Pending(1)
		defer o.Pending(-1)
	}
	var errs []string
	var err error
	for a := 1; a <= n; a++ {
		err = nil
		if o.Ensure != nil {
			if err = o.Ensure(ctx); err != nil {
				err = fmt.Errorf("not ready: %w", err)
			}
		}
		if err == nil {
			var id string
			if id, err = o.Send(ctx, m); err == nil {
				return id, a, errs, nil // server ack
			}
		}
		errs = append(errs, err.Error())
		fmt.Fprintf(os.Stderr, "%s outbox: %s attempt %d/%d failed: %v\n", time.Now().Format("15:04:05"), m.ID, a, n, err)
		if a == n || ctx.Err() != nil {
			break
		}
		if o.Reconnect != nil {
			o.Reconnect(ctx, "outbox send failed")
		}
		select {
		case <-ctx.Done():
		case <-time.After(wait):
		}
		wait *= 2
	}
	return m.MsgID, len(errs), errs, err
}

func outboxDir(dir string) string {
	ob := filepath.Join(dir, "outbox")
	for _, d := range []string{ob, filepath.Join(ob, "sent"), filepath.Join(ob, "failed")} {
		os.MkdirAll(d, 0o700)
	}
	return ob
}

func (o *Outbox) Watch(ctx context.Context) {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			o.processOutbox(ctx)
		}
	}
}

// enqueue writes msg to the outbox and waits for its result.
func enqueue(ob string, m OutMsg, timeout time.Duration) (Result, error) {
	name := m.ID + ".json"
	// Idempotency: an item with this id already sent (or queued) is not queued again.
	if b, err := os.ReadFile(filepath.Join(ob, "sent", name)); err == nil {
		var r Result
		err = json.Unmarshal(b, &r)
		r.Deduped = true
		return r, err
	}
	if m.MsgID != "" {
		if _, err := os.Stat(filepath.Join(ob, name)); err == nil {
			goto wait // identical request already queued; just wait for its result
		}
	}
	if err := atomicWrite(filepath.Join(ob, name), m); err != nil {
		return Result{}, err
	}
wait:
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, sub := range []string{"sent", "failed"} {
			if b, err := os.ReadFile(filepath.Join(ob, sub, name)); err == nil {
				var r Result
				err = json.Unmarshal(b, &r)
				return r, err
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return Result{}, fmt.Errorf("timeout waiting for result (message %s still queued in outbox)", m.ID)
}

// Notifier debounces inbound messages and POSTs a summary (no text) to a webhook.
type Notifier struct {
	URL, Key, Profile, InboxPath string
	Delay                        time.Duration
	Cooldown                     time.Duration // min gap between POSTs; bursts coalesce into one notice
	lastPost                     time.Time
	mu                           sync.Mutex
	count                        int
	latest                       string
	chats                        map[string]bool
	timer                        *time.Timer
	// Pending (optional) reports unhandled claimable inbound lines. Used to retry a notice whose
	// POST failed (so an outage of the webhook endpoint can't swallow it) and by Kick callers.
	Pending func() (PendingInfo, error)
	Retries int // failed-POST retries while Pending>0 (default 5), spaced by Cooldown
	// Watermark (optional): Snapshot returns the inbox position a notice built now covers; after
	// a successful POST, Mark persists it so later settle checks don't re-announce those lines.
	Snapshot func() NoticeMark
	Mark     func(NoticeMark)
	Log      func(format string, a ...any)
	failures int
}

func (n *Notifier) logf(f string, a ...any) {
	if n.Log != nil {
		n.Log(f, a...)
	}
}

// Kick schedules a notice for pending unhandled lines (after a (re)connect, process start or an
// offline-sync batch). Like Add it respects the cooldown by deferring, never by dropping: the
// notice is scheduled for when the window opens. No-op when nothing is pending.
func (n *Notifier) Kick(pi PendingInfo, reason string) {
	if n == nil || n.URL == "" || pi.Count == 0 {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.chats == nil {
		n.chats = map[string]bool{}
	}
	if pi.Count > n.count {
		n.count = pi.Count // pending is a snapshot of the whole backlog; don't double count Add()s
	}
	for _, c := range pi.Chats {
		n.chats[c] = true
	}
	if pi.Latest > n.latest {
		n.latest = pi.Latest
	}
	d := time.Second
	if w := time.Until(n.lastPost.Add(n.Cooldown)); w > d {
		d = w
	}
	if n.timer == nil {
		n.timer = time.AfterFunc(d, n.flush)
	}
	n.logf("webhook: %d pending after %s; notice scheduled in %s", pi.Count, reason, d.Round(time.Second))
}

func loadWebhook(dir string) string {
	p := filepath.Join(dir, "webhook_url")
	b, err := os.ReadFile(p)
	if err != nil {
		return ""
	}
	os.Chmod(p, 0o600)
	return strings.TrimSpace(string(b))
}

// loadWebhookKey reads webhook_key (sent as "Authorization: Bearer <key>"); never logged.
func loadWebhookKey(dir string) string {
	p := filepath.Join(dir, "webhook_key")
	b, err := os.ReadFile(p)
	if err != nil {
		return ""
	}
	os.Chmod(p, 0o600)
	return strings.TrimSpace(string(b))
}

// Post sends body to the webhook (with auth if Key set), up to 3 attempts. Returns last status.
func (n *Notifier) Post(body []byte) (int, error) {
	cl := &http.Client{Timeout: 10 * time.Second}
	status, lastErr := 0, error(nil)
	for i := 0; i < 3; i++ {
		req, err := http.NewRequest("POST", n.URL, bytes.NewReader(body))
		if err != nil {
			return 0, fmt.Errorf("bad webhook url")
		}
		req.Header.Set("Content-Type", "application/json")
		if n.Key != "" {
			req.Header.Set("Authorization", "Bearer "+n.Key)
		}
		resp, err := cl.Do(req)
		if err == nil {
			resp.Body.Close()
			status = resp.StatusCode
			if status < 300 {
				return status, nil
			}
			lastErr = fmt.Errorf("status %d", status)
			if status == 401 || status == 403 {
				return status, lastErr // auth won't fix itself
			}
		} else {
			lastErr = fmt.Errorf("request failed") // err may contain URL; don't log
		}
		if i < 2 {
			time.Sleep(time.Duration(i+1) * 2 * time.Second)
		}
	}
	return status, lastErr
}

func (n *Notifier) Add(chat, ts string) {
	if n == nil || n.URL == "" {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.chats == nil {
		n.chats = map[string]bool{}
	}
	n.count++
	n.chats[chat] = true
	if ts > n.latest {
		n.latest = ts
	}
	if n.timer == nil {
		d := n.Delay
		if w := time.Until(n.lastPost.Add(n.Cooldown)); w > d {
			d = w
		}
		n.timer = time.AfterFunc(d, n.flush)
	}
}

func (n *Notifier) flush() {
	n.mu.Lock()
	chats := []string{}
	for c := range n.chats {
		chats = append(chats, c)
	}
	sort.Strings(chats)
	body, _ := json.Marshal(map[string]any{"profile": n.Profile, "count": n.count,
		"latest_ts": n.latest, "chats": chats, "inbox_path": n.InboxPath})
	n.count, n.latest, n.chats, n.timer = 0, "", nil, nil
	n.lastPost = time.Now()
	var mark *NoticeMark
	if n.Snapshot != nil {
		mk := n.Snapshot()
		mark = &mk
	}
	n.mu.Unlock()
	if countOf(body) == 0 {
		return // nothing to announce
	}
	st, err := n.Post(body)
	if err != nil {
		fmt.Fprintln(os.Stderr, "webhook: notify failed:", err, "status", st) // URL/key deliberately not logged
		n.mu.Lock()
		n.failures++
		f, max := n.failures, n.Retries
		n.mu.Unlock()
		if max == 0 {
			max = 5
		}
		if n.Pending != nil && f <= max && st != 401 && st != 403 {
			if pi, perr := n.Pending(); perr == nil && pi.Count > 0 {
				n.Kick(pi, "failed notice (retry)")
			}
		}
		return
	}
	n.mu.Lock()
	n.failures = 0
	n.mu.Unlock()
	if mark != nil && n.Mark != nil {
		n.Mark(*mark)
	}
	n.logf("webhook: notice sent (count %d)", countOf(body))
}

// gaveUp: the last notices kept failing past Retries (or auth failed); periodic checks back off
// until a notice succeeds again (any successful POST resets the counter).
func (n *Notifier) gaveUp() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	max := n.Retries
	if max == 0 {
		max = 5
	}
	return n.failures > max
}

func countOf(body []byte) int {
	var m struct {
		Count int `json:"count"`
	}
	json.Unmarshal(body, &m)
	return m.Count
}

// Settler debounces "the connection has settled" after a (re)connect or an offline-sync batch:
// Trigger(d) (re)arms one timer; when it fires, Fn runs once. Connected arms a longer fallback,
// OfflineSyncCompleted shortens it, so one burst of events yields one check.
type Settler struct {
	Fn    func()
	mu    sync.Mutex
	timer *time.Timer
}

func (s *Settler) Trigger(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.timer != nil {
		s.timer.Stop()
	}
	s.timer = time.AfterFunc(d, s.Fn)
}

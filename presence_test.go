package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type presLog struct {
	mu  sync.Mutex
	evs []string
}

func (l *presLog) add(s string) { l.mu.Lock(); l.evs = append(l.evs, s); l.mu.Unlock() }
func (l *presLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.evs...)
}

func newTestPres(cfg PresenceConfig, l *presLog, fail bool) *Presencer {
	return &Presencer{Cfg: cfg, Refresh: 30 * time.Millisecond, Linger: 40 * time.Millisecond,
		Chat: func(ctx context.Context, to, st string) error {
			l.add(to + ":" + st)
			if fail {
				return errors.New("boom")
			}
			return nil
		},
		Online: func(ctx context.Context, on bool) error {
			if on {
				l.add("online")
			} else {
				l.add("offline")
			}
			return nil
		}}
}

func TestPresenceConfigDefaults(t *testing.T) {
	c, err := loadPresenceConfig(t.TempDir())
	if err != nil || !c.auto() || c.max() != 3*time.Second || c.online() != "around_sends" {
		t.Fatalf("defaults: %+v %v", c, err)
	}
	d := t.TempDir()
	os.WriteFile(filepath.Join(d, "presence.json"), []byte(`{"auto":false,"max_ms":1500,"online":"never"}`), 0o600)
	c, _ = loadPresenceConfig(d)
	if c.auto() || c.max() != 1500*time.Millisecond || c.online() != "never" {
		t.Fatalf("parsed: %+v", c)
	}
}

func TestDelaysCapped(t *testing.T) {
	if d := typingDelay(10000, 3*time.Second); d != 3*time.Second {
		t.Fatal(d)
	}
	if d := typingDelay(5, 3*time.Second); d >= time.Second {
		t.Fatal(d)
	}
	if d := recordingDelay(60, 2*time.Second); d != 2*time.Second {
		t.Fatal(d)
	}
}

func TestBeforeAfterSendText(t *testing.T) {
	l := &presLog{}
	p := newTestPres(PresenceConfig{MaxMs: 50}, l, false)
	m := OutMsg{To: "a", Text: "hola mundo largo largo"}
	start := time.Now()
	p.BeforeSend(context.Background(), m)
	if time.Since(start) < 45*time.Millisecond {
		t.Fatal("no typing delay")
	}
	p.AfterSend(context.Background(), m)
	time.Sleep(100 * time.Millisecond)
	got := l.get()
	want := []string{"online", "a:composing", "a:paused", "offline"}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v", got)
		}
	}
}

func TestVoiceRecordingAndForwardSkipped(t *testing.T) {
	l := &presLog{}
	p := newTestPres(PresenceConfig{MaxMs: 10, Online: "never"}, l, false)
	p.BeforeSend(context.Background(), OutMsg{To: "a", Voice: "x.ogg"})
	p.AfterSend(context.Background(), OutMsg{To: "a", Voice: "x.ogg"})
	p.BeforeSend(context.Background(), OutMsg{To: "b", FwdID: "f"})
	p.AfterSend(context.Background(), OutMsg{To: "b", FwdID: "f"})
	got := l.get()
	if len(got) != 2 || got[0] != "a:recording" || got[1] != "a:paused" {
		t.Fatalf("got %v", got)
	}
}

func TestAutoOffAndNilPresencer(t *testing.T) {
	l := &presLog{}
	f := false
	p := newTestPres(PresenceConfig{Auto: &f}, l, false)
	p.BeforeSend(context.Background(), OutMsg{To: "a", Text: "x"})
	p.AfterSend(context.Background(), OutMsg{To: "a", Text: "x"})
	var np *Presencer
	np.BeforeSend(context.Background(), OutMsg{To: "a", Text: "x"})
	np.AfterSend(context.Background(), OutMsg{To: "a", Text: "x"})
	if len(l.get()) != 0 {
		t.Fatal(l.get())
	}
}

func TestHoldRefreshesThenPauses(t *testing.T) {
	l := &presLog{}
	p := newTestPres(PresenceConfig{Online: "never"}, l, false)
	p.Hold(context.Background(), "a", PresComposing, 100*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	got := l.get()
	n := 0
	for _, e := range got {
		if e == "a:composing" {
			n++
		}
	}
	if n < 3 || got[len(got)-1] != "a:paused" {
		t.Fatalf("got %v", got)
	}
}

func TestPresenceFailureNeverFailsSend(t *testing.T) {
	l := &presLog{}
	p := newTestPres(PresenceConfig{MaxMs: 5}, l, true)
	sends := 0
	o := &Outbox{Dir: outboxDir(t.TempDir()), Presence: p, Attempts: 2,
		Send: func(ctx context.Context, m OutMsg) (string, error) { sends++; return "X1", nil }}
	r, err := enqueueAndProcess(o, OutMsg{ID: "i1", To: "a", Text: "hi", MsgID: "X1"})
	if err != nil || !r.OK || sends != 1 {
		t.Fatalf("r=%+v err=%v sends=%d", r, err, sends)
	}
}

func TestPresenceNotPerRetry(t *testing.T) {
	l := &presLog{}
	p := newTestPres(PresenceConfig{MaxMs: 5, Online: "never"}, l, false)
	n := 0
	o := &Outbox{Dir: outboxDir(t.TempDir()), Presence: p, Attempts: 3, Backoff: time.Millisecond,
		Send: func(ctx context.Context, m OutMsg) (string, error) {
			n++
			if n < 3 {
				return "", errors.New("x")
			}
			return "X2", nil
		}}
	r, _ := enqueueAndProcess(o, OutMsg{ID: "i2", To: "a", Text: "hi", MsgID: "X2"})
	if !r.OK || r.Attempts != 3 {
		t.Fatalf("%+v", r)
	}
	if got := l.get(); len(got) != 2 {
		t.Fatalf("presence per retry: %v", got)
	}
}

func TestRequestRoundTrip(t *testing.T) {
	l := &presLog{}
	p := newTestPres(PresenceConfig{Online: "never"}, l, false)
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.WatchRequests(ctx, presenceDir(dir))
	if err := requestPresence(dir, PresReq{To: "a", State: PresRecording, ForMs: 50}, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	got := l.get()
	if len(got) < 2 || got[0] != "a:recording" || got[len(got)-1] != "a:paused" {
		t.Fatalf("%v", got)
	}
}

func enqueueAndProcess(o *Outbox, m OutMsg) (Result, error) {
	if err := atomicWrite(filepath.Join(o.Dir, m.ID+".json"), m); err != nil {
		return Result{}, err
	}
	o.processOutbox(context.Background())
	var r Result
	err := readJSON(filepath.Join(o.Dir, "sent", m.ID+".json"), &r)
	return r, err
}

func TestPushNameSyncer(t *testing.T) {
	ctx := context.Background()
	var sent []string
	have := ""
	fail := errors.New("no app state keys")
	var sendErr error
	s := &PushNameSyncer{Want: "Example Bot",
		Have: func() string { return have },
		Send: func(_ context.Context, n string) error {
			sent = append(sent, n)
			if sendErr != nil {
				return sendErr
			}
			have = n
			return nil
		}}
	sendErr = fail
	if s.Sync(ctx) {
		t.Fatal("failed send reported success")
	}
	sendErr = nil
	if !s.Sync(ctx) {
		t.Fatal("retry after failure should send")
	}
	if s.Sync(ctx) {
		t.Fatal("should send once per process")
	}
	if len(sent) != 2 || sent[1] != "Example Bot" {
		t.Fatalf("sent=%v", sent)
	}

	// Already set: no send.
	n := 0
	s2 := &PushNameSyncer{Want: "X", Have: func() string { return "X" },
		Send: func(context.Context, string) error { n++; return nil }}
	if s2.Sync(ctx) || n != 0 {
		t.Fatal("same name must not be sent")
	}
	// No push_name configured: never sends (existing profiles unchanged).
	s3 := &PushNameSyncer{Have: func() string { return "" },
		Send: func(context.Context, string) error { n++; return nil }}
	if s3.Sync(ctx) || n != 0 {
		t.Fatal("empty push_name must not send")
	}
}

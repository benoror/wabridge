package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOutboxRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ob := outboxDir(dir)
	var sent []Result
	fake := func(_ context.Context, m OutMsg) (string, error) {
		to, text, id := m.To, m.Text, m.MsgID
		_, _, _ = to, text, id
		if to == "bad" {
			return "", errors.New("boom")
		}
		return "WAID-" + text, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o := &Outbox{Dir: ob, Send: fake, Attempts: 1, OnSent: func(r Result) { sent = append(sent, r) }}
	go o.Watch(ctx)

	r, err := enqueue(ob, OutMsg{ID: "a1", To: "521", Text: "hi"}, 5*time.Second)
	if err != nil || !r.OK || r.MessageID != "WAID-hi" || r.TS == "" {
		t.Fatalf("ok case: %+v %v", r, err)
	}
	r, err = enqueue(ob, OutMsg{ID: "b2", To: "bad", Text: "x"}, 5*time.Second)
	if err != nil || r.OK || r.Error != "boom" {
		t.Fatalf("fail case: %+v %v", r, err)
	}
	r, _ = enqueue(ob, OutMsg{ID: "c3", To: "521"}, 5*time.Second)
	if r.OK {
		t.Fatal("empty text should fail")
	}
	if len(sent) != 1 {
		t.Fatalf("onSent called %d times", len(sent))
	}
}

func TestRunLock(t *testing.T) {
	dir := t.TempDir()
	if runActive(dir) {
		t.Fatal("should be inactive")
	}
	f := lockRun(dir)
	defer f.Close()
	if !runActive(dir) {
		t.Fatal("should be active")
	}
}

func TestNotifierDebounce(t *testing.T) {
	got := make(chan string, 5)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- string(b)
	}))
	defer srv.Close()
	n := &Notifier{URL: srv.URL, Profile: "p", InboxPath: "/x", Delay: 100 * time.Millisecond}
	n.Add("c1", "2026-01-01T00:00:01Z")
	n.Add("c2", "2026-01-01T00:00:02Z")
	n.Add("c1", "2026-01-01T00:00:00Z")
	b := <-got
	var m map[string]any
	json.Unmarshal([]byte(b), &m)
	if m["count"].(float64) != 3 || m["latest_ts"] != "2026-01-01T00:00:02Z" || !strings.Contains(b, `"chats":["c1","c2"]`) {
		t.Fatal(b)
	}
	select {
	case b := <-got:
		t.Fatal("extra post", b)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestNotifierAuthHeader(t *testing.T) {
	got := make(chan string, 2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Get("Authorization")
		if r.Header.Get("Authorization") != "Bearer k123" {
			w.WriteHeader(401)
		}
	}))
	defer srv.Close()
	n := &Notifier{URL: srv.URL, Key: "k123", Profile: "p", InboxPath: "/x", Delay: 50 * time.Millisecond}
	n.Add("c1", "2026-01-01T00:00:01Z")
	if h := <-got; h != "Bearer k123" {
		t.Fatal("header", h)
	}
	bad := &Notifier{URL: srv.URL, Key: "wrong"}
	if st, err := bad.Post([]byte("{}")); err == nil || st != 401 {
		t.Fatal("expected 401", st, err)
	}
	<-got
	select {
	case <-got:
		t.Fatal("401 retried")
	case <-time.After(100 * time.Millisecond):
	}
}

// Disconnect on the first attempt, success on the second: same id reused, one success, one reconnect.
func TestOutboxRetrySameID(t *testing.T) {
	ob := outboxDir(t.TempDir())
	var ids []string
	successes, reconnects, ensures, gens := 0, 0, 0, 0
	fake := func(_ context.Context, m OutMsg) (string, error) {
		to, text, id := m.To, m.Text, m.MsgID
		_, _, _ = to, text, id
		ids = append(ids, id)
		if len(ids) == 1 {
			return "", errors.New("websocket disconnected before message send returned response")
		}
		successes++
		return id, nil
	}
	var sent []Result
	o := &Outbox{Dir: ob, Send: fake, Backoff: time.Millisecond,
		NewID:     func() string { gens++; return "3EBTESTID" },
		Ensure:    func(context.Context) error { ensures++; return nil },
		Reconnect: func(context.Context, string) error { reconnects++; return nil },
		OnSent:    func(r Result) { sent = append(sent, r) }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { o.Watch(ctx); close(done) }()
	r, err := enqueue(ob, OutMsg{ID: "r1", To: "g@g.us", Text: "hola"}, 5*time.Second)
	cancel()
	<-done
	if err != nil || !r.OK || r.Attempts != 2 || r.MessageID != "3EBTESTID" || r.MsgID != "3EBTESTID" || len(r.Errors) != 1 {
		t.Fatalf("result: %+v %v", r, err)
	}
	if len(ids) != 2 || ids[0] != ids[1] || successes != 1 || reconnects != 1 || ensures != 2 || gens != 1 || len(sent) != 1 {
		t.Fatalf("ids=%v successes=%d reconnects=%d ensures=%d gens=%d sent=%d", ids, successes, reconnects, ensures, gens, len(sent))
	}
}

// A caller-provided msg_id is used as-is; persistent failure stops after Attempts.
func TestOutboxGivenIDAndGiveUp(t *testing.T) {
	ob := outboxDir(t.TempDir())
	var ids []string
	o := &Outbox{Dir: ob, Backoff: time.Millisecond, NewID: func() string { return "NEW" },
		Send: func(_ context.Context, m OutMsg) (string, error) {
			id := m.MsgID
			ids = append(ids, id)
			return "", errors.New("timed out")
		}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { o.Watch(ctx); close(done) }()
	r, _ := enqueue(ob, OutMsg{ID: "r2", To: "x", Text: "y", MsgID: "GIVEN"}, 5*time.Second)
	cancel()
	<-done
	if r.OK || r.Attempts != 4 || r.MessageID != "GIVEN" || len(ids) != 4 {
		t.Fatalf("%+v ids=%v", r, ids)
	}
	for _, id := range ids {
		if id != "GIVEN" {
			t.Fatal(ids)
		}
	}
}

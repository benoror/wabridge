package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
)

func writeInbox(t *testing.T, dir string, lines ...string) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, "inbox.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, l := range lines {
		f.WriteString(l + "\n")
	}
}

// Offline-sync / delayed lines nobody handled yet are claimable like any new message.
func TestOfflineUnhandledIsClaimable(t *testing.T) {
	dir := t.TempDir()
	ib := &Inbox{Dir: dir}
	if l, _ := ib.Claim(time.Minute); l != nil { // establish offset at (empty) EOF
		t.Fatal("unexpected lease")
	}
	writeInbox(t, dir,
		`{"id":"A","chat":"g@g.us","sender":"e@s.whatsapp.net","ts":"2026-10-17T15:26:52Z","type":"media","offline":true,"delayed":true}`,
		`{"id":"B","chat":"g@g.us","sender":"e@s.whatsapp.net","ts":"2026-10-17T15:26:53Z","type":"text","offline":true,"replay":true}`)
	pi, err := ib.Pending()
	if err != nil || pi.Count != 2 || pi.Latest != "2026-10-17T15:26:53Z" || len(pi.Chats) != 1 {
		t.Fatalf("pending %+v %v", pi, err)
	}
	l, err := ib.Claim(time.Minute)
	if err != nil || l == nil || len(l.Messages) != 2 {
		t.Fatalf("offline lines not claimable: %+v %v", l, err)
	}
	g := groupMessages(l.ID, l.Messages)
	if len(g) != 1 || len(g[0].MessageIDs) != 2 || !g[0].Delayed {
		t.Fatalf("groups %+v", g)
	}
	if b, _ := json.Marshal(g); strings.Contains(string(b), "replay_only") {
		t.Fatal("replay_only must be gone")
	}
	if pi, _ := ib.Pending(); pi.Count != 0 {
		t.Fatal("leased lines still pending")
	}
}

// An offline line already handled (claimed + done) is never offered again, and a redelivered copy
// is dropped by seen.log before it reaches the inbox.
func TestOfflineAlreadyDoneSkipped(t *testing.T) {
	dir := t.TempDir()
	ib := &Inbox{Dir: dir}
	ib.Claim(time.Minute)
	writeInbox(t, dir, `{"id":"A","chat":"g@g.us","type":"text","offline":true}`)
	l, _ := ib.Claim(time.Minute)
	if l == nil || ib.Done(l.ID) != nil {
		t.Fatal("claim/done")
	}
	if l2, _ := ib.Claim(time.Minute); l2 != nil {
		t.Fatal("done offline line re-offered")
	}
	if pi, _ := ib.Pending(); pi.Count != 0 {
		t.Fatal("done line counted pending")
	}
	s := &SeenSet{Path: filepath.Join(dir, "seen.log")}
	s.Load()
	defer s.Close()
	j, _ := types.ParseJID("g@g.us")
	if s.Has(j, "A") {
		t.Fatal("not marked yet")
	}
	s.Mark(j, "A")
	if !s.Has(j, "A") || !s.Seen(j, "A") {
		t.Fatal("duplicate not detected")
	}
}

// Expired leases are pending again; live leases are not.
func TestPendingCountsExpiredLease(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	ib := &Inbox{Dir: dir, Now: func() time.Time { return now }}
	ib.Claim(time.Minute)
	writeInbox(t, dir, `{"id":"A","chat":"c@s.whatsapp.net","type":"text"}`)
	ib.Claim(time.Minute)
	if pi, _ := ib.Pending(); pi.Count != 0 {
		t.Fatal("live lease counted")
	}
	now = now.Add(2 * time.Minute)
	if pi, _ := ib.Pending(); pi.Count != 1 {
		t.Fatal("expired lease not pending")
	}
}

// After a (re)connect the settle check notifies when unhandled lines are waiting, and not otherwise.
func TestWebhookFiresAfterReconnectWithPending(t *testing.T) {
	var posts atomic.Int32
	got := make(chan map[string]any, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		json.Unmarshal(b, &m)
		posts.Add(1)
		got <- m
	}))
	defer srv.Close()
	dir := t.TempDir()
	ib := &Inbox{Dir: dir}
	ib.Claim(time.Minute)
	n := &Notifier{URL: srv.URL, Profile: "p", Delay: 10 * time.Millisecond, Cooldown: 50 * time.Millisecond, Pending: ib.Pending}
	st := &Settler{Fn: func() { pi, _ := ib.Pending(); n.Kick(pi, "test") }}
	st.Trigger(10 * time.Millisecond) // connect with nothing pending: no notice
	time.Sleep(1500 * time.Millisecond)
	if posts.Load() != 0 {
		t.Fatal("notice with nothing pending")
	}
	writeInbox(t, dir, `{"id":"A","chat":"g@g.us","ts":"2026-10-17T15:26:52Z","type":"text","offline":true,"delayed":true}`,
		`{"id":"B","chat":"g@g.us","ts":"2026-10-17T15:27:52Z","type":"media","offline":true,"delayed":true}`)
	st.Trigger(time.Hour)             // Connected arms the long fallback...
	st.Trigger(10 * time.Millisecond) // ...OfflineSyncCompleted shortens it: one check
	select {
	case m := <-got:
		if m["count"].(float64) != 2 || m["latest_ts"] != "2026-10-17T15:27:52Z" {
			t.Fatalf("body %v", m)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no notice after reconnect with pending lines")
	}
	time.Sleep(300 * time.Millisecond)
	if posts.Load() != 1 {
		t.Fatalf("posts %d", posts.Load())
	}
}

// The cooldown defers a notice to when the window opens; it never drops it.
func TestWebhookSpacingDefersNotDrops(t *testing.T) {
	got := make(chan time.Time, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { got <- time.Now() }))
	defer srv.Close()
	n := &Notifier{URL: srv.URL, Delay: 10 * time.Millisecond, Cooldown: 1500 * time.Millisecond}
	n.lastPost = time.Now() // a notice just went out
	start := time.Now()
	n.Kick(PendingInfo{Count: 3, Chats: []string{"g"}}, "reconnect")
	n.Kick(PendingInfo{Count: 3, Chats: []string{"g"}}, "offline sync") // coalesces
	select {
	case at := <-got:
		if at.Sub(start) < 1200*time.Millisecond {
			t.Fatal("cooldown not respected")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("notice swallowed by spacing")
	}
	select {
	case <-got:
		t.Fatal("duplicate notice")
	case <-time.After(300 * time.Millisecond):
	}
}

// A failed POST is retried after the cooldown while lines are still pending.
func TestWebhookRetriesFailedNotice(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 3 { // first notice: all 3 attempts fail
			w.WriteHeader(502)
		}
	}))
	defer srv.Close()
	n := &Notifier{URL: srv.URL, Delay: 10 * time.Millisecond, Cooldown: 100 * time.Millisecond,
		Pending: func() (PendingInfo, error) { return PendingInfo{Count: 1}, nil }}
	n.Add("c", "t")
	deadline := time.Now().Add(20 * time.Second)
	for calls.Load() < 4 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if calls.Load() < 4 {
		t.Fatal("failed notice not retried")
	}
}

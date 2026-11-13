package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func appendLines(t *testing.T, dir string, from, n int, fromMe bool) {
	f, err := os.OpenFile(filepath.Join(dir, "inbox.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for i := from; i < from+n; i++ {
		b, _ := json.Marshal(map[string]any{"id": fmt.Sprintf("M%d", i), "from_me": fromMe, "type": "text"})
		f.Write(append(b, '\n'))
	}
}

func ids(l *Lease) []string {
	var out []string
	for _, m := range l.Messages {
		var x struct{ ID string }
		json.Unmarshal(m, &x)
		out = append(out, x.ID)
	}
	return out
}

func TestClaimConcurrentExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	ib := &Inbox{Dir: dir}
	ib.Claim(time.Minute) // init offset at EOF (empty)
	const total = 200
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				l, err := (&Inbox{Dir: dir}).Claim(time.Minute)
				if err != nil {
					t.Error(err)
					return
				}
				if l != nil {
					mu.Lock()
					for _, id := range ids(l) {
						seen[id]++
					}
					mu.Unlock()
					(&Inbox{Dir: dir}).Done(l.ID)
					continue
				}
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
	}
	for i := 0; i < total; i += 10 {
		appendLines(t, dir, i, 10, false)
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
	if len(seen) != total {
		t.Fatalf("claimed %d distinct, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("%s claimed %d times", id, n)
		}
	}
}

func TestClaimInitFromLastHandledAndEmpty(t *testing.T) {
	dir := t.TempDir()
	appendLines(t, dir, 0, 5, false)
	os.WriteFile(filepath.Join(dir, "last_handled"), []byte(`{"id": "M2"}`), 0o600)
	ib := &Inbox{Dir: dir}
	l, _ := ib.Claim(time.Minute)
	if l == nil || fmt.Sprint(ids(l)) != "[M3 M4]" {
		t.Fatalf("got %v", l)
	}
	if l.MsgID != leaseMsgID(l.ID) || len(l.MsgID) != 20 {
		t.Fatalf("msg id %q", l.MsgID)
	}
	if l2, _ := ib.Claim(time.Minute); l2 != nil {
		t.Fatal("second claim should be empty")
	}
	appendLines(t, dir, 10, 2, true) // own sends only: no lease
	if l2, _ := ib.Claim(time.Minute); l2 != nil {
		t.Fatal("from_me-only should be empty")
	}
	if err := ib.Done(l.ID); err != nil {
		t.Fatal(err)
	}
	if ib.Done(l.ID) == nil {
		t.Fatal("double done should fail")
	}
}

func TestReleaseAndExpiryReoffer(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	ib := &Inbox{Dir: dir, Now: func() time.Time { return now }}
	ib.Claim(time.Minute)
	appendLines(t, dir, 0, 3, false)
	l, _ := ib.Claim(time.Minute)
	ib.Release(l.ID)
	l2, _ := ib.Claim(time.Minute)
	if l2 == nil || l2.ID == l.ID || fmt.Sprint(ids(l2)) != "[M0 M1 M2]" {
		t.Fatalf("release not reoffered: %v", l2)
	}
	if x, _ := ib.Claim(time.Minute); x != nil {
		t.Fatal("live lease must not be reoffered")
	}
	now = now.Add(2 * time.Minute)
	l3, _ := ib.Claim(time.Minute)
	if l3 == nil || fmt.Sprint(ids(l3)) != "[M0 M1 M2]" {
		t.Fatal("expired lease not reoffered")
	}
	if ib.Done(l2.ID) == nil {
		t.Fatal("stale lease done should fail")
	}
}

func TestInboxRunLock(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	ib := &Inbox{Dir: dir, Now: func() time.Time { return now }}
	tok, _ := ib.Lock("a", 10*time.Minute)
	if tok == "" {
		t.Fatal("first lock")
	}
	if t2, _ := ib.Lock("b", 10*time.Minute); t2 != "" {
		t.Fatal("second lock must fail")
	}
	if ib.Unlock("wrong") == nil {
		t.Fatal("wrong token unlock")
	}
	now = now.Add(11 * time.Minute)
	t3, _ := ib.Lock("c", 10*time.Minute)
	if t3 == "" {
		t.Fatal("expired lock not taken over")
	}
	if ib.Unlock(t3) != nil {
		t.Fatal("unlock")
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	got := 0
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if tk, _ := (&Inbox{Dir: dir}).Lock("x", time.Minute); tk != "" {
				mu.Lock()
				got++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if got != 1 {
		t.Fatalf("%d runs got the lock", got)
	}
}

func TestOutboxDedupesMsgID(t *testing.T) {
	ob := outboxDir(t.TempDir())
	sends := 0
	o := &Outbox{Dir: ob, Attempts: 1, Send: func(_ context.Context, m OutMsg) (string, error) { sends++; return m.MsgID, nil }}
	m := OutMsg{ID: "m-" + safeName("3EB0AA"), To: "1", Text: "x", MsgID: "3EB0AA"}
	atomicWrite(filepath.Join(ob, m.ID+".json"), m)
	o.processOutbox(context.Background())
	r, err := enqueue(ob, m, time.Second) // duplicate request: answered from sent/, not re-queued
	if err != nil || !r.Deduped || !r.OK {
		t.Fatalf("dup enqueue: %+v %v", r, err)
	}
	m2 := m
	m2.ID = "other" // same msg_id under another name still not resent
	atomicWrite(filepath.Join(ob, "other.json"), m2)
	o.processOutbox(context.Background())
	if sends != 1 {
		t.Fatalf("sent %d times", sends)
	}
}

func TestNotifierCooldownCoalesces(t *testing.T) {
	n := &Notifier{URL: "x", Delay: 10 * time.Millisecond, Cooldown: time.Hour}
	n.lastPost = time.Now()
	n.Add("c", "t")
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.timer == nil {
		t.Fatal("no timer")
	}
	n.timer.Stop() // would fire ~1h out, not 10ms
}

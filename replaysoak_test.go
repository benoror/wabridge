package main

// Replay soak suite: recorded, SANITIZED event sequences (testdata/replay/*.json) driven through
// the real inbound path (MsgPipe.Handle -> classifyProtocol -> SeenSet -> inbox.jsonl -> Settler ->
// Inbox.Pending -> Notifier -> HTTP webhook) with a fake clock and a fake webhook server.
// Run before restarting a live profile: scripts/soak.sh.
//
// Fixture format (all values synthetic: fake JIDs, placeholder text):
//
//	{"name","description",
//	 "skip": "reason"                      // documented expected-improvement case (t.Skip)
//	 "steps": [
//	   {"op":"connected"}                  // events.Connected: gate + settle arm
//	   {"op":"offline_sync","active":true} // OfflineSyncPreview (true) / OfflineSyncCompleted (false)
//	   {"op":"message","ev":{"id","chat","sender","from_me","is_group","type","push_name",
//	                         "ts_offset_s","message":{waE2E protojson},"raw_message":{...},"is_edit"}}
//	   {"op":"advance","seconds":N}        // fake clock
//	   {"op":"settle"}                     // let settle/notifier timers fire (real time, short)
//	   {"op":"restart"}                    // process restart: reload seen.log, fresh gate/notifier
//	   {"op":"claim"}                      // establish the claim offset (state.json), no lease kept
//	   {"op":"claim_done"}                 // a claim/done bot handles everything claimable
//	   {"op":"claim_abandon"}              // a run claims (lease TTL 1 min, fake clock) and dies
//	   {"op":"lease_tick"}                 // one periodic expired-lease check (watchLeases)
//	   {"op":"bot_handled","id":"X"}       // a cursor bot writes last_handled {"id":X}
//	   {"op":"expect","lines":N,"pending":N,"unhandled":N,"notices":N,"last_notice_count":N,
//	                  // pending = unhandled AND not yet announced (what a settle check notifies)
//	                  "line":{"index":i,"fields":{...}}}  // subset match on inbox line i (0-based; -1 = last)
//	 ]}

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type replayFixture struct {
	Name        string       `json:"name"`
	Description string       `json:"description"`
	Skip        string       `json:"skip"`
	Steps       []replayStep `json:"steps"`
}

type replayStep struct {
	Op      string          `json:"op"`
	Active  bool            `json:"active"`
	Seconds float64         `json:"seconds"`
	ID      string          `json:"id"`
	Ev      *replayEvent    `json:"ev"`
	Lines   *int            `json:"lines"`
	Pending *int            `json:"pending"`
	Unhandl *int            `json:"unhandled"`
	Notices *int            `json:"notices"`
	LastCnt *int            `json:"last_notice_count"`
	Line    *replayLineWant `json:"line"`
	Note    string          `json:"note"`
}

type replayLineWant struct {
	Index  int            `json:"index"`
	Fields map[string]any `json:"fields"`
	Absent []string       `json:"absent"`
}

type replayEvent struct {
	ID        string          `json:"id"`
	Chat      string          `json:"chat"`
	Sender    string          `json:"sender"`
	FromMe    bool            `json:"from_me"`
	IsGroup   bool            `json:"is_group"`
	Type      string          `json:"type"`
	PushName  string          `json:"push_name"`
	TSOffsetS float64         `json:"ts_offset_s"`
	Message   json.RawMessage `json:"message"`
	Raw       json.RawMessage `json:"raw_message"`
	IsEdit    bool            `json:"is_edit"`
}

// fakeHook records webhook POSTs (bodies carry counts and chat ids only, never text).
type fakeHook struct {
	mu     sync.Mutex
	counts []int
}

func (h *fakeHook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	h.mu.Lock()
	h.counts = append(h.counts, countOf(b))
	h.mu.Unlock()
}

func (h *fakeHook) snapshot() []int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]int(nil), h.counts...)
}

type replayRig struct {
	t      *testing.T
	dir    string
	clk    *hClock
	start  time.Time
	hook   *fakeHook
	url    string
	ib     *Inbox
	seen   *SeenSet
	notif  *Notifier
	settle *Settler
	pipe   *MsgPipe
	f      *os.File
	encMu  sync.Mutex
	logs   []string
	logMu  sync.Mutex
}

func (r *replayRig) logf(f string, a ...any) {
	r.logMu.Lock()
	r.logs = append(r.logs, f) // format only: never args (could carry ids/content in other paths)
	r.logMu.Unlock()
}

// boot builds the same object graph as `run` (main.go), minus the WhatsApp client.
func (r *replayRig) boot() {
	t := r.t
	if r.f != nil {
		r.f.Close()
		r.seen.Close()
	}
	f, err := os.OpenFile(filepath.Join(r.dir, "inbox.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	r.f = f
	enc := json.NewEncoder(f)
	write := func(v any) { r.encMu.Lock(); enc.Encode(v); r.encMu.Unlock() }
	r.seen = &SeenSet{Path: filepath.Join(r.dir, "seen.log"), Now: r.clk.Now}
	if err := r.seen.Load(); err != nil {
		t.Fatal(err)
	}
	r.ib = &Inbox{Dir: r.dir, Now: r.clk.Now}
	r.notif = &Notifier{URL: r.url, Profile: "soak", InboxPath: f.Name(), Delay: 20 * time.Millisecond,
		Cooldown: 50 * time.Millisecond, Pending: r.ib.Pending, Snapshot: r.ib.NotifyMark, Log: r.logf}
	ib := r.ib
	r.notif.Mark = func(mk NoticeMark) { ib.MarkNotified(mk) }
	notif := r.notif
	r.settle = &Settler{Fn: func() { settleCheck(ib, notif, r.logf) }}
	r.pipe = &MsgPipe{Seen: r.seen, Gate: &ReplayGate{}, Write: write, Log: r.logf, MediaDir: filepath.Join(r.dir, "media"),
		Settle: r.settle.Trigger, Notify: notif.Add, Now: r.clk.Now, IsPoll: func(id string) bool { return pollMarked(r.dir, id) },
		ConnectSettle: 60 * time.Millisecond, OfflineSettle: 20 * time.Millisecond}
}

func (r *replayRig) close() {
	if r.f != nil {
		r.f.Close()
		r.seen.Close()
	}
}

func (r *replayRig) lines() []map[string]any {
	b, _ := os.ReadFile(filepath.Join(r.dir, "inbox.jsonl"))
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if l == "" {
			continue
		}
		var m map[string]any
		if json.Unmarshal([]byte(l), &m) != nil {
			r.t.Fatalf("bad inbox line")
		}
		out = append(out, m)
	}
	return out
}

func fxJID(t *testing.T, s string) types.JID {
	if s == "" {
		return types.EmptyJID
	}
	j, err := types.ParseJID(s)
	if err != nil {
		t.Fatalf("jid %q: %v", s, err)
	}
	return j
}

func (r *replayRig) event(e *replayEvent) *events.Message {
	t := r.t
	msg := &waE2E.Message{}
	if len(e.Message) > 0 {
		if err := protojson.Unmarshal(e.Message, msg); err != nil {
			t.Fatalf("message %s: %v", e.ID, err)
		}
	}
	raw := msg
	if len(e.Raw) > 0 {
		raw = &waE2E.Message{}
		if err := protojson.Unmarshal(e.Raw, raw); err != nil {
			t.Fatalf("raw_message %s: %v", e.ID, err)
		}
	}
	typ := e.Type
	if typ == "" {
		typ = "text"
	}
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: fxJID(t, e.Chat), Sender: fxJID(t, e.Sender),
				IsFromMe: e.FromMe, IsGroup: e.IsGroup},
			ID: types.MessageID(e.ID), Type: typ, PushName: e.PushName,
			Timestamp: r.start.Add(time.Duration(e.TSOffsetS * float64(time.Second))),
		},
		Message: msg, RawMessage: raw, IsEdit: e.IsEdit,
	}
}

func (r *replayRig) step(i int, s replayStep) {
	t := r.t
	where := func() string { return s.Op + " step " + itoa(i) + " " + s.Note }
	switch s.Op {
	case "mark_poll": // `wabridge poll --message-id ID` (or send --poll)
		if err := markPoll(r.dir, s.ID); err != nil {
			r.t.Fatal(err)
		}
	case "connected":
		r.pipe.Connected()
	case "offline_sync":
		r.pipe.OfflineSync(s.Active)
	case "message":
		r.pipe.Handle(r.event(s.Ev))
	case "advance":
		r.clk.Add(time.Duration(s.Seconds * float64(time.Second)))
	case "settle":
		time.Sleep(1400 * time.Millisecond) // settle timers (<=60ms) + Kick's 1s floor + POST
	case "restart":
		r.boot()
	case "claim":
		if l, err := r.ib.Claim(time.Minute); err != nil {
			t.Fatalf("%s: %v", where(), err)
		} else if l != nil {
			r.ib.Release(l.ID)
		}
	case "claim_abandon": // a run claims and dies without done/release; the lease expires after 1 min
		if _, err := r.ib.Claim(time.Minute); err != nil {
			t.Fatalf("%s: %v", where(), err)
		}
	case "lease_tick": // one tick of the periodic expired-lease check (main: every 30s)
		leaseCheck(r.ib, r.notif, r.logf)
	case "claim_done":
		for {
			l, err := r.ib.Claim(time.Minute)
			if err != nil {
				t.Fatalf("%s: %v", where(), err)
			}
			if l == nil {
				break
			}
			r.ib.Done(l.ID)
		}
	case "bot_handled":
		b, _ := json.Marshal(map[string]string{"id": s.ID, "ts": rfc(r.clk.Now())})
		if err := os.WriteFile(filepath.Join(r.dir, "last_handled"), b, 0o644); err != nil {
			t.Fatal(err)
		}
	case "expect":
		ls := r.lines()
		if s.Lines != nil && len(ls) != *s.Lines {
			t.Errorf("%s: inbox lines %d, want %d", where(), len(ls), *s.Lines)
		}
		if s.Pending != nil {
			pi, err := r.ib.Pending()
			if err != nil || pi.Count != *s.Pending {
				t.Errorf("%s: pending %d (unhandled %d) err %v, want %d", where(), pi.Count, pi.Unhandled, err, *s.Pending)
			}
		}
		if s.Unhandl != nil {
			if pi, _ := r.ib.Pending(); pi.Unhandled != *s.Unhandl {
				t.Errorf("%s: unhandled %d, want %d", where(), pi.Unhandled, *s.Unhandl)
			}
		}
		posts := r.hook.snapshot()
		if s.Notices != nil && len(posts) != *s.Notices {
			t.Errorf("%s: webhook notices %d %v, want %d", where(), len(posts), posts, *s.Notices)
		}
		if s.LastCnt != nil && (len(posts) == 0 || posts[len(posts)-1] != *s.LastCnt) {
			t.Errorf("%s: last notice counts %v, want last=%d", where(), posts, *s.LastCnt)
		}
		if w := s.Line; w != nil {
			idx := w.Index
			if idx < 0 {
				idx += len(ls)
			}
			if idx < 0 || idx >= len(ls) {
				t.Errorf("%s: no inbox line %d (have %d)", where(), w.Index, len(ls))
				return
			}
			got := ls[idx]
			for k, want := range w.Fields {
				if !reflect.DeepEqual(got[k], want) {
					t.Errorf("%s: line %d field %q = %#v, want %#v", where(), w.Index, k, got[k], want)
				}
			}
			for _, k := range w.Absent {
				if _, ok := got[k]; ok {
					t.Errorf("%s: line %d has field %q, want absent", where(), w.Index, k)
				}
			}
		}
	default:
		t.Fatalf("unknown op %q", s.Op)
	}
}

func itoa(i int) string { b, _ := json.Marshal(i); return string(b) }

// The fixtures must stay synthetic: no real-looking phone numbers, JIDs or long free text.
func checkSanitized(t *testing.T, name string, b []byte) {
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			for _, y := range x {
				walk(y)
			}
		case []any:
			for _, y := range x {
				walk(y)
			}
		case string:
			if at := strings.IndexByte(x, '@'); at > 0 {
				user := x[:at]
				if i := strings.IndexAny(user, ":."); i >= 0 {
					user = user[:i]
				}
				ok := user == "111" || user == "222" || user == "333" || user == "999" || strings.HasPrefix(user, "120363000000000")
				if !ok {
					t.Errorf("%s: non-synthetic JID %q (use 111/222/333/999@s.whatsapp.net or 120363000000000000@g.us)", name, x)
				}
			}
		}
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	walk(v)
}

func TestReplaySoak(t *testing.T) {
	files, _ := filepath.Glob("testdata/replay/*.json")
	if len(files) == 0 {
		t.Fatal("no replay fixtures")
	}
	for _, p := range files {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		var fx replayFixture
		if err := json.Unmarshal(b, &fx); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		t.Run(strings.TrimSuffix(filepath.Base(p), ".json"), func(t *testing.T) {
			checkSanitized(t, p, b)
			if fx.Skip != "" {
				t.Skip(fx.Skip)
			}
			t.Parallel()
			hook := &fakeHook{}
			srv := httptest.NewServer(hook)
			defer srv.Close()
			clk := newHClock()
			r := &replayRig{t: t, dir: t.TempDir(), clk: clk, start: clk.Now(), hook: hook, url: srv.URL}
			r.boot()
			defer r.close()
			for i, s := range fx.Steps {
				r.step(i, s)
			}
		})
	}
}

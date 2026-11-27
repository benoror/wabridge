package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	waCommon "go.mau.fi/whatsmeow/proto/waCommon"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waHistorySync "go.mau.fi/whatsmeow/proto/waHistorySync"
	waWeb "go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func histMsg(id string, ts time.Time, fromMe bool) *waHistorySync.HistorySyncMsg {
	return &waHistorySync.HistorySyncMsg{Message: &waWeb.WebMessageInfo{
		Key:              &waCommon.MessageKey{ID: proto.String(id), FromMe: proto.Bool(fromMe)},
		MessageTimestamp: proto.Uint64(uint64(ts.Unix())),
		Message:          &waE2E.Message{Conversation: proto.String("placeholder")},
	}}
}

func fakeParse(chat types.JID, wm *waWeb.WebMessageInfo) (*events.Message, error) {
	return &events.Message{Info: types.MessageInfo{
		MessageSource: types.MessageSource{Chat: chat, Sender: types.JID{User: "111", Server: "s.whatsapp.net"},
			IsFromMe: wm.GetKey().GetFromMe(), IsGroup: chat.Server == types.GroupServer},
		ID: wm.GetKey().GetID(), Type: "text", Timestamp: time.Unix(int64(wm.GetMessageTimestamp()), 0),
	}, Message: wm.GetMessage()}, nil
}

type histRig struct {
	dir      string
	lines    []map[string]any
	notified int
	settled  int
	h        *HistoryReplayer
}

func newHistRig(t *testing.T, inbox string, allow string) *histRig {
	r := &histRig{dir: t.TempDir()}
	ip := filepath.Join(r.dir, "inbox.jsonl")
	os.WriteFile(ip, []byte(inbox), 0o600)
	if allow != "" {
		os.WriteFile(filepath.Join(r.dir, HistoryChatsFile), []byte(allow), 0o600)
	}
	ss := &SeenSet{Path: filepath.Join(r.dir, "seen.log")}
	ss.Load()
	t.Cleanup(func() { ss.Close() })
	pipe := &MsgPipe{Seen: ss, Gate: &ReplayGate{}, Log: func(string, ...any) {},
		Write:  func(x any) { r.lines = append(r.lines, x.(map[string]any)) },
		Notify: func(string, string) { r.notified++ }, Settle: func(time.Duration) { r.settled++ },
		RetryReceipt: func(*types.MessageInfo, []byte) error { t.Fatal("replay must never send"); return nil }}
	r.h = &HistoryReplayer{Dir: r.dir, InboxPath: ip, Pipe: pipe, Parse: fakeParse, Log: func(string, ...any) {},
		Now: func() time.Time { return time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC) }}
	r.h.Init()
	return r
}

const histInbox = `{"id":"OLD1","chat":"120363000000000000@g.us","from_me":false,"ts":"2026-01-10T09:00:00Z","type":"text","text":"x"}
{"id":"MINE","chat":"120363000000000000@g.us","from_me":true,"ts":"2026-01-10T10:00:00Z","type":"text","text":"x"}
`

func TestHistoryReplayAllowlistWatermarkDedupe(t *testing.T) {
	r := newHistRig(t, histInbox, "# allowed\n120363000000000000@g.us\n222@lid # a 1:1\n")
	if want := time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC); !r.h.Since.Equal(want) {
		t.Fatalf("watermark %v want %v (from_me lines must not move it)", r.h.Since, want)
	}
	g := "120363000000000000@g.us"
	base := time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC)
	hs := &events.HistorySync{Data: &waHistorySync.HistorySync{SyncType: waHistorySync.HistorySync_RECENT.Enum(),
		Conversations: []*waHistorySync.Conversation{
			{ID: proto.String(g), Messages: []*waHistorySync.HistorySyncMsg{ // newest first, like WhatsApp
				histMsg("NEW2", base.Add(2*time.Hour), false),
				histMsg("MINE", base.Add(time.Hour), true), // already in inbox
				histMsg("NEW1", base.Add(30*time.Minute), true),
				histMsg("OLD1", base, false),                 // at watermark
				histMsg("OLD0", base.Add(-time.Hour), false), // before watermark
			}},
			{ID: proto.String("120363999999999999@g.us"), Messages: []*waHistorySync.HistorySyncMsg{
				histMsg("NOPE", base.Add(3*time.Hour), false)}}, // not allowlisted
			{ID: proto.String("222@lid"), Messages: []*waHistorySync.HistorySyncMsg{
				histMsg("DM1", base.Add(time.Hour), false)}},
		}}}
	if n := r.h.Handle(hs); n != 3 {
		t.Fatalf("wrote %d want 3: %+v", n, r.lines)
	}
	want := []string{"NEW1", "DM1", "NEW2"} // chronological across chats
	for i, l := range r.lines {
		if l["id"] != want[i] || l["replay"] != true || l["source"] != "history_sync" || l["delayed"] != nil || l["offline"] != nil {
			t.Fatalf("line %d: %+v", i, l)
		}
	}
	if r.notified != 0 || r.settled == 0 {
		t.Fatalf("replay must batch via settle, never per-message notify (notified %d settled %d)", r.notified, r.settled)
	}
	// Same blob again (WhatsApp re-sends chunks): nothing new.
	if n := r.h.Handle(hs); n != 0 {
		t.Fatalf("second pass wrote %d", n)
	}
}

func TestHistoryReplayFailsClosedWithoutAllowlist(t *testing.T) {
	r := newHistRig(t, histInbox, "")
	hs := &events.HistorySync{Data: &waHistorySync.HistorySync{Conversations: []*waHistorySync.Conversation{
		{ID: proto.String("120363000000000000@g.us"), Messages: []*waHistorySync.HistorySyncMsg{
			histMsg("NEW", time.Date(2026, 1, 10, 11, 0, 0, 0, time.UTC), false)}}}}}
	if n := r.h.Handle(hs); n != 0 || len(r.lines) != 0 {
		t.Fatalf("no history_chats.txt must write nothing, wrote %d", n)
	}
}

func TestHistoryReplayAliasAndFloor(t *testing.T) {
	r := newHistRig(t, "", "333@lid\n")
	if want := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC).Add(-HistoryMaxBack); !r.h.Since.Equal(want) {
		t.Fatalf("empty inbox watermark %v want floor %v", r.h.Since, want)
	}
	r.h.Alt = func(j types.JID) types.JID {
		if j.String() == "5218112345678@s.whatsapp.net" {
			return types.JID{User: "333", Server: types.HiddenUserServer}
		}
		return types.EmptyJID
	}
	hs := &events.HistorySync{Data: &waHistorySync.HistorySync{Conversations: []*waHistorySync.Conversation{
		{ID: proto.String("5218112345678@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{
			histMsg("PN1", time.Date(2026, 1, 9, 0, 0, 0, 0, time.UTC), false),
			histMsg("ANCIENT", time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC), false)}}}}}
	if n := r.h.Handle(hs); n != 1 || r.lines[0]["id"] != "PN1" {
		t.Fatalf("PN alias of allowlisted LID: wrote %d %+v", n, r.lines)
	}
}

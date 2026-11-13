package main

import (
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"os"
	"testing"
	"time"

	waCommon "go.mau.fi/whatsmeow/proto/waCommon"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

func reactMsg(target, emoji string, fromMe bool, participant string) *waE2E.Message {
	k := &waCommon.MessageKey{ID: proto.String(target), FromMe: proto.Bool(fromMe)}
	if participant != "" {
		k.Participant = proto.String(participant)
	}
	return &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{Key: k, Text: proto.String(emoji)}}
}

func TestClassifyReaction(t *testing.T) {
	pl := classifyProtocol(evt("R1", reactMsg("T1", "👍", true, "222@s.whatsapp.net")))
	if pl.Skip || pl.Type != "reaction" || pl.SeenID != "R1" {
		t.Fatalf("reaction: %+v", pl)
	}
	f := pl.Fields
	if f["reacted_id"] != "T1" || f["emoji"] != "👍" || f["reacted_from_me"] != true ||
		f["reacted_sender"] != "222@s.whatsapp.net" || f["removed"] != nil || f["text"] != nil {
		t.Fatalf("reaction fields: %+v", f)
	}
}

func TestClassifyReactionRemoved(t *testing.T) {
	pl := classifyProtocol(evt("R2", reactMsg("T1", "", false, "")))
	if pl.Skip || pl.Type != "reaction" || pl.Fields["emoji"] != "" || pl.Fields["removed"] != true {
		t.Fatalf("removal: %+v", pl)
	}
	if _, ok := pl.Fields["reacted_sender"]; ok {
		t.Fatal("1:1 reaction has no participant: reacted_sender must be absent")
	}
}

func TestClassifyReactionWithoutTarget(t *testing.T) {
	m := &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{Text: proto.String("👍")}}
	if pl := classifyProtocol(evt("R3", m)); !pl.Skip || pl.NoMark || pl.Reason != "reaction without target id" {
		t.Fatalf("no target: %+v", pl)
	}
}

func TestReactionIsHandleable(t *testing.T) {
	if isUpdateLine("reaction") {
		t.Fatal("reaction lines are handleable (they wake the routine, 2026-10-25), not updates")
	}
}

// Reactions wake the routine (2026-10-25): pending and claimable like any message, but never
// form a reply group (the bot reads them in the lease's messages, like poll votes).
func TestReactionsPendingAndClaimable(t *testing.T) {
	dir := t.TempDir()
	ib := &Inbox{Dir: dir}
	if l, _ := ib.Claim(time.Minute); l != nil {
		t.Fatal("unexpected lease")
	}
	r := `{"id":"R1","chat":"g@g.us","sender":"e@s.whatsapp.net","ts":"2026-10-19T10:00:00Z","type":"reaction","reacted_id":"T1","reacted_from_me":true,"emoji":"+"}`
	writeInbox(t, dir, r, `{"id":"R2","chat":"g@g.us","sender":"e@s.whatsapp.net","ts":"2026-10-19T10:00:01Z","type":"reaction","reacted_id":"T1","reacted_from_me":true,"emoji":"","removed":true}`)
	if pi, err := ib.Pending(); err != nil || pi.Count != 2 || pi.Unhandled != 2 {
		t.Fatalf("reactions pending: %+v %v", pi, err)
	}
	l, err := ib.Claim(time.Minute)
	if err != nil || l == nil || len(l.Messages) != 2 {
		t.Fatalf("reaction-only batch claim: %+v %v", l, err)
	}
	if g := groupMessages(l.ID, l.Messages); len(g) != 0 {
		t.Fatalf("reactions must form no reply group: %+v", g)
	}
	writeInbox(t, dir, `{"id":"T2","chat":"g@g.us","sender":"e@s.whatsapp.net","ts":"2026-10-19T10:00:02Z","type":"text","text":"x"}`)
	l, err = ib.Claim(time.Minute)
	if err != nil || l == nil {
		t.Fatalf("lease: %+v %v", l, err)
	}
	g := groupMessages(l.ID, l.Messages)
	if len(g) != 1 || len(g[0].MessageIDs) != 1 || g[0].MessageIDs[0] != "T2" || g[0].LastID != "T2" {
		t.Fatalf("groups %+v", g)
	}
}

func TestPollMarks(t *testing.T) {
	d := t.TempDir()
	if pollMarked(d, "P1") {
		t.Fatal("no file: nothing marked")
	}
	for i := 0; i < 2; i++ {
		if err := markPoll(d, " P1 "); err != nil {
			t.Fatal(err)
		}
	}
	if !pollMarked(d, "P1") || pollMarked(d, "P2") || pollMarked(d, "") {
		t.Fatal("pollMarked")
	}
	if b, _ := os.ReadFile(pollsPath(d)); string(b) != "P1\n" {
		t.Fatalf("idempotent append: %q", b)
	}
	if markPoll(d, "a\nb") == nil || markPoll(d, "") == nil {
		t.Fatal("bad ids must be rejected")
	}
}

func TestEmptyTextKind(t *testing.T) {
	if k, _ := emptyTextKind(&waE2E.Message{}); k != "" {
		t.Fatalf("bare message: %q", k)
	}
	if k, _ := emptyTextKind(&waE2E.Message{ContactMessage: &waE2E.ContactMessage{}}); k != "contact" {
		t.Fatalf("contact: %q", k)
	}
	k, x := emptyTextKind(&waE2E.Message{PinInChatMessage: &waE2E.PinInChatMessage{}})
	if k != "unsupported" || x["subtype"] != "pinInChatMessage" {
		t.Fatalf("pin: %q %v", k, x)
	}
}

// An encrypted reaction is decrypted and then handled exactly like a plain one.
func TestEncryptedReactionDecrypted(t *testing.T) {
	var lines []map[string]any
	ss := &SeenSet{Path: t.TempDir() + "/seen.log"}
	_ = ss.Load()
	p := &MsgPipe{Seen: ss, Gate: &ReplayGate{}, Log: func(string, ...any) {},
		Write:  func(x any) { lines = append(lines, x.(map[string]any)) },
		Notify: func(string, string) {}, Settle: func(time.Duration) {},
		DecryptReaction: func(*events.Message) (*waE2E.ReactionMessage, error) {
			return &waE2E.ReactionMessage{Key: &waCommon.MessageKey{ID: proto.String("T9"), FromMe: proto.Bool(true)}, Text: proto.String("+")}, nil
		}}
	e := evt("ER1", &waE2E.Message{EncReactionMessage: &waE2E.EncReactionMessage{}})
	e.Info.Chat, e.Info.Type = types.JID{User: "1", Server: "g.us"}, "reaction"
	p.Handle(e)
	if len(lines) != 1 || lines[0]["type"] != "reaction" || lines[0]["reacted_id"] != "T9" || lines[0]["emoji"] != "+" {
		t.Fatalf("decrypted reaction line: %+v", lines)
	}
}

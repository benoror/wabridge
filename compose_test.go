package main

import (
	"os"
	"path/filepath"
	"testing"

	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

func testStore(t *testing.T) Lookup {
	d := t.TempDir()
	inbox := filepath.Join(d, "inbox.jsonl")
	os.WriteFile(inbox, []byte(
		`{"id":"Q1","chat":"g@g.us","sender":"a:2@s.whatsapp.net","text":"hola","type":"text"}`+"\n"+
			`{"id":"Q2","chat":"me@s.whatsapp.net","from_me":true,"text":"mine","type":"text","via":"outbox"}`+"\n"), 0o600)
	img := &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Caption: proto.String("cap"), URL: proto.String("u"),
		ContextInfo: &waE2E.ContextInfo{IsForwarded: proto.Bool(true), ForwardingScore: proto.Uint32(3)}}}
	raw, _ := proto.Marshal(img)
	os.MkdirAll(filepath.Join(d, "media"), 0o700)
	os.WriteFile(filepath.Join(d, "media", "IMG.msg.pb"), raw, 0o600)
	af, _ := os.OpenFile(inbox, os.O_APPEND|os.O_WRONLY, 0o600)
	af.WriteString(`{"id":"IMG","chat":"g@g.us","sender":"b@s.whatsapp.net","type":"image"}` + "\n")
	af.Close()
	return inboxLookup(inbox, filepath.Join(d, "media"))
}

func TestReplyBuildsContextInfo(t *testing.T) {
	look := testStore(t)
	m, err := buildMessage(OutMsg{To: "g@g.us", Text: "va", ReplyTo: "Q1"}, look, "me@s.whatsapp.net", nil)
	if err != nil {
		t.Fatal(err)
	}
	et := m.GetExtendedTextMessage()
	if m.Conversation != nil || et.GetText() != "va" {
		t.Fatal("reply not upgraded to ExtendedTextMessage")
	}
	ci := et.GetContextInfo()
	if ci.GetStanzaID() != "Q1" || ci.GetParticipant() != "a@s.whatsapp.net" || ci.GetQuotedMessage().GetConversation() != "hola" || ci.RemoteJID != nil {
		t.Fatalf("bad context %v", ci)
	}
	// Own message: participant = own JID; cross-chat reply sets RemoteJID.
	m, _ = buildMessage(OutMsg{To: "x@s.whatsapp.net", Text: "y", ReplyTo: "Q2", ReplyChat: "me@s.whatsapp.net"}, look, "me@s.whatsapp.net", nil)
	ci = m.GetExtendedTextMessage().GetContextInfo()
	if ci.GetParticipant() != "me@s.whatsapp.net" || ci.GetRemoteJID() != "me@s.whatsapp.net" {
		t.Fatalf("own/cross-chat: %v", ci)
	}
	// Unknown id with explicit sender still quotes.
	m, _ = buildMessage(OutMsg{To: "g@g.us", Text: "y", ReplyTo: "ZZ", ReplySender: "c@s.whatsapp.net"}, look, "", nil)
	if ci = m.GetExtendedTextMessage().GetContextInfo(); ci.GetParticipant() != "c@s.whatsapp.net" || ci.QuotedMessage == nil {
		t.Fatalf("unknown quote: %v", ci)
	}
	// Quoting a forwarded image: quote carries the image, without its nested context.
	m, _ = buildMessage(OutMsg{To: "g@g.us", Text: "y", ReplyTo: "IMG"}, look, "", nil)
	q := m.GetExtendedTextMessage().GetContextInfo().GetQuotedMessage()
	if q.GetImageMessage().GetCaption() != "cap" || q.GetImageMessage().ContextInfo != nil {
		t.Fatalf("media quote: %v", q)
	}
}

func TestReplyOnVoice(t *testing.T) {
	v := &waE2E.Message{AudioMessage: &waE2E.AudioMessage{PTT: proto.Bool(true)}}
	m, err := buildMessage(OutMsg{To: "g@g.us", Voice: "x.ogg", ReplyTo: "Q1"}, testStore(t), "", v)
	if err != nil || m.GetAudioMessage().GetContextInfo().GetStanzaID() != "Q1" || !m.GetAudioMessage().GetPTT() {
		t.Fatalf("voice reply: %v %v", m, err)
	}
}

func TestForward(t *testing.T) {
	look := testStore(t)
	m, err := buildMessage(OutMsg{To: "z@s.whatsapp.net", FwdChat: "g@g.us", FwdID: "Q1"}, look, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	ci := m.GetExtendedTextMessage().GetContextInfo()
	if m.GetExtendedTextMessage().GetText() != "hola" || !ci.GetIsForwarded() || ci.GetForwardingScore() != 1 || ci.StanzaID != nil {
		t.Fatalf("text forward: %v", m)
	}
	m, err = buildMessage(OutMsg{To: "z@s.whatsapp.net", FwdChat: "g@g.us", FwdID: "IMG"}, look, "", nil)
	if err != nil || m.GetImageMessage().GetURL() != "u" || m.GetImageMessage().GetContextInfo().GetForwardingScore() != 4 {
		t.Fatalf("media forward: %v %v", m, err)
	}
	if _, err := buildMessage(OutMsg{To: "z", FwdChat: "other@g.us", FwdID: "Q1"}, look, "", nil); err == nil {
		t.Fatal("forward matched wrong chat")
	}
}

func TestOutMsgValid(t *testing.T) {
	for _, m := range []OutMsg{{To: "a", Text: "t"}, {To: "a", Voice: "v"}, {To: "a", FwdID: "f"}} {
		if m.valid() != nil {
			t.Fatalf("%+v should be valid", m)
		}
	}
	if (OutMsg{To: "a"}).valid() == nil || (OutMsg{Text: "x"}).valid() == nil {
		t.Fatal("empty item accepted")
	}
}

func TestInboundExtras(t *testing.T) {
	m := &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String("ok"),
		ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String("Q"), Participant: proto.String("a:5@s.whatsapp.net"),
			QuotedMessage: &waE2E.Message{Conversation: proto.String(string(make([]rune, 0)) + "una frase bastante larga que definitivamente pasa de los ochenta caracteres de vista previa ok")},
			IsForwarded:   proto.Bool(true), ForwardingScore: proto.Uint32(2)}}}
	x := inboundExtras(m)
	if x["quoted_id"] != "Q" || x["quoted_sender"] != "a@s.whatsapp.net" || x["is_forwarded"] != true || x["forwarding_score"] != uint32(2) {
		t.Fatalf("extras %v", x)
	}
	if r := []rune(x["quoted_text"].(string)); len(r) != quotePreviewMax+1 {
		t.Fatalf("preview not truncated: %d", len(r))
	}
	a := inboundExtras(&waE2E.Message{AudioMessage: &waE2E.AudioMessage{PTT: proto.Bool(true), Seconds: proto.Uint32(7)}})
	if a["ptt"] != true || a["duration"] != uint32(7) {
		t.Fatalf("voice extras %v", a)
	}
	if v, ok := a["transcript"]; !ok || v != nil {
		t.Fatal("transcript:null missing")
	}
	if len(inboundExtras(&waE2E.Message{Conversation: proto.String("x")})) != 0 {
		t.Fatal("plain text got extras")
	}
}

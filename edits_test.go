package main

import (
	"testing"
	"time"

	waCommon "go.mau.fi/whatsmeow/proto/waCommon"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func evt(id string, raw *waE2E.Message) *events.Message {
	e := &events.Message{Info: types.MessageInfo{ID: types.MessageID(id), Type: "text"}, RawMessage: raw}
	return e.UnwrapRaw() // same unwrapping whatsmeow applies before dispatch
}

func protoMsg(t waE2E.ProtocolMessage_Type, orig string, edited *waE2E.Message) *waE2E.Message {
	return &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Type: t.Enum(),
		Key: &waCommon.MessageKey{ID: proto.String(orig)}, EditedMessage: edited}}
}

func TestClassifyEdit(t *testing.T) {
	for name, edited := range map[string]*waE2E.Message{
		"conversation": {Conversation: proto.String("new")},
		"extended":     {ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String("new")}},
		"caption":      {ImageMessage: &waE2E.ImageMessage{Caption: proto.String("new")}},
	} {
		// As sent by WhatsApp: EditedMessage{ProtocolMessage{MESSAGE_EDIT, key, new content}}.
		raw := &waE2E.Message{EditedMessage: &waE2E.FutureProofMessage{Message: protoMsg(waE2E.ProtocolMessage_MESSAGE_EDIT, "ORIG", edited)}}
		v := evt("EDIT1", raw)
		if !v.IsEdit {
			t.Fatalf("%s: whatsmeow no longer sets IsEdit", name)
		}
		pl := classifyProtocol(v)
		if pl.Skip || pl.Type != "edit" || pl.Fields["edited_id"] != "ORIG" || pl.Fields["text"] != "new" || pl.SeenID != "EDIT1" {
			t.Fatalf("%s: %+v", name, pl)
		}
	}
	// Unwrapped MESSAGE_EDIT protocol message is classified the same.
	if pl := classifyProtocol(evt("E2", protoMsg(waE2E.ProtocolMessage_MESSAGE_EDIT, "O", &waE2E.Message{Conversation: proto.String("x")}))); pl.Type != "edit" {
		t.Fatalf("bare edit: %+v", pl)
	}
}

func TestClassifyEditNeverEmpty(t *testing.T) {
	empty := &waE2E.Message{EditedMessage: &waE2E.FutureProofMessage{Message: protoMsg(waE2E.ProtocolMessage_MESSAGE_EDIT, "ORIG", &waE2E.Message{})}}
	if pl := classifyProtocol(evt("E", empty)); !pl.Skip || pl.Type != "" {
		t.Fatalf("empty edit must be skipped: %+v", pl)
	}
	noKey := &waE2E.Message{EditedMessage: &waE2E.FutureProofMessage{Message: protoMsg(waE2E.ProtocolMessage_MESSAGE_EDIT, "", &waE2E.Message{Conversation: proto.String("x")})}}
	if pl := classifyProtocol(evt("E", noKey)); !pl.Skip {
		t.Fatalf("edit without original id must be skipped: %+v", pl)
	}
	noProto := &waE2E.Message{EditedMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{Conversation: proto.String("x")}}}
	if pl := classifyProtocol(evt("E", noProto)); !pl.Skip {
		t.Fatalf("edit without protocol message must be skipped: %+v", pl)
	}
}

func TestClassifyRevokeAndHousekeeping(t *testing.T) {
	pl := classifyProtocol(evt("DEL1", protoMsg(waE2E.ProtocolMessage_REVOKE, "ORIG", nil)))
	if pl.Skip || pl.Type != "delete" || pl.Fields["deleted_id"] != "ORIG" || pl.Fields["text"] != nil {
		t.Fatalf("revoke: %+v", pl)
	}
	if pl := classifyProtocol(evt("D", protoMsg(waE2E.ProtocolMessage_REVOKE, "", nil))); !pl.Skip {
		t.Fatalf("revoke without id: %+v", pl)
	}
	ks := &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_APP_STATE_SYNC_KEY_SHARE.Enum()}}
	if pl := classifyProtocol(evt("K", ks)); !pl.Skip || pl.Reason != "protocol APP_STATE_SYNC_KEY_SHARE" {
		t.Fatalf("housekeeping: %+v", pl)
	}
}

func TestClassifyOrdinaryUntouched(t *testing.T) {
	for _, m := range []*waE2E.Message{
		{Conversation: proto.String("hola")},
		{ImageMessage: &waE2E.ImageMessage{Caption: proto.String("c")}},
	} {
		if pl := classifyProtocol(evt("M1", m)); pl.Skip || pl.Type != "" || pl.SeenID != "M1" {
			t.Fatalf("ordinary message reclassified: %+v", pl)
		}
	}
}

func TestClassifyNewsletterEdit(t *testing.T) {
	v := evt("N1", &waE2E.Message{Conversation: proto.String("new")})
	v.NewsletterMeta = &events.NewsletterMessageMeta{EditTS: time.UnixMilli(1700000000123)}
	pl := classifyProtocol(v)
	if pl.Type != "edit" || pl.Fields["edited_id"] != "N1" || pl.Fields["text"] != "new" {
		t.Fatalf("newsletter edit: %+v", pl)
	}
	if pl.SeenID == "N1" {
		t.Fatal("newsletter edit reuses the original id: it must not be deduped against the original")
	}
	v.NewsletterMeta = &events.NewsletterMessageMeta{} // plain newsletter post
	if pl := classifyProtocol(v); pl.Type != "" || pl.SeenID != "N1" {
		t.Fatalf("newsletter post: %+v", pl)
	}
}

func TestEditAndDeleteLinesAreInbound(t *testing.T) {
	if isUpdateLine("edit") || isUpdateLine("delete") {
		t.Fatal("edit/delete must be claimable inbound lines, not silent updates")
	}
}

func TestKeyOnlyGroupPartNotMarked(t *testing.T) {
	skdm := &waE2E.SenderKeyDistributionMessage{GroupID: proto.String("g"), AxolotlSenderKeyDistributionMessage: []byte{1}}
	key := &events.Message{Info: types.MessageInfo{ID: "X"}, Message: &waE2E.Message{SenderKeyDistributionMessage: skdm, MessageContextInfo: &waE2E.MessageContextInfo{}}}
	key.RawMessage = key.Message
	pl := classifyProtocol(key)
	if !pl.Skip || !pl.NoMark {
		t.Fatalf("key-only part: want Skip+NoMark, got %+v", pl)
	}
	both := &events.Message{Info: types.MessageInfo{ID: "X"}, Message: &waE2E.Message{SenderKeyDistributionMessage: skdm, Conversation: proto.String("hola")}}
	both.RawMessage = both.Message
	if pl := classifyProtocol(both); pl.Skip {
		t.Fatalf("key+content: must not skip, got %+v", pl)
	}
}

package main

// Protocol messages on the inbound path. whatsmeow dispatches every ProtocolMessage as an
// events.Message (Info.Type "text", no Conversation): edits (UnwrapRaw unwraps EditedMessage and
// sets IsEdit; the inner message is ProtocolMessage{Type: MESSAGE_EDIT, Key: original, EditedMessage:
// new content}), deletes for everyone (REVOKE, Key: original) and housekeeping (app-state key
// shares, history-sync notices, ephemeral settings, ...). Read as plain text they became empty
// "text" inbox lines (and webhook notices). Newsletter edits are not wrapped: the message is the
// new content, Info.ID is the original id and NewsletterMeta.EditTS is set.

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types/events"
)

// protoLine says how an inbound event is written to the inbox.
//
//	Type == "" && !Skip: an ordinary message (text/media path).
//	Type "edit"/"delete"/"reaction": write a line of that type with Fields merged in.
//	Skip: write nothing (Reason is a short log tag, never message content).
//
// SeenID is the seen.log id (differs from Info.ID only for newsletter edits, which reuse the original id).
type protoLine struct {
	Type   string
	Fields map[string]any
	Skip   bool
	Reason string
	SeenID string
	NoMark bool // Skip without writing seen.log (a later part with the same id carries the content)
}

// keyOnly reports whether a message carries nothing but a sender key (plus context info).
func keyOnly(m *waE2E.Message) bool {
	if m == nil || m.GetSenderKeyDistributionMessage() == nil {
		return false
	}
	c := proto.Clone(m).(*waE2E.Message)
	c.SenderKeyDistributionMessage, c.MessageContextInfo = nil, nil
	return proto.Size(c) == 0
}

// textOf is the user-visible text of a message: conversation, extended text or media caption.
func textOf(m *waE2E.Message) string {
	if m == nil {
		return ""
	}
	if t := m.GetConversation(); t != "" {
		return t
	}
	if t := m.GetExtendedTextMessage().GetText(); t != "" {
		return t
	}
	_, _, caption, _ := mediaOf(m)
	return caption
}

func classifyProtocol(v *events.Message) protoLine {
	pl := protoLine{SeenID: string(v.Info.ID)}
	m := v.Message
	if keyOnly(v.RawMessage) {
		// A sender's first group message after a key change arrives as two parts with the same id:
		// a pkmsg carrying only the SenderKeyDistributionMessage, then the skmsg with the content.
		// whatsmeow dispatches both. Skip the key part WITHOUT marking it seen, or the real content
		// (same chat+id) is dropped as a duplicate.
		pl.Skip, pl.NoMark, pl.Reason = true, true, "sender key distribution only"
		return pl
	}
	if nm := v.NewsletterMeta; nm != nil && !nm.EditTS.IsZero() {
		pl.SeenID = fmt.Sprintf("%s#edit@%d", v.Info.ID, nm.EditTS.UnixMilli())
		return editLine(pl, string(v.Info.ID), textOf(m))
	}
	if rm := m.GetReactionMessage(); rm != nil {
		return reactionLine(pl, rm)
	}
	pm := m.GetProtocolMessage()
	if pm == nil {
		if v.IsEdit { // EditedMessage that didn't carry a ProtocolMessage: no original id to point at
			pl.Skip, pl.Reason = true, "edit without original key"
		}
		return pl
	}
	switch pm.GetType() {
	case waE2E.ProtocolMessage_MESSAGE_EDIT:
		return editLine(pl, pm.GetKey().GetID(), textOf(pm.GetEditedMessage()))
	case waE2E.ProtocolMessage_REVOKE:
		id := pm.GetKey().GetID()
		if id == "" {
			pl.Skip, pl.Reason = true, "delete without original id"
			return pl
		}
		pl.Type, pl.Fields = "delete", map[string]any{"deleted_id": id}
		return pl
	}
	pl.Skip, pl.Reason = true, "protocol "+pm.GetType().String()
	return pl
}

func editLine(pl protoLine, orig, text string) protoLine {
	switch {
	case orig == "":
		pl.Skip, pl.Reason = true, "edit without original id"
	case text == "":
		pl.Skip, pl.Reason = true, "edit without text"
	default:
		pl.Type, pl.Fields = "edit", map[string]any{"edited_id": orig, "text": text}
	}
	return pl
}

// reactionLine: a reaction (Info.Type "reaction", ReactionMessage{Key: reacted message, Text: emoji})
// used to become an empty "text" line that woke the routine. It is now
// {"type":"reaction","reacted_id","reacted_from_me","emoji"} (no "text"); an empty emoji is
// WhatsApp's "reaction removed" and carries "removed": true. Reaction lines are recorded only:
// isUpdateLine counts them as annotations of an earlier message (not claimable, not pending, no
// webhook notice), so a reaction alone never starts a run.
func reactionLine(pl protoLine, rm *waE2E.ReactionMessage) protoLine {
	k := rm.GetKey()
	if k.GetID() == "" {
		pl.Skip, pl.Reason = true, "reaction without target id"
		return pl
	}
	f := map[string]any{"reacted_id": k.GetID(), "reacted_from_me": k.GetFromMe(), "emoji": rm.GetText()}
	if p := k.GetParticipant(); p != "" {
		f["reacted_sender"] = p
	}
	if rm.GetText() == "" {
		f["removed"] = true
	}
	pl.Type, pl.Fields = "reaction", f
	return pl
}

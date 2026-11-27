package main

// Outbound message composition: plain text, quoted replies (--reply-to), forwards
// (forward --from-chat --message-id) and voice notes (--voice). Mirrors Baileys'
// generateWAMessageContent as used by OpenClaw/Hermes: a reply carries ContextInfo
// {StanzaID, Participant, QuotedMessage[, RemoteJID]} on the content sub-message; a forward
// re-sends the original content with ContextInfo{IsForwarded, ForwardingScore}.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

// Stored is what the bridge remembers about a past message: its inbox line and, for media,
// the raw proto saved next to the media file.
type Stored struct {
	Chat, Sender, Text string
	FromMe             bool
	Proto              *waE2E.Message
}

// Lookup finds a stored message by chat (may be "" = any chat) and id.
type Lookup func(chat, id string) (*Stored, error)

// inboxLookup scans inbox.jsonl (last match wins) and media/<id>.msg.pb.
func inboxLookup(inboxPath, mediaDir string) Lookup {
	return func(chat, id string) (*Stored, error) {
		var st *Stored
		if f, err := os.Open(inboxPath); err == nil {
			sc := bufio.NewScanner(f)
			sc.Buffer(make([]byte, 1<<20), 1<<24)
			for sc.Scan() {
				var m struct {
					ID, Chat, Sender, Text, Type string
					FromMe                       bool `json:"from_me"`
				}
				if json.Unmarshal(sc.Bytes(), &m) != nil || m.ID != id || isUpdateLine(m.Type) || m.Type == "poll_vote" || m.Type == "reaction" {
					continue
				}
				if chat != "" && normJID(m.Chat) != normJID(chat) {
					continue
				}
				st = &Stored{Chat: m.Chat, Sender: m.Sender, Text: m.Text, FromMe: m.FromMe}
			}
			f.Close()
		}
		if raw, err := os.ReadFile(filepath.Join(mediaDir, filepath.Base(id)+".msg.pb")); err == nil {
			pm := &waE2E.Message{}
			if proto.Unmarshal(raw, pm) == nil {
				if st == nil {
					st = &Stored{Chat: chat}
				}
				st.Proto = pm
			}
		}
		if st == nil {
			return nil, fmt.Errorf("message %s not found in inbox/media", id)
		}
		return st, nil
	}
}

// contentOf returns the stored message as a proto (media proto if saved, else text).
func (s *Stored) content() *waE2E.Message {
	if s.Proto != nil {
		return proto.Clone(s.Proto).(*waE2E.Message)
	}
	return &waE2E.Message{Conversation: proto.String(s.Text)}
}

// contextOf returns the ContextInfo of the content sub-message, if any.
func contextOf(m *waE2E.Message) *waE2E.ContextInfo {
	switch {
	case m.GetExtendedTextMessage() != nil:
		return m.GetExtendedTextMessage().GetContextInfo()
	case m.GetImageMessage() != nil:
		return m.GetImageMessage().GetContextInfo()
	case m.GetVideoMessage() != nil:
		return m.GetVideoMessage().GetContextInfo()
	case m.GetPtvMessage() != nil:
		return m.GetPtvMessage().GetContextInfo()
	case m.GetAudioMessage() != nil:
		return m.GetAudioMessage().GetContextInfo()
	case m.GetDocumentMessage() != nil:
		return m.GetDocumentMessage().GetContextInfo()
	case m.GetDocumentWithCaptionMessage().GetMessage().GetDocumentMessage() != nil:
		return m.GetDocumentWithCaptionMessage().GetMessage().GetDocumentMessage().GetContextInfo()
	case m.GetStickerMessage() != nil:
		return m.GetStickerMessage().GetContextInfo()
	case m.GetLocationMessage() != nil:
		return m.GetLocationMessage().GetContextInfo()
	case m.GetContactMessage() != nil:
		return m.GetContactMessage().GetContextInfo()
	}
	return nil
}

// setContext attaches ci to the content sub-message. A bare Conversation is upgraded to
// ExtendedTextMessage (Conversation cannot carry ContextInfo).
func setContext(m *waE2E.Message, ci *waE2E.ContextInfo) error {
	switch {
	case m.Conversation != nil:
		m.ExtendedTextMessage = &waE2E.ExtendedTextMessage{Text: m.Conversation}
		m.Conversation = nil
		m.ExtendedTextMessage.ContextInfo = ci
	case m.GetExtendedTextMessage() != nil:
		m.ExtendedTextMessage.ContextInfo = ci
	case m.GetImageMessage() != nil:
		m.ImageMessage.ContextInfo = ci
	case m.GetVideoMessage() != nil:
		m.VideoMessage.ContextInfo = ci
	case m.GetPtvMessage() != nil:
		m.PtvMessage.ContextInfo = ci
	case m.GetAudioMessage() != nil:
		m.AudioMessage.ContextInfo = ci
	case m.GetDocumentMessage() != nil:
		m.DocumentMessage.ContextInfo = ci
	case m.GetDocumentWithCaptionMessage().GetMessage().GetDocumentMessage() != nil:
		m.DocumentWithCaptionMessage.Message.DocumentMessage.ContextInfo = ci
	case m.GetStickerMessage() != nil:
		m.StickerMessage.ContextInfo = ci
	case m.GetLocationMessage() != nil:
		m.LocationMessage.ContextInfo = ci
	case m.GetContactMessage() != nil:
		m.ContactMessage.ContextInfo = ci
	default:
		return fmt.Errorf("message type cannot carry reply/forward context")
	}
	return nil
}

// quotedContext builds the reply ContextInfo. participant: explicit --reply-sender, else the
// stored sender, else (our own message or a 1:1 chat) own/chat JID like Baileys does.
func quotedContext(o OutMsg, look Lookup, ownJID string) *waE2E.ContextInfo {
	ci := &waE2E.ContextInfo{StanzaID: proto.String(o.ReplyTo)}
	var st *Stored
	if look != nil {
		st, _ = look(firstNonEmpty(o.ReplyChat, o.To), o.ReplyTo)
	}
	part := o.ReplySender
	if part == "" && st != nil {
		part = st.Sender
		if st.FromMe && part == "" {
			part = ownJID
		}
	}
	if part == "" && o.ReplyChat == "" {
		part = o.To // 1:1 chat, quoting the other side
	}
	if part != "" {
		ci.Participant = proto.String(normJID(part))
	}
	if st != nil {
		q := st.content()
		if c := contextOf(q); c != nil { // never nest quotes
			setContext(q, nil)
		}
		ci.QuotedMessage = q
	} else {
		ci.QuotedMessage = &waE2E.Message{Conversation: proto.String("")}
	}
	if o.ReplyChat != "" && normJID(o.ReplyChat) != normJID(o.To) {
		ci.RemoteJID = proto.String(normJID(o.ReplyChat))
	}
	return ci
}

func firstNonEmpty(a ...string) string {
	for _, s := range a {
		if s != "" {
			return s
		}
	}
	return ""
}

// forwardOf rebuilds the stored message as a forward (score = original+1, at least 1).
func forwardOf(o OutMsg, look Lookup) (*waE2E.Message, error) {
	if look == nil {
		return nil, fmt.Errorf("no message store")
	}
	st, err := look(o.FwdChat, o.FwdID)
	if err != nil {
		return nil, err
	}
	m := st.content()
	if m.Conversation != nil && m.GetConversation() == "" {
		return nil, fmt.Errorf("message %s has no text or stored media to forward", o.FwdID)
	}
	score := uint32(1)
	if c := contextOf(m); c != nil {
		score = c.GetForwardingScore() + 1
	}
	return m, setContext(m, &waE2E.ContextInfo{IsForwarded: proto.Bool(true), ForwardingScore: proto.Uint32(score)})
}

// buildMessage composes the outgoing proto for an outbox item (voice/image/document are built
// by the caller, which owns the upload, and passed in as media).
func buildMessage(o OutMsg, look Lookup, ownJID string, media *waE2E.Message) (*waE2E.Message, error) {
	var m *waE2E.Message
	var err error
	switch {
	case o.FwdID != "":
		if m, err = forwardOf(o, look); err != nil {
			return nil, err
		}
	case media != nil:
		m = media
	default:
		m = &waE2E.Message{Conversation: proto.String(o.Text)}
	}
	if o.ReplyTo != "" && o.FwdID == "" {
		if err := setContext(m, quotedContext(o, look, ownJID)); err != nil {
			return nil, err
		}
	}
	return m, nil
}

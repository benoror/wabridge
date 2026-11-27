package main

import (
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
)

const quotePreviewMax = 80

// quotedText gives a short preview of a quoted message (text or media caption/type).
func quotedText(q *waE2E.Message) string {
	if q == nil {
		return ""
	}
	t := q.GetConversation()
	if t == "" {
		t = q.GetExtendedTextMessage().GetText()
	}
	if t == "" {
		_, _, caption, _ := mediaOf(q)
		t = caption
	}
	if t == "" {
		switch {
		case q.GetAudioMessage() != nil:
			t = "[voice]"
		case q.GetImageMessage() != nil:
			t = "[image]"
		case q.GetVideoMessage() != nil:
			t = "[video]"
		case q.GetStickerMessage() != nil:
			t = "[sticker]"
		case q.GetDocumentMessage() != nil:
			t = "[document]"
		}
	}
	if r := []rune(t); len(r) > quotePreviewMax {
		t = string(r[:quotePreviewMax]) + "…"
	}
	return t
}

// inboundExtras returns reply/forward/voice fields for an inbox line.
func inboundExtras(m *waE2E.Message) map[string]any {
	out := map[string]any{}
	if ci := contextOf(m); ci != nil {
		if id := ci.GetStanzaID(); id != "" {
			out["quoted_id"] = id
			if p := ci.GetParticipant(); p != "" {
				out["quoted_sender"] = normJID(p)
			}
			if r := ci.GetRemoteJID(); r != "" {
				out["quoted_chat"] = normJID(r)
			}
			out["quoted_text"] = quotedText(ci.GetQuotedMessage())
		}
		if ci.GetIsForwarded() {
			out["is_forwarded"] = true
			out["forwarding_score"] = ci.GetForwardingScore()
		}
	}
	if a := m.GetAudioMessage(); a != nil {
		out["ptt"] = a.GetPTT()
		out["duration"] = a.GetSeconds()
		out["transcript"] = nil // filled by a later transcript_update line when transcription is on
	}
	return out
}

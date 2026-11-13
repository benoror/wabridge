package main

import (
	"encoding/json"

	"go.mau.fi/whatsmeow/types"
)

// ClaimGroup is one (chat, sender) slice of a lease (OpenClaw message-debounce: split a
// batch when the sender changes), so a routine can answer each person separately.
// AckMsgID is deterministic per lease+chat+sender: use it with `send --msg-id` and a retry
// can never post the same ack twice.
type ClaimGroup struct {
	Chat       string   `json:"chat"`
	Sender     string   `json:"sender"`
	PushName   string   `json:"push_name,omitempty"`
	MessageIDs []string `json:"message_ids"`
	AckMsgID   string   `json:"ack_msg_id"`
	Delayed    bool     `json:"delayed,omitempty"` // informational: some message here was delivered late (offline sync / after a reconnect); still new, handle it normally
	LastID     string   `json:"last_id"`           // newest message (natural --reply-to target)
}

func normJID(s string) string {
	j, err := types.ParseJID(s)
	if err != nil {
		return s
	}
	return j.ToNonAD().String()
}

// groupMessages groups inbound lines (own sends and media_update skipped) in first-seen order.
func groupMessages(leaseID string, msgs []json.RawMessage) []ClaimGroup {
	out := []ClaimGroup{}
	idx := map[string]int{}
	for _, raw := range msgs {
		var m struct {
			ID       string `json:"id"`
			Chat     string `json:"chat"`
			Sender   string `json:"sender"`
			PushName string `json:"push_name"`
			FromMe   bool   `json:"from_me"`
			Type     string `json:"type"`
			Delayed  bool   `json:"delayed"`
			Offline  bool   `json:"offline"`
			Replay   bool   `json:"replay"` // legacy name of delayed (lines written before 2026-10-17)
		}
		if json.Unmarshal(raw, &m) != nil || m.FromMe || isUpdateLine(m.Type) || m.Type == "poll_vote" || m.Type == "reaction" || m.ID == "" {
			continue
		}
		chat, sender := normJID(m.Chat), normJID(m.Sender)
		k := chat + "|" + sender
		i, ok := idx[k]
		if !ok {
			i = len(out)
			idx[k] = i
			out = append(out, ClaimGroup{Chat: chat, Sender: sender, AckMsgID: leaseMsgID(leaseID + "|" + k)})
		}
		g := &out[i]
		g.MessageIDs = append(g.MessageIDs, m.ID)
		g.LastID = m.ID
		g.Delayed = g.Delayed || m.Delayed || m.Offline || m.Replay
		if m.PushName != "" {
			g.PushName = m.PushName
		}
	}
	return out
}

package main

// MsgPipe is the inbound message path of `run` (events.Message -> classify -> dedupe -> inbox line
// -> notify), pulled out of main's event handler so the replay suite can drive it with recorded
// events, a fake clock and a fake webhook. main wires the real client, seen.log, settle and
// notifier; behaviour is unchanged.

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type MsgPipe struct {
	Ctx                          context.Context
	Seen                         *SeenSet
	Gate                         *ReplayGate
	Write                        func(any)
	Log                          func(f string, a ...any)
	DL                           Downloader // media download (nil: media messages get a download error)
	MediaDir                     string
	RetryReceipt                 func(info *types.MessageInfo, mediaKey []byte) error // expired media: ask the phone to re-upload
	STT                          *Transcriber
	Settle                       func(time.Duration)                                     // arm the settle check (Settler.Trigger)
	Notify                       func(chat, ts string)                                   // live inbound notice (Notifier.Add)
	IsPoll                       func(id string) bool                                    // our message marked as a poll (polls.txt); nil: none
	DecryptReaction              func(v *events.Message) (*waE2E.ReactionMessage, error) // encrypted reactions; nil: left undecrypted
	Now                          func() time.Time                                        // default time.Now
	ConnectSettle, OfflineSettle time.Duration
}

func (p *MsgPipe) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func (p *MsgPipe) retryReceipt(info *types.MessageInfo, key []byte) error {
	if p.RetryReceipt == nil {
		return fmt.Errorf("no retry receipt sender")
	}
	return p.RetryReceipt(info, key)
}

// Connected: (re)connect or process start. Arms the settle check (notify if anything is pending).
func (p *MsgPipe) Connected() {
	p.Gate.Connected(p.now())
	p.Settle(p.ConnectSettle)
}

// OfflineSync brackets offline delivery (OfflineSyncPreview / OfflineSyncCompleted).
func (p *MsgPipe) OfflineSync(active bool) {
	p.Gate.OfflineSync(active)
	if !active {
		p.Settle(p.OfflineSettle)
	}
}

// settleCheck is the settle Settler's Fn: notice for unhandled, not yet announced lines.
func settleCheck(ib *Inbox, notif *Notifier, logf func(string, ...any)) {
	pi, err := ib.Pending()
	if err != nil {
		logf("webhook: pending check failed: %v", err)
		return
	}
	logf("webhook: settle check: %d pending (%d unhandled, already announced %d)", pi.Count, pi.Unhandled, pi.Unhandled-pi.Count)
	notif.Kick(pi, "connect/offline sync")
}

// leaseCheck is the periodic expired-lease check: a run that claimed and died before done does
// not coincide with a reconnect, so without this its re-offered lines would wait for the next
// message or scheduled run. Quiet unless it kicks. Only unannounced expired leases trigger it
// (live inbound lines keep their own debounced/transcript-gated notice path).
func leaseCheck(ib *Inbox, notif *Notifier, logf func(string, ...any)) {
	if notif == nil || notif.URL == "" || notif.gaveUp() {
		return
	}
	pi, err := ib.Pending()
	if err != nil || pi.Leases == 0 {
		return
	}
	logf("webhook: %d line(s) in expired lease(s) (run died before done)", pi.Leases)
	notif.Kick(pi, "expired lease")
}

// watchLeases runs leaseCheck every interval until ctx ends.
func watchLeases(ctx context.Context, every time.Duration, check func()) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			check()
		}
	}
}

// Handle processes one events.Message (the body of main's former inline case).
func (p *MsgPipe) Handle(v *events.Message) {
	if v.Message.GetEncReactionMessage() != nil && p.DecryptReaction != nil {
		if rm, err := p.DecryptReaction(v); err != nil {
			p.Log("inbox: encrypted reaction not decrypted (id %s): %v", v.Info.ID, err)
		} else if rm != nil {
			v.Message = &waE2E.Message{ReactionMessage: rm} // handled like a plain reaction below
		}
	}
	pl := classifyProtocol(v) // edits, deletes, housekeeping protocol messages
	if p.Seen.Has(v.Info.Chat, pl.SeenID) {
		return // true duplicate (reconnect/retry/offline sync): this chat+id is already in the inbox
	}
	if pl.Skip { // never an empty line: nothing a bot can act on
		p.Log("inbox: skipped %s (id %s)", pl.Reason, v.Info.ID)
		if !pl.NoMark {
			p.Seen.Mark(v.Info.Chat, pl.SeenID)
		}
		return
	}
	m := v.Message
	ts := v.Info.Timestamp.UTC().Format(time.RFC3339)
	line := map[string]any{
		"id": v.Info.ID, "chat": v.Info.Chat.String(), "sender": v.Info.Sender.String(),
		"push_name": v.Info.PushName, "from_me": v.Info.IsFromMe, "is_group": v.Info.IsGroup,
		"ts": ts, "type": v.Info.Type, "text": "",
	}
	if pl.Type != "" { // edit / delete / reaction: refers to an earlier message (edited_id / deleted_id / reacted_id)
		line["type"] = pl.Type
		for k, x := range pl.Fields {
			line[k] = x
		}
		if pl.Type == "reaction" {
			delete(line, "text") // emoji is in "emoji"; never an empty text line
			if rid, _ := pl.Fields["reacted_id"].(string); !v.Info.IsFromMe && pl.Fields["reacted_from_me"] == true &&
				p.IsPoll != nil && p.IsPoll(rid) {
				pl.Type = "poll_vote" // someone reacted to a marked poll: a handleable line that wakes the routine
				line["type"] = pl.Type
			}
		}
	} else {
		txt := m.GetConversation() // media captions stay in "caption" (unchanged line shape)
		if txt == "" {
			txt = m.GetExtendedTextMessage().GetText()
		}
		line["text"] = txt
	}
	// Download before the line is written so consumers always find the file.
	var mf map[string]any
	var d whatsmeow.DownloadableMessage
	var err error
	if pl.Type == "" {
		mf, d, err = saveMedia(p.Ctx, p.DL, p.MediaDir, v.Info.ID, m)
	}
	for k, x := range mf {
		line[k] = x
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "media download failed:", v.Info.ID, err)
		if isExpired(err) {
			info := v.Info
			if e := p.retryReceipt(&info, d.GetMediaKey()); e != nil {
				fmt.Fprintln(os.Stderr, "media retry request failed:", v.Info.ID, e)
			}
		}
	}
	if pl.Type == "" {
		for k, x := range inboundExtras(m) {
			line[k] = x
		}
	}
	if m.GetAudioMessage().GetPTT() {
		line["type"] = "voice"
	}
	// A "text" stanza with no text and no media is something unsupported (contact, pin, event,
	// keep-in-chat, ...). Name it instead of shipping an empty text line; log field names only.
	if pl.Type == "" && mf == nil && line["type"] == "text" && line["text"] == "" {
		if t, extra := emptyTextKind(m); t != "" {
			line["type"] = t
			for k, x := range extra {
				line[k] = x
			}
			p.Log("inbox: empty text line classified %s %v (id %s)", t, extra, v.Info.ID)
		}
	}
	if mf == nil && v.Info.Type == "media" && pl.Type == "" {
		t, extra := describe(m)
		line["type"] = t
		for k, x := range extra {
			line[k] = x
		}
	}
	// delayed/offline are informational only: a late-delivered message is still new and
	// unhandled, so it is claimable and notified like any other (only seen.log duplicates
	// and already-claimed offsets are ever skipped).
	delayed, offline := p.Gate.Classify(v.Info.Timestamp, p.now())
	if delayed {
		line["delayed"] = true
	}
	if offline {
		line["offline"] = true
	}
	p.Write(line)
	p.Seen.Mark(v.Info.Chat, pl.SeenID)
	notify := func() {
		switch {
		case v.Info.IsFromMe, isUpdateLine(pl.Type): // media/transcript updates are recorded, never announced on their own
		case offline:
			p.Settle(p.OfflineSettle) // batch: one notice once the offline sync settles
		default:
			p.Notify(v.Info.Chat.String(), ts)
		}
	}
	if mp, _ := line["media_path"].(string); p.STT != nil && mp != "" && m.GetAudioMessage() != nil &&
		p.STT.Submit(sttJob{ID: string(v.Info.ID), Chat: v.Info.Chat.String(), Path: mp, Done: notify}) {
		return // webhook fires once the transcript line is written
	}
	notify()
}

// emptyTextKind names what an empty-text, no-media "text" message really is. Known kinds reuse
// describe(); anything else is "unsupported" with the first populated top-level field as
// "subtype" (a field name, never content). A message with no populated field at all (a bare
// sender-key/secret carrier) returns "" and keeps its old shape.
func emptyTextKind(m *waE2E.Message) (string, map[string]any) {
	if m == nil {
		return "", nil
	}
	t, extra := describe(m)
	if t == "media" {
		if extra == nil {
			return "", nil
		}
		return "unsupported", extra
	}
	return t, extra
}

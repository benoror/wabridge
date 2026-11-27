package main

// History-sync replay. After a re-link (fresh session after
// logged_out) WhatsApp never re-delivers the messages sent while the profile was logged out as
// live messages; the phone ships them inside HISTORY_SYNC blobs, which whatsmeow downloads and
// dispatches as events.HistorySync. Until now those were dropped, so that window never reached
// inbox.jsonl.
//
// HistoryReplayer writes messages from such a blob into inbox.jsonl, flagged
// "replay": true, "source": "history_sync", only when ALL hold:
//   - the chat is listed in <profile>/history_chats.txt (one chat JID per line, # comments; a
//     PN/LID alias of a listed JID also matches). No file or an empty file = replay off (fail
//     closed); the file is re-read on every history event, so editing it needs no restart.
//   - the message is newer than Since: the newest inbound (not from_me, not replay) ts already in
//     inbox.jsonl when the run started, floored at now-HistoryMaxBack (an empty inbox uses the floor).
//   - its id is not already in inbox.jsonl (and not in seen.log; see MsgPipe).
// Lines go through the normal inbound path (MsgPipe.HandleReplay): same shape, media download,
// transcription; never a media retry receipt, never an ack or any other send, one batched notice.
// Logs carry counts only (no chat ids, no text).

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	waWeb "go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

const HistoryChatsFile = "history_chats.txt"
const HistoryMaxBack = 14 * 24 * time.Hour

// loadHistoryChats reads <dir>/history_chats.txt into a set of normalized chat JIDs.
func loadHistoryChats(dir string) map[string]bool {
	f, err := os.Open(filepath.Join(dir, HistoryChatsFile))
	if err != nil {
		return nil
	}
	defer f.Close()
	out := map[string]bool{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		l := strings.TrimSpace(sc.Text())
		if i := strings.Index(l, "#"); i >= 0 {
			l = strings.TrimSpace(l[:i])
		}
		if l != "" {
			out[normJID(l)] = true
		}
	}
	return out
}

// inboxScan returns every message id in inbox.jsonl and the newest inbound (not from_me, not
// replay) timestamp. Missing file: empty set, zero time.
func inboxScan(path string) (map[string]bool, time.Time) {
	ids := map[string]bool{}
	var last time.Time
	f, err := os.Open(path)
	if err != nil {
		return ids, last
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for sc.Scan() {
		var m struct {
			ID     string `json:"id"`
			TS     string `json:"ts"`
			FromMe bool   `json:"from_me"`
			Replay bool   `json:"replay"`
			Type   string `json:"type"`
		}
		if json.Unmarshal(sc.Bytes(), &m) != nil {
			continue
		}
		if m.ID != "" {
			ids[m.ID] = true
		}
		if m.FromMe || m.Replay || isUpdateLine(m.Type) {
			continue
		}
		if t, err := time.Parse(time.RFC3339, m.TS); err == nil && t.After(last) {
			last = t
		}
	}
	return ids, last
}

type HistoryReplayer struct {
	Dir       string // profile dir (history_chats.txt)
	InboxPath string
	Since     time.Time // set by Init
	Pipe      *MsgPipe
	Parse     func(chat types.JID, m *waWeb.WebMessageInfo) (*events.Message, error) // cl.ParseWebMessage
	Alt       func(types.JID) types.JID                                              // PN<->LID alias (zero JID if unknown); nil: none
	Log       func(f string, a ...any)
	Now       func() time.Time
}

// Init fixes the watermark from inbox.jsonl as it is before this run writes anything.
func (h *HistoryReplayer) Init() {
	now := time.Now()
	if h.Now != nil {
		now = h.Now()
	}
	_, last := inboxScan(h.InboxPath)
	floor := now.Add(-HistoryMaxBack)
	if last.Before(floor) {
		last = floor
	}
	h.Since = last
}

func (h *HistoryReplayer) allowed(allow map[string]bool, chat types.JID) bool {
	if allow[chat.ToNonAD().String()] {
		return true
	}
	if h.Alt != nil {
		if a := h.Alt(chat.ToNonAD()); !a.IsEmpty() && allow[a.ToNonAD().String()] {
			return true
		}
	}
	return false
}

// Handle processes one events.HistorySync; returns the number of replay lines written.
func (h *HistoryReplayer) Handle(v *events.HistorySync) int {
	if v == nil || v.Data == nil || len(v.Data.GetConversations()) == 0 {
		return 0
	}
	convs := v.Data.GetConversations()
	allow := loadHistoryChats(h.Dir)
	if len(allow) == 0 {
		h.Log("history sync: %s, %d conversation(s) ignored (no %s: replay off)", v.Data.GetSyncType(), len(convs), HistoryChatsFile)
		return 0
	}
	known, _ := inboxScan(h.InboxPath)
	var evs []*events.Message
	nAllowed, old, dup, bad := 0, 0, 0, 0
	for _, c := range convs {
		chat, err := types.ParseJID(c.GetID())
		if err != nil || !h.allowed(allow, chat) {
			continue
		}
		nAllowed++
		for _, hm := range c.GetMessages() {
			wm := hm.GetMessage()
			if wm == nil {
				continue
			}
			if !time.Unix(int64(wm.GetMessageTimestamp()), 0).After(h.Since) {
				old++
				continue
			}
			if known[wm.GetKey().GetID()] {
				dup++
				continue
			}
			e, err := h.Parse(chat, wm)
			if err != nil || e == nil || e.Message == nil {
				bad++
				continue
			}
			evs = append(evs, e)
		}
	}
	sort.SliceStable(evs, func(i, j int) bool { return evs[i].Info.Timestamp.Before(evs[j].Info.Timestamp) })
	wrote := 0
	for _, e := range evs {
		if known[e.Info.ID] {
			continue // same id twice in one blob
		}
		known[e.Info.ID] = true
		if h.Pipe.HandleReplay(e) {
			wrote++
		}
	}
	h.Log("history sync: %s, %d conversation(s), %d allowed; wrote %d replay line(s); skipped %d older than watermark, %d already in inbox, %d unparseable",
		v.Data.GetSyncType(), len(convs), nAllowed, wrote, old, dup, bad)
	return wrote
}

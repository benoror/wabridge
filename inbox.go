package main

// Inbox claiming for bots: exactly-once handoff of inbound lines to concurrent routine runs.
//
// State under <profile>/claims/ (all mutations under an exclusive flock on claims/state.lock):
//   state.json        {"offset": <byte offset in inbox.jsonl already claimed>}
//   leases/<id>.json  {"lease_id","msg_id","start","end","expires","messages"}
//   run.json          single-run lock {"token","owner","expires"}
// A lease returned by `release` or expired (TTL) is re-offered whole by the next `claim`.

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Lease struct {
	ID       string            `json:"lease_id"`
	MsgID    string            `json:"msg_id"` // deterministic WhatsApp id for this lease's ack (use with send --msg-id)
	Start    int64             `json:"start"`
	End      int64             `json:"end"`
	Expires  time.Time         `json:"expires"`
	Released bool              `json:"released,omitempty"`
	Messages []json.RawMessage `json:"messages"`
}

type claimState struct {
	Offset int64 `json:"offset"`
}

type RunLock struct {
	Token   string    `json:"token"`
	Owner   string    `json:"owner,omitempty"`
	Expires time.Time `json:"expires"`
}

type Inbox struct {
	Dir string // profile dir
	Now func() time.Time
}

func (ib *Inbox) now() time.Time {
	if ib.Now != nil {
		return ib.Now()
	}
	return time.Now()
}

func (ib *Inbox) cdir() string {
	d := filepath.Join(ib.Dir, "claims")
	os.MkdirAll(filepath.Join(d, "leases"), 0o700)
	return d
}

// withLock runs fn under an exclusive (blocking) flock on claims/state.lock.
func (ib *Inbox) withLock(fn func() error) error {
	f, err := os.OpenFile(filepath.Join(ib.cdir(), "state.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return fn()
}

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// leaseMsgID derives a WhatsApp-style id (3EB0 + 16 upper hex) from the lease id.
func leaseMsgID(lease string) string {
	h := sha256.Sum256([]byte("wabridge-ack:" + lease))
	return "3EB0" + strings.ToUpper(hex.EncodeToString(h[:8]))
}

// chunkMsgID derives chunk n's stable WhatsApp id from a base id (Hermes #68713: a split reply
// that fails partway must not resend earlier chunks). Same base+n => same id => deduped.
func chunkMsgID(base string, n int) string {
	return leaseMsgID(fmt.Sprintf("chunk:%s:%d", base, n))
}

func readJSON(p string, v any) error {
	b, err := os.ReadFile(p)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// lastHandledEnd returns the byte offset just past the last complete inbox line whose id is the
// one in <profile>/last_handled ({"id":..,"ts":..}, written by bots that track a cursor instead
// of claim/done), or -1 if there is no such file or line.
func (ib *Inbox) lastHandledEnd(inbox string) int64 {
	var lh struct {
		ID string `json:"id"`
	}
	if readJSON(filepath.Join(ib.Dir, "last_handled"), &lh) != nil || lh.ID == "" {
		return -1
	}
	f, err := os.Open(inbox)
	if err != nil {
		return -1
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var off, found int64 = 0, -1
	for {
		line, err := r.ReadBytes('\n')
		if len(line) == 0 || line[len(line)-1] != '\n' {
			break // EOF or partial trailing line
		}
		off += int64(len(line))
		var m struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(line, &m) == nil && m.ID == lh.ID {
			found = off
		}
		if err != nil {
			break
		}
	}
	return found
}

// completeSize is the byte offset after the last complete ('\n'-terminated) line.
func completeSize(inbox string) int64 {
	b, err := os.ReadFile(inbox)
	if err != nil {
		return 0
	}
	return int64(bytes.LastIndexByte(b, '\n') + 1)
}

// initOffset: first claim ever starts after the legacy cursor (last_handled id) if found, else at EOF,
// so history is never replayed.
func (ib *Inbox) initOffset(inbox string) int64 {
	if lh := ib.lastHandledEnd(inbox); lh >= 0 {
		return lh
	}
	return completeSize(inbox)
}

// handledOffset is where unhandled lines start: the claimed offset, moved forward past the
// last_handled line when a bot advanced that cursor without claiming (a bot that handles the inbox
// directly and writes last_handled; without this the claim offset stays where its last claim left
// it and every reconnect re-reported the same already-answered messages as pending).
func (ib *Inbox) handledOffset(inbox string, st claimState) int64 {
	if lh := ib.lastHandledEnd(inbox); lh > st.Offset {
		return lh
	}
	return st.Offset
}

// Claim atomically takes every complete inbox line after the claimed offset (or a released/expired
// lease) and returns a lease. Returns nil when there is nothing inbound to handle.
func (ib *Inbox) Claim(ttl time.Duration) (*Lease, error) {
	var out *Lease
	err := ib.withLock(func() error {
		cd := ib.cdir()
		now := ib.now()
		// 1. Re-offer a released or expired lease (oldest first).
		names, _ := filepath.Glob(filepath.Join(cd, "leases", "*.json"))
		var stale []*Lease
		for _, p := range names {
			var l Lease
			if readJSON(p, &l) == nil && (l.Released || now.After(l.Expires)) {
				l2 := l
				stale = append(stale, &l2)
			}
		}
		if len(stale) > 0 {
			sort.Slice(stale, func(i, j int) bool { return stale[i].Start < stale[j].Start })
			old := stale[0]
			os.Remove(filepath.Join(cd, "leases", old.ID+".json"))
			nl := &Lease{ID: randHex(8), Start: old.Start, End: old.End, Messages: old.Messages, Expires: now.Add(ttl)}
			nl.MsgID = leaseMsgID(nl.ID)
			if err := atomicWrite(filepath.Join(cd, "leases", nl.ID+".json"), nl); err != nil {
				return err
			}
			out = nl
			return nil
		}
		// 2. New lines.
		inbox := filepath.Join(ib.Dir, "inbox.jsonl")
		sp := filepath.Join(cd, "state.json")
		var st claimState
		if readJSON(sp, &st) != nil {
			st.Offset = ib.initOffset(inbox)
		}
		st.Offset = ib.handledOffset(inbox, st)
		f, err := os.Open(inbox)
		if err != nil {
			if os.IsNotExist(err) {
				return atomicWrite(sp, st)
			}
			return err
		}
		defer f.Close()
		if fi, _ := f.Stat(); fi != nil && fi.Size() < st.Offset {
			st.Offset = fi.Size() // file was truncated/rotated
		}
		f.Seek(st.Offset, io.SeekStart)
		r := bufio.NewReader(f)
		end := st.Offset
		var msgs []json.RawMessage
		inbound := 0
		for {
			line, err := r.ReadBytes('\n')
			if len(line) == 0 || line[len(line)-1] != '\n' {
				break // EOF or partial line still being written
			}
			end += int64(len(line))
			var m struct {
				FromMe bool   `json:"from_me"`
				Type   string `json:"type"`
			}
			if json.Unmarshal(line, &m) != nil {
				continue
			}
			msgs = append(msgs, json.RawMessage(append([]byte(nil), line[:len(line)-1]...)))
			if !m.FromMe && !isUpdateLine(m.Type) {
				inbound++
			}
			if err != nil {
				break
			}
		}
		start := st.Offset
		st.Offset = end
		if inbound == 0 { // only own sends / updates: advance quietly, nothing to handle
			return atomicWrite(sp, st)
		}
		nl := &Lease{ID: randHex(8), Start: start, End: end, Messages: msgs, Expires: now.Add(ttl)}
		nl.MsgID = leaseMsgID(nl.ID)
		if err := atomicWrite(filepath.Join(cd, "leases", nl.ID+".json"), nl); err != nil {
			return err
		}
		if err := atomicWrite(sp, st); err != nil {
			os.Remove(filepath.Join(cd, "leases", nl.ID+".json"))
			return err
		}
		out = nl
		return nil
	})
	return out, err
}

// Done marks a lease handled (deletes it). Release hands it back for the next claim.
func (ib *Inbox) Done(id string) error    { return ib.finish(id, false) }
func (ib *Inbox) Release(id string) error { return ib.finish(id, true) }

func (ib *Inbox) finish(id string, release bool) error {
	if id == "" || strings.ContainsAny(id, "/.") {
		return fmt.Errorf("bad lease id")
	}
	return ib.withLock(func() error {
		p := filepath.Join(ib.cdir(), "leases", id+".json")
		var l Lease
		if err := readJSON(p, &l); err != nil {
			return fmt.Errorf("lease %s not found (expired and re-claimed, or already done)", id)
		}
		if !release {
			return os.Remove(p)
		}
		l.Released = true
		return atomicWrite(p, l)
	})
}

// Lock takes the single-run lock for ttl. Returns ("", nil) if another live holder has it.
func (ib *Inbox) Lock(owner string, ttl time.Duration) (string, error) {
	tok := ""
	err := ib.withLock(func() error {
		p := filepath.Join(ib.cdir(), "run.json")
		var cur RunLock
		if readJSON(p, &cur) == nil && ib.now().Before(cur.Expires) {
			return nil
		}
		tok = randHex(8)
		return atomicWrite(p, RunLock{Token: tok, Owner: owner, Expires: ib.now().Add(ttl)})
	})
	return tok, err
}

func (ib *Inbox) Unlock(token string) error {
	return ib.withLock(func() error {
		p := filepath.Join(ib.cdir(), "run.json")
		var cur RunLock
		if readJSON(p, &cur) != nil {
			return nil
		}
		if cur.Token != token {
			return fmt.Errorf("lock held by another token")
		}
		return os.Remove(p)
	})
}

// PendingInfo summarizes inbound lines that are unhandled and not yet announced: lines a
// `claim` would hand out right now (after the claimed offset and after last_handled, plus
// released/expired leases) that have not been announced by a webhook notice yet.
//   - tail lines: announced once a notice covered their position (the notified watermark,
//     claims/notified.json "offset", advanced after every successful notice);
//   - released leases: like tail lines (a deliberate hand-back; announced by position);
//   - expired leases (a run claimed and died before done): announced once PER LEASE ID, so a
//     re-claimed range that dies again is announced again, at most maxExpiryNotices times per
//     range (a message that kills every run must not wake the routine forever).
//
// Lines under a live lease are not pending (a run is handling them). Delayed/offline-sync lines
// count like any other inbound line. Unhandled is the count ignoring announcements (for logs);
// Leases is the part of Count that comes from unannounced expired leases.
type PendingInfo struct {
	Count     int
	Latest    string
	Chats     []string
	Unhandled int
	Leases    int
}

// maxExpiryNotices caps expiry announcements per lease range (same start offset).
const maxExpiryNotices = 3

type notifiedState struct {
	Offset   int64            `json:"offset"`
	Leases   map[string]int64 `json:"leases,omitempty"`   // announced expired lease id -> its start offset
	Expiries map[string]int   `json:"expiries,omitempty"` // start offset -> expiry announcements so far
}

// NoticeMark is what a notice built now covers: the inbox position and the expired leases.
type NoticeMark struct {
	Offset int64
	Leases map[string]int64 // expired (not released) lease id -> start
}

func (ib *Inbox) notifiedPath() string { return filepath.Join(ib.cdir(), "notified.json") }

func expiredLease(l Lease, now time.Time) bool { return !l.Released && now.After(l.Expires) }

func (ns *notifiedState) leaseAnnounced(l Lease) bool {
	if _, ok := ns.Leases[l.ID]; ok {
		return true
	}
	return ns.Expiries[strconv.FormatInt(l.Start, 10)] >= maxExpiryNotices
}

// NotifyMark snapshots what a notice built now covers (end of the last complete line + the
// leases that are expired right now).
func (ib *Inbox) NotifyMark() NoticeMark {
	mk := NoticeMark{Offset: completeSize(filepath.Join(ib.Dir, "inbox.jsonl")), Leases: map[string]int64{}}
	ib.withLock(func() error {
		now := ib.now()
		names, _ := filepath.Glob(filepath.Join(ib.cdir(), "leases", "*.json"))
		for _, p := range names {
			var l Lease
			if readJSON(p, &l) == nil && expiredLease(l, now) {
				mk.Leases[l.ID] = l.Start
			}
		}
		return nil
	})
	return mk
}

// MarkNotified persists a successful notice: the watermark (never moves backwards) and the
// expired leases it announced. Entries for leases/ranges that no longer exist are pruned.
func (ib *Inbox) MarkNotified(mk NoticeMark) error {
	return ib.withLock(func() error {
		var ns notifiedState
		readJSON(ib.notifiedPath(), &ns)
		if mk.Offset > ns.Offset {
			ns.Offset = mk.Offset
		}
		if ns.Leases == nil {
			ns.Leases = map[string]int64{}
		}
		if ns.Expiries == nil {
			ns.Expiries = map[string]int{}
		}
		for id, start := range mk.Leases {
			if _, ok := ns.Leases[id]; !ok {
				ns.Leases[id] = start
				ns.Expiries[strconv.FormatInt(start, 10)]++
			}
		}
		live, starts := map[string]bool{}, map[string]bool{}
		names, _ := filepath.Glob(filepath.Join(ib.cdir(), "leases", "*.json"))
		for _, p := range names {
			var l Lease
			if readJSON(p, &l) == nil {
				live[l.ID], starts[strconv.FormatInt(l.Start, 10)] = true, true
			}
		}
		for id := range ns.Leases {
			if !live[id] {
				delete(ns.Leases, id)
			}
		}
		for st := range ns.Expiries {
			if !starts[st] {
				delete(ns.Expiries, st) // range done (or file gone): forget its count
			}
		}
		return atomicWrite(ib.notifiedPath(), ns)
	})
}

func (ib *Inbox) Pending() (PendingInfo, error) {
	var pi PendingInfo
	chats := map[string]bool{}
	var wm int64
	add := func(line []byte, announced, lease bool) {
		var m struct {
			Chat   string `json:"chat"`
			TS     string `json:"ts"`
			FromMe bool   `json:"from_me"`
			Type   string `json:"type"`
		}
		if json.Unmarshal(line, &m) != nil || m.FromMe || isUpdateLine(m.Type) {
			return
		}
		pi.Unhandled++
		if announced {
			return
		}
		pi.Count++
		if lease {
			pi.Leases++
		}
		chats[m.Chat] = true
		if m.TS > pi.Latest {
			pi.Latest = m.TS
		}
	}
	err := ib.withLock(func() error {
		cd := ib.cdir()
		now := ib.now()
		inbox := filepath.Join(ib.Dir, "inbox.jsonl")
		var ns notifiedState
		readJSON(ib.notifiedPath(), &ns)
		wm = ns.Offset
		if fi, err := os.Stat(inbox); err == nil && fi.Size() < wm {
			wm = 0 // inbox truncated/rotated: watermark is meaningless
		}
		names, _ := filepath.Glob(filepath.Join(cd, "leases", "*.json"))
		for _, p := range names {
			var l Lease
			if readJSON(p, &l) != nil {
				continue
			}
			switch {
			case l.Released:
				for _, m := range l.Messages {
					add(m, l.End <= wm, false)
				}
			case expiredLease(l, now):
				ann := ns.leaseAnnounced(l)
				for _, m := range l.Messages {
					add(m, ann, !ann)
				}
			}
		}
		var st claimState
		if readJSON(filepath.Join(cd, "state.json"), &st) != nil {
			st.Offset = ib.initOffset(inbox)
		}
		st.Offset = ib.handledOffset(inbox, st)
		f, err := os.Open(inbox)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		defer f.Close()
		if fi, _ := f.Stat(); fi != nil && fi.Size() < st.Offset {
			st.Offset = fi.Size()
		}
		f.Seek(st.Offset, io.SeekStart)
		r := bufio.NewReader(f)
		off := st.Offset
		for {
			line, err := r.ReadBytes('\n')
			if len(line) == 0 || line[len(line)-1] != '\n' {
				break
			}
			off += int64(len(line))
			add(line, off <= wm, false)
			if err != nil {
				break
			}
		}
		return nil
	})
	for c := range chats {
		pi.Chats = append(pi.Chats, c)
	}
	sort.Strings(pi.Chats)
	return pi, err
}

package main

// Inbound dedupe (OpenClaw inbound/dedupe.ts + durable-receive.ts): whatsmeow can deliver
// the same message twice (reconnect, retry receipts, offline sync). Each copy would become a
// new inbox line, claim and ack. SeenSet is a bounded set keyed by normalized chat + message
// id, persisted per profile (<profile>/seen.log) so a restart doesn't replay either.

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/types"
)

type SeenSet struct {
	Path string
	Max  int           // entries kept (default 5000)
	TTL  time.Duration // entries older than this are forgotten (default 24h)
	Now  func() time.Time

	mu    sync.Mutex
	seen  map[string]int64
	order []string
	f     *os.File
	lines int
}

// dedupeKey normalizes the chat JID (device suffix and agent dropped) so the same message
// seen via different device addressing maps to one key.
func dedupeKey(chat types.JID, id string) string {
	return chat.ToNonAD().String() + "|" + id
}

func (s *SeenSet) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Load reads the persisted log (if any) and opens it for appending.
func (s *SeenSet) Load() error {
	if s.Max <= 0 {
		s.Max = 5000
	}
	if s.TTL <= 0 {
		s.TTL = 24 * time.Hour
	}
	s.seen = map[string]int64{}
	cutoff := s.now().Add(-s.TTL).Unix()
	if f, err := os.Open(s.Path); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			ts, key, ok := strings.Cut(sc.Text(), " ")
			n, err := strconv.ParseInt(ts, 10, 64)
			if !ok || err != nil || n < cutoff {
				continue
			}
			s.remember(key, n)
		}
		f.Close()
	}
	return s.rewrite()
}

func (s *SeenSet) remember(key string, ts int64) {
	if _, ok := s.seen[key]; !ok {
		s.order = append(s.order, key)
	}
	s.seen[key] = ts
	for len(s.order) > s.Max {
		delete(s.seen, s.order[0])
		s.order = s.order[1:]
	}
}

// rewrite compacts the log to the live entries.
func (s *SeenSet) rewrite() error {
	if s.f != nil {
		s.f.Close()
	}
	tmp := s.Path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, k := range s.order {
		fmt.Fprintf(w, "%d %s\n", s.seen[k], k)
	}
	w.Flush()
	f.Close()
	if err := os.Rename(tmp, s.Path); err != nil {
		return err
	}
	s.lines = len(s.order)
	s.f, err = os.OpenFile(s.Path, os.O_APPEND|os.O_WRONLY, 0o600)
	return err
}

// Seen records key and reports whether it was already present (true = duplicate, drop it).
func (s *SeenSet) Seen(chat types.JID, id string) bool {
	if s.Has(chat, id) {
		return true
	}
	s.Mark(chat, id)
	return false
}

// Has reports whether chat+id was already written to the inbox (a true duplicate delivery).
func (s *SeenSet) Has(chat types.JID, id string) bool {
	key := dedupeKey(chat, id)
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, ok := s.seen[key]
	return ok && s.now().Unix()-ts <= int64(s.TTL/time.Second)
}

// Mark records chat+id once its inbox line is written. Marking after the write (not before)
// means a crash between delivery and write can't make the redelivered copy look like a duplicate.
func (s *SeenSet) Mark(chat types.JID, id string) {
	key := dedupeKey(chat, id)
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().Unix()
	s.remember(key, now)
	if s.f != nil {
		fmt.Fprintf(s.f, "%d %s\n", now, key)
		s.f.Sync()
		if s.lines++; s.lines > 2*s.Max {
			s.rewrite()
		}
	}
}

func (s *SeenSet) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f != nil {
		s.f.Close()
		s.f = nil
	}
}

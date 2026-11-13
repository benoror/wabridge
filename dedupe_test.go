package main

import (
	"path/filepath"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
)

func TestSeenSetDedupesAndNormalizes(t *testing.T) {
	s := &SeenSet{Path: filepath.Join(t.TempDir(), "seen.log")}
	if err := s.Load(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	chat := types.NewJID("5218100000000", types.DefaultUserServer)
	if s.Seen(chat, "A") {
		t.Fatal("first copy reported as dup")
	}
	ad := chat
	ad.Device = 7
	if !s.Seen(ad, "A") {
		t.Fatal("device-suffixed chat not normalized")
	}
	if s.Seen(chat, "B") {
		t.Fatal("different id deduped")
	}
	other := types.NewJID("123-456", types.GroupServer)
	if s.Seen(other, "A") {
		t.Fatal("same id in other chat deduped")
	}
}

func TestSeenSetPersistsAcrossRestart(t *testing.T) {
	p := filepath.Join(t.TempDir(), "seen.log")
	chat := types.NewJID("1", types.DefaultUserServer)
	s := &SeenSet{Path: p}
	s.Load()
	s.Seen(chat, "X")
	s.Close()
	s2 := &SeenSet{Path: p}
	s2.Load()
	defer s2.Close()
	if !s2.Seen(chat, "X") {
		t.Fatal("restart replayed a seen message")
	}
}

func TestSeenSetBoundedAndTTL(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	s := &SeenSet{Path: filepath.Join(t.TempDir(), "seen.log"), Max: 3, TTL: time.Hour, Now: func() time.Time { return now }}
	s.Load()
	defer s.Close()
	chat := types.NewJID("1", types.DefaultUserServer)
	for _, id := range []string{"1", "2", "3", "4", "5", "6", "7", "8"} {
		s.Seen(chat, id)
	}
	if len(s.seen) != 3 {
		t.Fatalf("set not bounded: %d", len(s.seen))
	}
	if s.Seen(chat, "1") {
		t.Fatal("evicted entry still deduped")
	}
	now = now.Add(2 * time.Hour)
	if s.Seen(chat, "8") {
		t.Fatal("expired entry still deduped")
	}
}

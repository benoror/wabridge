package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHistorySuggestedNeverEnables(t *testing.T) {
	dir := t.TempDir()
	inbox := `{"id":"a","chat":"120363000000000000@g.us","from_me":false,"is_group":true,"push_name":"P1","ts":"2026-01-02T00:00:00Z","type":"text","text":"x"}
{"id":"b","chat":"120363000000000000@g.us","from_me":false,"is_group":true,"push_name":"P2","ts":"2026-01-03T00:00:00Z","type":"text","text":"x"}
{"id":"c","chat":"111@lid","from_me":false,"is_group":false,"push_name":"Synthetic Person","ts":"2026-01-01T00:00:00Z","type":"text","text":"x"}
{"id":"d","chat":"120363000000000001@g.us","from_me":false,"is_group":true,"ts":"2026-01-01T00:00:00Z","type":"text","text":"x"}
{"id":"e","chat":"222@lid","from_me":true,"ts":"2026-01-01T00:00:00Z","type":"text","text":"x"}
{"id":"f","chat":"status@broadcast","from_me":false,"ts":"2026-01-01T00:00:00Z","type":"text","text":"x"}
`
	os.WriteFile(filepath.Join(dir, "inbox.jsonl"), []byte(inbox), 0o600)
	if w := historyMissingWarning(dir, "p"); !strings.Contains(w, "replay is OFF") {
		t.Fatalf("missing file must warn: %q", w)
	}
	n, err := writeHistorySuggested(dir, "p", map[string]string{"120363000000000000@g.us": "Team # Group",
		"120363000000000001@g.us": "Kitchen Display"}, time.Unix(0, 0))
	if err != nil || n != 2 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	b, _ := os.ReadFile(filepath.Join(dir, HistorySuggestedFile))
	s := string(b)
	for _, l := range strings.Split(strings.TrimSpace(s), "\n") {
		if !strings.HasPrefix(l, "#") {
			t.Fatalf("uncommented line in suggestions: %q", l)
		}
	}
	for _, want := range []string{"# 120363000000000000@g.us  # Team Group; 2 inbound", "# 111@lid  # Synthetic Person; 1 inbound",
		"# EXCLUDED (shared display): 120363000000000001@g.us"} {
		if !strings.Contains(s, want) {
			t.Fatalf("missing %q in:\n%s", want, s)
		}
	}
	if strings.Contains(s, "222@lid") || strings.Contains(s, "status@broadcast") || strings.Contains(s, "\"x\"") {
		t.Fatalf("from_me-only / broadcast chat or text leaked:\n%s", s)
	}
	if len(loadHistoryChats(dir)) != 0 {
		t.Fatal("suggestions must not enable replay")
	}
	os.WriteFile(filepath.Join(dir, HistoryChatsFile), []byte("111@lid\n"), 0o600)
	if w := historyMissingWarning(dir, "p"); w != "" {
		t.Fatalf("present file must not warn: %q", w)
	}
}

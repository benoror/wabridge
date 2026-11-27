package main

// Poll marks: message ids (our own sent messages) that the bot marked as polls. A reaction from
// someone else to a marked message is written as type "poll_vote" instead of "reaction", and a
// poll_vote is a normal handleable line: pending, claimable and announced to the webhook, so it
// wakes the routine (2026-10-20). Every other reaction stays recorded-only.
// The file is re-read on each check, so `wabridge poll` takes effect without a restart.

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func pollsPath(dir string) string { return filepath.Join(dir, "polls.txt") }

// pollMarked reports whether id is listed in dir/polls.txt (one id per line, # comments allowed).
func pollMarked(dir, id string) bool {
	if id == "" {
		return false
	}
	f, err := os.Open(pollsPath(dir))
	if err != nil {
		return false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) == id {
			return true
		}
	}
	return false
}

// markPoll appends id to dir/polls.txt (idempotent).
func markPoll(dir, id string) error {
	id = strings.TrimSpace(id)
	if id == "" || strings.ContainsAny(id, "\n\r#") {
		return fmt.Errorf("bad message id %q", id)
	}
	if pollMarked(dir, id) {
		return nil
	}
	f, err := os.OpenFile(pollsPath(dir), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintln(f, id)
	return err
}

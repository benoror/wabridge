package main

import (
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types/events"
)

func TestClassifyFailure(t *testing.T) {
	cases := []struct {
		name  string
		evt   any
		state string
		halt  bool
		exit  bool
		retry time.Duration
	}{
		{"outdated event", &events.ClientOutdated{}, "client_outdated", true, false, 0},
		{"outdated 405", &events.ConnectFailure{Reason: events.ConnectFailureClientOutdated}, "client_outdated", true, false, 0},
		{"ban with expiry", &events.TemporaryBan{Code: events.TempBanBlockedByUsers, Expire: time.Hour}, "temp_banned", true, false, time.Hour + tempBanMargin},
		{"ban without expiry", &events.TemporaryBan{Code: events.TempBanBroadcastList}, "temp_banned", true, false, tempBanUnknownRetry},
		{"ban 402", &events.ConnectFailure{Reason: events.ConnectFailureTempBanned}, "temp_banned", true, false, tempBanUnknownRetry},
		{"logged out 401", &events.ConnectFailure{Reason: events.ConnectFailureLoggedOut}, "logged_out", false, true, 0},
		{"main device gone 403", &events.ConnectFailure{Reason: events.ConnectFailureMainDeviceGone}, "logged_out", false, true, 0},
		{"unknown logout 406", &events.ConnectFailure{Reason: events.ConnectFailureUnknownLogout}, "logged_out", false, true, 0},
		{"generic 400", &events.ConnectFailure{Reason: events.ConnectFailureGeneric}, "connect_failure", false, false, 0},
		{"server 503", &events.ConnectFailure{Reason: events.ConnectFailureServiceUnavailable}, "connect_failure", false, false, 0},
	}
	for _, c := range cases {
		a, ok := classifyFailure(c.evt)
		if !ok {
			t.Errorf("%s: not classified", c.name)
			continue
		}
		if a.State != c.state || a.Halt != c.halt || a.Exit != c.exit || a.RetryAfter != c.retry {
			t.Errorf("%s: got %+v, want state=%s halt=%v exit=%v retry=%s", c.name, a, c.state, c.halt, c.exit, c.retry)
		}
		if a.Reason == "" {
			t.Errorf("%s: empty reason", c.name)
		}
		if a.Halt && a.Exit {
			t.Errorf("%s: halt and exit are exclusive", c.name)
		}
	}
	if _, ok := classifyFailure(&events.Connected{}); ok {
		t.Error("Connected classified as a failure")
	}
	if a, _ := classifyFailure(&events.TemporaryBan{Code: events.TempBanBlockedByUsers, Expire: 90 * time.Minute}); !strings.Contains(a.Reason, "1h30m0s") || !strings.Contains(a.Reason, "102") {
		t.Errorf("ban reason should carry the code and expiry: %q", a.Reason)
	}
}

// A halted watchdog never asks for a reconnect, and Resume lifts the halt.
func TestWatchdogHaltResume(t *testing.T) {
	w := &Watchdog{}
	w.Halt()
	if w.dueWith(time.Now(), false) != "" {
		t.Fatal("halted watchdog wants a reconnect")
	}
	w.Resume()
	if w.Halted() {
		t.Fatal("Resume did not lift the halt")
	}
}

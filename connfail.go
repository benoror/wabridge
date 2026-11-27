package main

import (
	"fmt"
	"time"

	"go.mau.fi/whatsmeow/types/events"
)

// Connect failures WhatsApp reports explicitly (issue #13). A restart can't fix any of them, so
// each gets a clear status.json state and no restart loop:
//
//	405 client outdated    -> state client_outdated: stop reconnecting, stay up idle until the
//	                          binary is upgraded and restarted.
//	402 temporary ban      -> state temp_banned: stop reconnecting; if WhatsApp says when the ban
//	                          ends, try once more after that, else stay idle until restarted.
//	401/403/406 logged out -> state logged_out: exit; supervise.sh parks until a re-pair.
//	anything else          -> state connect_failure: informational, normal reconnects continue.
//
// The ideas (and the codes) follow wacli's session-state handling.

// FailureAction is what `run` does after a server-reported connect failure.
type FailureAction struct {
	State      string        // status.json state
	Reason     string        // status.json reason (no message content, no keys)
	Halt       bool          // stop reconnecting: auto-reconnect off, watchdog halted, socket closed
	Exit       bool          // exit the process (logged out: the supervisor parks)
	RetryAfter time.Duration // with Halt: reconnect once after this long (0 = stay halted)
}

// tempBanMargin is added to WhatsApp's ban expiry before the single retry.
const tempBanMargin = 5 * time.Minute

// tempBanUnknownRetry is the retry delay when a 402 arrives without an expiry.
const tempBanUnknownRetry = 6 * time.Hour

// classifyFailure maps a whatsmeow event to a FailureAction; ok=false for any other event.
func classifyFailure(evt any) (FailureAction, bool) {
	switch v := evt.(type) {
	case *events.ClientOutdated:
		return outdatedAction(), true
	case *events.TemporaryBan:
		return banAction(v.Code.String(), v.Expire), true
	case *events.ConnectFailure:
		switch {
		case v.Reason.IsLoggedOut():
			return FailureAction{State: "logged_out", Reason: "connect failure " + v.Reason.String() + "; re-pair required", Exit: true}, true
		case v.Reason == events.ConnectFailureClientOutdated:
			return outdatedAction(), true
		case v.Reason == events.ConnectFailureTempBanned:
			return banAction("", 0), true
		}
		return FailureAction{State: "connect_failure", Reason: "connect failure " + v.Reason.String() + "; reconnecting"}, true
	}
	return FailureAction{}, false
}

func outdatedAction() FailureAction {
	return FailureAction{State: "client_outdated", Halt: true,
		Reason: "WhatsApp rejected this client version (405); upgrade wabridge, then restart it"}
}

func banAction(code string, expire time.Duration) FailureAction {
	a := FailureAction{State: "temp_banned", Halt: true}
	why := "temporary ban (402)"
	if code != "" {
		why += " " + code
	}
	if expire > 0 {
		a.RetryAfter = expire + tempBanMargin
		a.Reason = fmt.Sprintf("%s; expires in %s; one reconnect after %s", why, expire.Round(time.Second), a.RetryAfter.Round(time.Second))
	} else {
		a.RetryAfter = tempBanUnknownRetry
		a.Reason = fmt.Sprintf("%s; expiry unknown; one reconnect after %s", why, a.RetryAfter)
	}
	return a
}

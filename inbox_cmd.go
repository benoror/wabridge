package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"
)

// wabridge inbox lock|unlock|claim|done|release --profile P [...]
// Exit codes: 0 ok; 3 = lock busy (lock) ; 1 = error. `claim` with nothing to do prints {"count":0}.
func inboxCmd(args []string) {
	if len(args) < 1 {
		die("usage: wabridge inbox lock|unlock|claim|done|release --profile P [--lease ID] [--token T] [--ttl 10m]")
	}
	sub := args[0]
	fs := flag.NewFlagSet("inbox "+sub, flag.ExitOnError)
	profile := fs.String("profile", "", "profile name")
	lease := fs.String("lease", "", "lease id (done/release)")
	token := fs.String("token", "", "lock token (unlock)")
	owner := fs.String("owner", "", "lock owner label (lock)")
	ttl := fs.Duration("ttl", 10*time.Minute, "lock/lease TTL")
	fs.Parse(args[1:])
	if *profile == "" {
		die("--profile required")
	}
	ib := &Inbox{Dir: profileDir(*profile)}
	pr := func(v any) { b, _ := json.Marshal(v); fmt.Println(string(b)) }
	switch sub {
	case "lock":
		tok, err := ib.Lock(*owner, *ttl)
		if err != nil {
			die("lock: %v", err)
		}
		if tok == "" {
			pr(map[string]any{"locked": false})
			os.Exit(3)
		}
		pr(map[string]any{"locked": true, "token": tok})
	case "unlock":
		if err := ib.Unlock(*token); err != nil {
			die("unlock: %v", err)
		}
		pr(map[string]any{"unlocked": true})
	case "claim":
		l, err := ib.Claim(*ttl)
		if err != nil {
			die("claim: %v", err)
		}
		if l == nil {
			pr(map[string]any{"count": 0, "lease_id": ""})
			return
		}
		pr(map[string]any{"count": len(l.Messages), "lease_id": l.ID, "msg_id": l.MsgID,
			"expires": l.Expires.UTC().Format(time.RFC3339), "messages": l.Messages,
			"groups": groupMessages(l.ID, l.Messages)})
	case "done", "release":
		var err error
		if sub == "done" {
			err = ib.Done(*lease)
		} else {
			err = ib.Release(*lease)
		}
		if err != nil {
			die("%s: %v", sub, err)
		}
		pr(map[string]any{sub: true, "lease_id": *lease})
	default:
		die("unknown inbox command %s", sub)
	}
}

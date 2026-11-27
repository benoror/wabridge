package main

// Issue #7: a wake ping must never carry message content. Every message below hides a canary
// string in a place a careless notifier could copy (text, caption, edit, reaction, location,
// contact, push name). The messages go through the real inbound path, the settle check, the
// expired-lease check and a retried notice; every webhook body must stay free of the canaries
// and use only the allowed keys.

import (
	"encoding/json"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
)

const canary = "CANARYc0ntent"

var wakeAllowedKeys = map[string]bool{"profile": true, "count": true, "latest_ts": true, "chats": true, "inbox_path": true}

func TestWakePingNeverCarriesContent(t *testing.T) {
	hook := &fakeHook{}
	srv := httptest.NewServer(hook)
	defer srv.Close()
	clk := newHClock()
	r := &replayRig{t: t, dir: t.TempDir(), clk: clk, start: clk.Now(), hook: hook, url: srv.URL}
	r.boot()
	defer r.close()

	dm, grp := "15550000001@s.whatsapp.net", "120363000000000001@g.us"
	msgs := []replayEvent{
		{ID: "W1", Chat: dm, Sender: dm, PushName: canary + "-name",
			Message: json.RawMessage(`{"conversation":"` + canary + `-text"}`)},
		{ID: "W2", Chat: grp, Sender: dm, IsGroup: true, PushName: canary + "-name",
			Message: json.RawMessage(`{"extendedTextMessage":{"text":"` + canary + `-ext"}}`)},
		{ID: "W3", Chat: dm, Sender: dm,
			Message: json.RawMessage(`{"locationMessage":{"degreesLatitude":1.5,"degreesLongitude":2.5,"name":"` + canary + `-loc","address":"` + canary + `-addr"}}`)},
		{ID: "W4", Chat: dm, Sender: dm,
			Message: json.RawMessage(`{"contactMessage":{"displayName":"` + canary + `-contact","vcard":"BEGIN:VCARD\nFN:` + canary + `-vcard\nEND:VCARD"}}`)},
		{ID: "W5", Chat: dm, Sender: dm,
			Message: json.RawMessage(`{"reactionMessage":{"key":{"remoteJID":"` + dm + `","fromMe":true,"ID":"OUT1"},"text":"` + canary + `"}}`)},
		{ID: "W6", Chat: dm, Sender: dm, IsEdit: true,
			Message: json.RawMessage(`{"protocolMessage":{"type":"MESSAGE_EDIT","key":{"remoteJID":"` + dm + `","ID":"W1"},"editedMessage":{"conversation":"` + canary + `-edit"}}}`)},
	}
	r.step(0, replayStep{Op: "claim"}) // establish the claim offset before any message
	r.step(0, replayStep{Op: "connected"})
	for i := range msgs {
		r.step(1+i, replayStep{Op: "message", Ev: &msgs[i]})
	}
	r.step(10, replayStep{Op: "settle"})
	// An abandoned claim: the expired lease is announced again by the periodic check.
	r.step(11, replayStep{Op: "claim_abandon"})
	r.step(12, replayStep{Op: "advance", Seconds: 120})
	r.step(13, replayStep{Op: "connected"}) // reconnect: the settle check re-announces the expired lease
	r.step(14, replayStep{Op: "settle"})

	inbox := strings.Join(func() []string {
		var out []string
		for _, l := range r.lines() {
			b, _ := json.Marshal(l)
			out = append(out, string(b))
		}
		return out
	}(), "\n")
	if !strings.Contains(inbox, canary) {
		t.Fatal("test is vacuous: the canary never reached inbox.jsonl")
	}

	hook.mu.Lock()
	bodies := append([][]byte(nil), hook.bodies...)
	hook.mu.Unlock()
	if len(bodies) < 2 {
		t.Fatalf("want at least 2 wake pings (settle + expired lease), got %d", len(bodies))
	}
	for i, b := range bodies {
		if strings.Contains(strings.ToLower(string(b)), strings.ToLower(canary)) {
			t.Errorf("ping %d carries message content: %s", i, b)
		}
		var m map[string]json.RawMessage
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("ping %d is not JSON: %v", i, err)
		}
		var keys []string
		for k := range m {
			keys = append(keys, k)
			if !wakeAllowedKeys[k] {
				t.Errorf("ping %d has unexpected key %q", i, k)
			}
		}
		sort.Strings(keys)
		var chats []string
		if err := json.Unmarshal(m["chats"], &chats); err != nil {
			t.Errorf("ping %d chats: %v (keys %v)", i, err, keys)
		}
		for _, c := range chats {
			if !strings.HasSuffix(c, "@s.whatsapp.net") && !strings.HasSuffix(c, "@g.us") && !strings.HasSuffix(c, "@lid") {
				t.Errorf("ping %d chats entry %q is not a chat JID", i, c)
			}
		}
	}
}

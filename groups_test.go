package main

import (
	"encoding/json"
	"testing"
)

func TestGroupMessages(t *testing.T) {
	raw := []string{
		`{"id":"1","chat":"g@g.us","sender":"a:3@s.whatsapp.net","type":"text"}`,
		`{"id":"2","chat":"g@g.us","sender":"b@s.whatsapp.net","type":"text","delayed":true,"offline":true}`,
		`{"id":"3","chat":"g@g.us","sender":"a@s.whatsapp.net","type":"text"}`,
		`{"id":"4","chat":"g@g.us","sender":"me@s.whatsapp.net","from_me":true}`,
		`{"id":"1","chat":"g@g.us","type":"media_update"}`,
		`{"id":"5","chat":"d@s.whatsapp.net","sender":"d@s.whatsapp.net","type":"text"}`,
	}
	var msgs []json.RawMessage
	for _, r := range raw {
		msgs = append(msgs, json.RawMessage(r))
	}
	g := groupMessages("L1", msgs)
	if len(g) != 3 {
		t.Fatalf("want 3 groups, got %+v", g)
	}
	if g[0].Sender != "a@s.whatsapp.net" || len(g[0].MessageIDs) != 2 || g[0].LastID != "3" || g[0].Delayed {
		t.Fatalf("group a: %+v", g[0])
	}
	if !g[1].Delayed || len(g[1].MessageIDs) != 1 {
		t.Fatal("b: delayed message must still be grouped (and flagged delayed)")
	}
	if g[0].AckMsgID == g[1].AckMsgID || g[0].AckMsgID == g[2].AckMsgID {
		t.Fatal("ack ids collide")
	}
	if again := groupMessages("L1", msgs); again[0].AckMsgID != g[0].AckMsgID {
		t.Fatal("ack id not deterministic")
	}
	if other := groupMessages("L2", msgs); other[0].AckMsgID == g[0].AckMsgID {
		t.Fatal("ack id not lease-scoped")
	}
}

func TestChunkMsgID(t *testing.T) {
	a1, a2 := chunkMsgID("3EB0AAAA", 1), chunkMsgID("3EB0AAAA", 2)
	if a1 == a2 || a1 != chunkMsgID("3EB0AAAA", 1) || a1 == chunkMsgID("3EB0BBBB", 1) {
		t.Fatal("chunk ids not stable/distinct")
	}
	if len(a1) != 20 || a1[:4] != "3EB0" {
		t.Fatalf("not a WhatsApp-style id: %s", a1)
	}
}

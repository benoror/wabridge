package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow/types"
)

func TestBarePhone(t *testing.T) {
	for in, want := range map[string]string{
		"528112345678": "528112345678", "+528112345678": "528112345678", " +52 811 234-5678 ": "528112345678",
		"+1 (555) 123-4567": "15551234567",
	} {
		if d, ok := barePhone(in); !ok || d != want {
			t.Errorf("%q -> %q %v", in, d, ok)
		}
	}
	for _, in := range []string{"5218112345678@s.whatsapp.net", "123@g.us", "99@lid", "abc", "", "+", "12345", "52811x234567", "1234567890123456"} {
		if _, ok := barePhone(in); ok {
			t.Errorf("%q must not be a bare phone", in)
		}
	}
}

func TestResolver(t *testing.T) {
	calls := 0
	r := &Resolver{Lookup: func(_ context.Context, p string) (types.JID, bool, error) {
		calls++
		switch p {
		case "+528112345678": // MX canonical form gets the 1
			return types.NewJID("5218112345678", types.DefaultUserServer), true, nil
		case "+520000000000":
			return types.JID{}, false, nil
		}
		return types.JID{}, false, errors.New("boom")
	}}
	ctx := context.Background()
	for _, in := range []string{"528112345678", "+528112345678"} {
		j, err := r.Resolve(ctx, in)
		if err != nil || j.String() != "5218112345678@s.whatsapp.net" {
			t.Fatalf("%s: %v %v", in, j, err)
		}
	}
	if calls != 1 {
		t.Fatalf("lookups should be cached, got %d", calls)
	}
	if r.Cached("+528112345678").String() != "5218112345678@s.whatsapp.net" {
		t.Fatal("cached form for logging")
	}
	if _, err := r.Resolve(ctx, "520000000000"); err == nil || !strings.Contains(err.Error(), "number not on WhatsApp") {
		t.Fatalf("not on WA: %v", err)
	}
	if _, err := r.Resolve(ctx, "529999999999"); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("lookup error: %v", err)
	}
	for _, in := range []string{"120363000000000000@g.us", "12345678901234@lid", "5218112345678@s.whatsapp.net"} {
		j, err := r.Resolve(ctx, in)
		if err != nil || j.String() != in {
			t.Fatalf("full JID must pass through: %s -> %v %v", in, j, err)
		}
	}
	if calls != 3 {
		t.Fatalf("full JIDs must not trigger lookups (calls=%d)", calls)
	}
	if _, err := r.Resolve(ctx, "notajid"); err == nil {
		t.Fatal("garbage must fail, not become a server name")
	}
	if (&Resolver{}).Cached("528112345678").String() != "528112345678@s.whatsapp.net" {
		t.Fatal("uncached fallback")
	}
}

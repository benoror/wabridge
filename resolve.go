package main

// Recipient resolution. A bare phone number (digits, optional leading +, spaces/dashes ignored)
// is resolved to its canonical WhatsApp JID with an IsOnWhatsApp query (Mexican numbers may be
// 521XXXXXXXXXX or 52XXXXXXXXXX); full JIDs (...@s.whatsapp.net, @g.us, @lid, ...) pass through.

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"go.mau.fi/whatsmeow/types"
)

// barePhone returns the digits of s if s is a bare phone number.
func barePhone(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if strings.Contains(s, "@") {
		return "", false
	}
	s = strings.TrimPrefix(s, "+")
	d := strings.NewReplacer(" ", "", "-", "", "(", "", ")", "").Replace(s)
	if len(d) < 7 || len(d) > 15 {
		return "", false
	}
	for _, r := range d {
		if r < '0' || r > '9' {
			return "", false
		}
	}
	return d, true
}

// parseJID parses a full JID; it never treats a bare string as a server name.
func parseJID(s string) (types.JID, error) {
	s = strings.TrimSpace(s)
	if !strings.Contains(s, "@") {
		return types.JID{}, fmt.Errorf("recipient %q is neither a phone number nor a JID", s)
	}
	j, err := types.ParseJID(s)
	if err != nil || j.Server == "" {
		return types.JID{}, fmt.Errorf("bad JID %q", s)
	}
	return j, nil
}

// PhoneLookup asks WhatsApp for the canonical JID of "+<digits>".
type PhoneLookup func(ctx context.Context, plusDigits string) (jid types.JID, onWA bool, err error)

// Resolver maps recipients to JIDs, caching phone lookups for the process lifetime.
type Resolver struct {
	Lookup PhoneLookup
	mu     sync.Mutex
	cache  map[string]types.JID
}

func (r *Resolver) Resolve(ctx context.Context, to string) (types.JID, error) {
	d, ok := barePhone(to)
	if !ok {
		return parseJID(to)
	}
	r.mu.Lock()
	j, hit := r.cache[d]
	r.mu.Unlock()
	if hit {
		return j, nil
	}
	if r.Lookup == nil {
		return types.JID{}, fmt.Errorf("cannot resolve phone number %s: no connection", to)
	}
	j, onWA, err := r.Lookup(ctx, "+"+d)
	if err != nil {
		return types.JID{}, fmt.Errorf("resolve +%s: %w", d, err)
	}
	if !onWA || j.User == "" {
		return types.JID{}, fmt.Errorf("number not on WhatsApp: +%s", d)
	}
	j = j.ToNonAD()
	r.mu.Lock()
	if r.cache == nil {
		r.cache = map[string]types.JID{}
	}
	r.cache[d] = j
	r.mu.Unlock()
	return j, nil
}

// Cached returns the resolved JID for to without a network call (phone numbers only if already
// resolved); falls back to the s.whatsapp.net form of the digits for logging.
func (r *Resolver) Cached(to string) types.JID {
	if d, ok := barePhone(to); ok {
		r.mu.Lock()
		j, hit := r.cache[d]
		r.mu.Unlock()
		if hit {
			return j
		}
		return types.NewJID(d, types.DefaultUserServer)
	}
	j, _ := parseJID(to)
	return j
}

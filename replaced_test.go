package main

import (
	"testing"
	"time"
)

func TestNextReplacedDelay(t *testing.T) {
	d := time.Duration(0)
	var got []time.Duration
	for i := 0; i < 8; i++ {
		d = nextReplacedDelay(d)
		got = append(got, d)
	}
	if got[0] != time.Minute || got[1] != 2*time.Minute || got[5] != 30*time.Minute || got[7] != 30*time.Minute {
		t.Fatalf("backoff %v", got)
	}
}

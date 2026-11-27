package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestTranscribeConfig(t *testing.T) {
	d := t.TempDir()
	c, err := loadTranscribeConfig(d)
	if err != nil || c.Lang != "es" || c.Workers != 1 || c.Bin != defWhisperBin {
		t.Fatalf("defaults %+v %v", c, err)
	}
	os.WriteFile(filepath.Join(d, "transcribe.json"), []byte(`{"enabled":false,"lang":"auto","bin":"/x","model":"/y"}`), 0o600)
	c, _ = loadTranscribeConfig(d)
	if c.Lang != "auto" || c.Active() {
		t.Fatalf("override %+v", c)
	}
	bin, model := filepath.Join(d, "b"), filepath.Join(d, "m")
	os.WriteFile(bin, nil, 0o700)
	os.WriteFile(model, nil, 0o600)
	os.WriteFile(filepath.Join(d, "transcribe.json"), []byte(`{"bin":"`+bin+`","model":"`+model+`"}`), 0o600)
	if c, _ = loadTranscribeConfig(d); !c.Active() {
		t.Fatal("should be active when bin+model exist")
	}
	os.WriteFile(filepath.Join(d, "transcribe.json"), []byte(`{bad`), 0o600)
	if _, err := loadTranscribeConfig(d); err == nil {
		t.Fatal("bad json accepted")
	}
}

func TestCleanTranscriptAndArgs(t *testing.T) {
	if got := cleanTranscript("\n Hola, \n  qué tal.\n\n"); got != "Hola, qué tal." {
		t.Fatalf("%q", got)
	}
	a := whisperArgs(TranscribeConfig{Model: "m", Lang: "auto", Threads: 2}, "w.wav")
	if a[3] != "auto" || a[len(a)-1] != "w.wav" {
		t.Fatalf("%v", a)
	}
}

func TestTranscriberAsync(t *testing.T) {
	var mu sync.Mutex
	var lines []map[string]any
	write := func(v any) { mu.Lock(); lines = append(lines, v.(map[string]any)); mu.Unlock() }
	release := make(chan struct{})
	tr := &Transcriber{Cfg: TranscribeConfig{Lang: "es", TimeoutSec: 5}, Write: write, jobs: make(chan sttJob, 1)}
	tr.Run = func(ctx context.Context, c TranscribeConfig, p string) (string, error) {
		<-release
		if p == "bad" {
			return "", errors.New("boom")
		}
		return "hola", nil
	}
	go tr.worker()
	done := make(chan string, 2)
	start := time.Now()
	if !tr.Submit(sttJob{ID: "A", Chat: "c", Path: "ok", Done: func() { done <- "A" }}) {
		t.Fatal("submit")
	}
	// worker is busy with A; queue (cap 1) takes B; C overflows without blocking
	time.Sleep(20 * time.Millisecond)
	tr.Submit(sttJob{ID: "B", Chat: "c", Path: "bad", Done: func() { done <- "B" }})
	if tr.Submit(sttJob{ID: "C"}) {
		t.Fatal("full queue should refuse")
	}
	if time.Since(start) > time.Second {
		t.Fatal("submit blocked")
	}
	close(release)
	<-done
	<-done
	mu.Lock()
	defer mu.Unlock()
	if len(lines) != 2 || lines[0]["type"] != "transcript_update" || lines[0]["transcript"] != "hola" || lines[0]["id"] != "A" {
		t.Fatalf("%v", lines)
	}
	if lines[1]["transcript_error"] != "boom" || lines[1]["transcript"] != nil {
		t.Fatalf("%v", lines[1])
	}
}

func TestUpdateLinesNotInbound(t *testing.T) {
	if !isUpdateLine("transcript_update") || !isUpdateLine("media_update") || isUpdateLine("voice") {
		t.Fatal("isUpdateLine")
	}
}

// End to end against the real whisper install (skipped when absent): set WABRIDGE_STT_SAMPLE.
func TestTranscribeFileReal(t *testing.T) {
	s := os.Getenv("WABRIDGE_STT_SAMPLE")
	c, _ := loadTranscribeConfig(t.TempDir())
	if s == "" || !c.Active() {
		t.Skip("set WABRIDGE_STT_SAMPLE with whisper installed")
	}
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("no ffmpeg")
	}
	txt, err := TranscribeFile(context.Background(), c, s)
	if err != nil || txt == "" {
		t.Fatalf("%q %v", txt, err)
	}
	t.Logf("transcript: %s", txt)
}

package main

// Inbound voice-note transcription (whisper.cpp). Non-blocking: the voice line is written
// immediately with transcript:null; a worker transcribes the downloaded audio and appends
//   {"id","chat","type":"transcript_update","ts","transcript","lang"} (or "transcript_error")
// like media_update. The webhook notice for a voice note is held until its transcript line is
// written (or failed/timed out), so the routine's claim usually already contains the text.
//
// Config: <profile>/transcribe.json (all optional)
//   {"enabled":true,"bin":"/opt/whisper.cpp/build/bin/whisper-cli",
//    "model":"/opt/whisper.cpp/models/ggml-small.bin","lang":"es","threads":4,
//    "workers":1,"timeout_sec":120}
// Without the file, it's on when the default bin and model exist. lang "auto" = detect.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	defWhisperBin   = "/opt/whisper.cpp/build/bin/whisper-cli"
	defWhisperModel = "/opt/whisper.cpp/models/ggml-small.bin"
)

type TranscribeConfig struct {
	Enabled    *bool  `json:"enabled,omitempty"`
	Bin        string `json:"bin,omitempty"`
	Model      string `json:"model,omitempty"`
	Lang       string `json:"lang,omitempty"`
	Threads    int    `json:"threads,omitempty"`
	Workers    int    `json:"workers,omitempty"`
	TimeoutSec int    `json:"timeout_sec,omitempty"`
}

func loadTranscribeConfig(dir string) (TranscribeConfig, error) {
	var c TranscribeConfig
	if b, err := os.ReadFile(filepath.Join(dir, "transcribe.json")); err == nil {
		if err := json.Unmarshal(b, &c); err != nil {
			return c, fmt.Errorf("transcribe.json: %w", err)
		}
	}
	c.Bin = firstNonEmpty(c.Bin, defWhisperBin)
	c.Model = firstNonEmpty(c.Model, defWhisperModel)
	c.Lang = firstNonEmpty(c.Lang, "es")
	if c.Threads <= 0 {
		c.Threads = 4
	}
	if c.Workers <= 0 {
		c.Workers = 1
	}
	if c.TimeoutSec <= 0 {
		c.TimeoutSec = 120
	}
	return c, nil
}

// Active reports whether transcription should run (explicit flag, else bin+model present).
func (c TranscribeConfig) Active() bool {
	if c.Enabled != nil && !*c.Enabled {
		return false
	}
	_, e1 := os.Stat(c.Bin)
	_, e2 := os.Stat(c.Model)
	return e1 == nil && e2 == nil
}

// whisperArgs builds the whisper-cli command line for a 16k mono WAV.
func whisperArgs(c TranscribeConfig, wav string) []string {
	return []string{"-m", c.Model, "-l", c.Lang, "-t", strconv.Itoa(c.Threads), "-nt", "-np", "-f", wav}
}

// cleanTranscript joins whisper's stdout lines into one string.
func cleanTranscript(out string) string {
	var parts []string
	for _, l := range strings.Split(out, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			parts = append(parts, l)
		}
	}
	return strings.Join(parts, " ")
}

// TranscribeFile converts audio to 16k mono WAV with ffmpeg and runs whisper on it.
func TranscribeFile(ctx context.Context, c TranscribeConfig, audio string) (string, error) {
	tmp, err := os.MkdirTemp("", "wabridge-stt-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(tmp)
	wav := filepath.Join(tmp, "in.wav")
	if b, err := exec.CommandContext(ctx, "ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-i", audio,
		"-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le", wav).CombinedOutput(); err != nil {
		return "", fmt.Errorf("ffmpeg: %v: %s", err, firstLine(string(b)))
	}
	cmd := exec.CommandContext(ctx, c.Bin, whisperArgs(c, wav)...)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		if ctx.Err() != nil {
			return "", fmt.Errorf("whisper timed out")
		}
		return "", fmt.Errorf("whisper: %v", err)
	}
	return cleanTranscript(string(out)), nil
}

type sttJob struct {
	ID, Chat, Path string
	Done           func() // called after the update line is written (e.g. webhook notice)
}

// Transcriber runs jobs on a small worker pool; Submit never blocks the event handler.
type Transcriber struct {
	Cfg   TranscribeConfig
	Write func(any)
	Run   func(ctx context.Context, c TranscribeConfig, path string) (string, error) // default TranscribeFile
	jobs  chan sttJob
}

func NewTranscriber(c TranscribeConfig, write func(any)) *Transcriber {
	t := &Transcriber{Cfg: c, Write: write, Run: TranscribeFile, jobs: make(chan sttJob, 256)}
	for i := 0; i < c.Workers; i++ {
		go t.worker()
	}
	return t
}

// Submit queues a job; false if the queue is full (caller should not wait on Done then).
func (t *Transcriber) Submit(j sttJob) bool {
	select {
	case t.jobs <- j:
		return true
	default:
		return false
	}
}

func (t *Transcriber) worker() {
	for j := range t.jobs {
		t.handle(j)
	}
}

func (t *Transcriber) handle(j sttJob) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(t.Cfg.TimeoutSec)*time.Second)
	defer cancel()
	line := map[string]any{"id": j.ID, "chat": j.Chat, "type": "transcript_update",
		"ts": time.Now().UTC().Format(time.RFC3339), "lang": t.Cfg.Lang}
	txt, err := t.Run(ctx, t.Cfg, j.Path)
	if err != nil {
		line["transcript_error"] = err.Error()
		fmt.Fprintln(os.Stderr, "transcribe failed:", j.ID, err)
	} else {
		line["transcript"] = txt
	}
	t.Write(line)
	if j.Done != nil {
		j.Done()
	}
}

// isUpdateLine: lines that patch or annotate an earlier message rather than being a new one to
// handle (not claimable, not counted as pending/unhandled, never notified on their own).
// Reactions are NOT update lines since 2026-10-25 (they can be acknowledgements or more, so
// they wake the routine like any message); they are only never grouped / reply-to targets.
func isUpdateLine(typ string) bool {
	return typ == "media_update" || typ == "transcript_update"
}

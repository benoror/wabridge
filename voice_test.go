package main

import (
	"context"
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow"
)

func TestWaveform(t *testing.T) {
	pcm := make([]byte, 2*640)
	for i := 0; i < 640; i++ {
		v := int16(0)
		if i >= 320 {
			v = 1000
		}
		binary.LittleEndian.PutUint16(pcm[2*i:], uint16(v))
	}
	w := waveform(pcm, 64)
	if len(w) != 64 || w[0] != 0 || w[63] != 100 {
		t.Fatalf("waveform %v", w)
	}
	if len(waveform(nil, 64)) != 64 {
		t.Fatal("empty pcm")
	}
}

func TestTTSCmd(t *testing.T) {
	c, err := ttsCmd(context.Background(), VoiceProfile{Engine: "espeak-ng", Voice: "es-419", Rate: 1.2}, "hola", "/tmp/o.wav")
	if err != nil || c.Args[0] != "espeak-ng" || c.Args[2] != "es-419" || c.Args[4] != "210" {
		t.Fatalf("espeak %v %v", c, err)
	}
	if _, err := ttsCmd(context.Background(), VoiceProfile{Engine: "piper"}, "x", "o"); err == nil {
		t.Fatal("piper without model accepted")
	}
	c, _ = ttsCmd(context.Background(), VoiceProfile{Engine: "command", Locale: "es-VE", Command: []string{"tts", "-l", "{locale}", "-o", "{out}", "{text}"}}, "hola", "o.wav")
	if c.Args[2] != "es-VE" || c.Args[4] != "o.wav" || c.Args[5] != "hola" {
		t.Fatalf("command %v", c.Args)
	}
	d := t.TempDir()
	os.MkdirAll(filepath.Join(d, "voices"), 0o700)
	os.WriteFile(filepath.Join(d, "voices", "alice.json"), []byte(`{"engine":"piper","locale":"es-VE","model":"m.onnx"}`), 0o600)
	if v, err := loadVoiceProfile(d, "alice"); err != nil || v.Locale != "es-VE" {
		t.Fatalf("profile %v %v", v, err)
	}
}

func TestPrepareVoiceAndMessage(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("no ffmpeg")
	}
	d := t.TempDir()
	src := filepath.Join(d, "in.wav")
	if err := exec.Command("ffmpeg", "-loglevel", "error", "-f", "lavfi", "-i", "sine=frequency=440:duration=2", "-ac", "2", src).Run(); err != nil {
		t.Fatal(err)
	}
	secs, wave, err := prepareVoice(src, filepath.Join(d, "out.ogg"))
	if err != nil || secs < 2 || secs > 3 || len(wave) != 64 {
		t.Fatalf("prepare: %d %d %v", secs, len(wave), err)
	}
	out, _ := exec.Command("ffprobe", "-v", "error", "-show_entries", "stream=codec_name,channels", "-of", "csv=p=0", filepath.Join(d, "out.ogg")).Output()
	if string(out) != "opus,1\n" {
		t.Fatalf("not mono opus: %q", out)
	}
	m := voiceMessage(whatsmeow.UploadResponse{URL: "u", DirectPath: "/p", FileLength: 9}, secs, wave)
	a := m.GetAudioMessage()
	if !a.GetPTT() || a.GetMimetype() != "audio/ogg; codecs=opus" || a.GetSeconds() != secs || len(a.GetWaveform()) != 64 {
		t.Fatalf("audio msg %v", a)
	}
}

func TestEdgeTTSCmdAndFallback(t *testing.T) {
	c, err := ttsCmd(context.Background(), VoiceProfile{Engine: "edge-tts", Voice: "es-VE-PaolaNeural", Rate: 0.95, Bin: "edge-tts"}, "hola", "o.mp3")
	if err != nil {
		t.Fatal(err)
	}
	want := "edge-tts --voice es-VE-PaolaNeural --rate=-5% --text hola --write-media o.mp3"
	if got := strings.Join(c.Args, " "); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if _, err := ttsCmd(context.Background(), VoiceProfile{Engine: "edge-tts"}, "x", "o"); err == nil {
		t.Fatal("edge-tts without voice must fail")
	}
	out := filepath.Join(t.TempDir(), "o.wav")
	v := VoiceProfile{Engine: "edge-tts", Voice: "x", Bin: "/nonexistent/edge-tts",
		Fallback: &VoiceProfile{Engine: "command", Command: []string{"sh", "-c", "printf RIFF > {out}"}}}
	if err := synthesize(v, "hola", out); err != nil {
		t.Fatalf("fallback not used: %v", err)
	}
	if b, _ := os.ReadFile(out); string(b) != "RIFF" {
		t.Fatalf("fallback output %q", b)
	}
}

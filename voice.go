package main

// Voice notes. Outbound: any audio file (or --say text through a per-profile TTS voice) is
// converted with ffmpeg to OGG/Opus mono 48k (what WhatsApp's own PTT uses), measured, and a
// 64-point waveform (0..100) is computed so it renders as a voice note, not a file.
//
// Voice profiles: <profile>/voices/<name>.json
//   {"engine":"espeak-ng|piper|edge-tts|command","voice":"es-419","locale":"es-MX","rate":1.0,
//    "model":"/path/es_MX-claude-high.onnx","command":["tts","--out","{out}","{text}"],
//    "fallback":{...another voice profile, used if this engine fails...}}
// edge-tts (online, unofficial Microsoft endpoint): "voice" is the neural voice name, e.g. es-VE-PaolaNeural;
// it writes MP3, which ffmpeg probes by content regardless of the out file's extension.

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

const (
	defPiperBin   = "/opt/piper/piper"
	defEdgeTTSBin = "/opt/edge-tts-venv/bin/edge-tts"
	ttsTimeout    = 45 * time.Second // edge-tts is a network call; a hang must fall back, not block a send
)

type VoiceProfile struct {
	Engine   string        `json:"engine"`
	Voice    string        `json:"voice,omitempty"`
	Locale   string        `json:"locale,omitempty"`
	Rate     float64       `json:"rate,omitempty"` // 1.0 = normal
	Model    string        `json:"model,omitempty"`
	Bin      string        `json:"bin,omitempty"`      // engine binary; piper defaults to /opt/piper/piper if present
	Command  []string      `json:"command,omitempty"`  // engine "command": {text} and {out} are substituted
	Fallback *VoiceProfile `json:"fallback,omitempty"` // tried when this engine fails
}

func loadVoiceProfile(profileDir, name string) (VoiceProfile, error) {
	var v VoiceProfile
	b, err := os.ReadFile(filepath.Join(profileDir, "voices", filepath.Base(name)+".json"))
	if err != nil {
		return v, fmt.Errorf("voice profile %q: %w", name, err)
	}
	return v, json.Unmarshal(b, &v)
}

// ttsCmd returns the command that renders text to the WAV file out.
func ttsCmd(ctx context.Context, v VoiceProfile, text, out string) (*exec.Cmd, error) {
	rate := v.Rate
	if rate <= 0 {
		rate = 1
	}
	switch v.Engine {
	case "espeak-ng":
		voice := firstNonEmpty(v.Voice, strings.ToLower(v.Locale), "es-419")
		return exec.CommandContext(ctx, "espeak-ng", "-v", voice, "-s", strconv.Itoa(int(175*rate)), "-w", out, text), nil
	case "piper":
		if v.Model == "" {
			return nil, fmt.Errorf("piper needs \"model\" (.onnx path)")
		}
		bin := v.Bin
		if bin == "" {
			bin = "piper"
			if _, err := os.Stat(defPiperBin); err == nil {
				bin = defPiperBin
			}
		}
		c := exec.CommandContext(ctx, bin, "--model", v.Model, "--length_scale", strconv.FormatFloat(1/rate, 'f', 2, 64), "--output_file", out)
		c.Stdin = strings.NewReader(text)
		return c, nil
	case "edge-tts":
		if v.Voice == "" {
			return nil, fmt.Errorf("edge-tts needs \"voice\" (e.g. es-VE-PaolaNeural)")
		}
		bin := v.Bin
		if bin == "" {
			bin = "edge-tts"
			if _, err := os.Stat(defEdgeTTSBin); err == nil {
				bin = defEdgeTTSBin
			}
		}
		pct := int(math.Round((rate - 1) * 100))
		return exec.CommandContext(ctx, bin, "--voice", v.Voice, fmt.Sprintf("--rate=%+d%%", pct), "--text", text, "--write-media", out), nil
	case "command":
		if len(v.Command) == 0 {
			return nil, fmt.Errorf("engine command needs \"command\"")
		}
		args := make([]string, len(v.Command))
		for i, a := range v.Command {
			args[i] = strings.NewReplacer("{text}", text, "{out}", out, "{locale}", v.Locale, "{voice}", v.Voice).Replace(a)
		}
		return exec.CommandContext(ctx, args[0], args[1:]...), nil
	}
	return nil, fmt.Errorf("unknown tts engine %q", v.Engine)
}

// synthesize renders text with v, then with each fallback in turn; the error names every failure.
func synthesize(v VoiceProfile, text, out string) error {
	var errs []string
	for p := &v; p != nil; p = p.Fallback {
		err := synthOnce(*p, text, out)
		if err == nil {
			if len(errs) > 0 {
				fmt.Fprintf(os.Stderr, "%s tts: %s failed (%s), used fallback %s\n", time.Now().Format("15:04:05"), v.Engine, strings.Join(errs, "; "), p.Engine)
			}
			return nil
		}
		errs = append(errs, p.Engine+": "+err.Error())
		os.Remove(out)
	}
	return fmt.Errorf("%s", strings.Join(errs, "; "))
}

func synthOnce(v VoiceProfile, text, out string) error {
	ctx, cancel := context.WithTimeout(context.Background(), ttsTimeout)
	defer cancel()
	c, err := ttsCmd(ctx, v, text, out)
	if err != nil {
		return err
	}
	if _, err := exec.LookPath(c.Args[0]); err != nil {
		return fmt.Errorf("tts engine %s not installed", c.Args[0])
	}
	if b, err := c.CombinedOutput(); err != nil {
		return fmt.Errorf("tts failed: %v: %s", err, firstLine(string(b)))
	}
	if st, err := os.Stat(out); err != nil || st.Size() == 0 {
		return fmt.Errorf("tts produced no audio")
	}
	return nil
}

func firstLine(s string) string { l, _, _ := strings.Cut(strings.TrimSpace(s), "\n"); return l }

// prepareVoice converts src to OGG/Opus mono at dst and returns seconds + waveform.
func prepareVoice(src, dst string) (uint32, []byte, error) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return 0, nil, fmt.Errorf("ffmpeg not installed")
	}
	if b, err := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-i", src,
		"-vn", "-ac", "1", "-ar", "48000", "-c:a", "libopus", "-b:a", "32k", "-application", "voip", dst).CombinedOutput(); err != nil {
		return 0, nil, fmt.Errorf("ffmpeg convert: %v: %s", err, firstLine(string(b)))
	}
	pcm, err := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-i", dst,
		"-f", "s16le", "-ac", "1", "-ar", "8000", "-").Output()
	if err != nil {
		return 0, nil, fmt.Errorf("ffmpeg decode: %v", err)
	}
	n := len(pcm) / 2
	secs := uint32(math.Ceil(float64(n) / 8000))
	return secs, waveform(pcm, 64), nil
}

// waveform reduces s16le PCM to n peak buckets scaled 0..100.
func waveform(pcm []byte, n int) []byte {
	samples := len(pcm) / 2
	out := make([]byte, n)
	if samples == 0 {
		return out
	}
	peaks := make([]float64, n)
	maxp := 0.0
	for i := 0; i < samples; i++ {
		v := math.Abs(float64(int16(binary.LittleEndian.Uint16(pcm[2*i:]))))
		b := i * n / samples
		if v > peaks[b] {
			peaks[b] = v
		}
		if v > maxp {
			maxp = v
		}
	}
	if maxp == 0 {
		return out
	}
	for i, p := range peaks {
		out[i] = byte(math.Round(p / maxp * 100))
	}
	return out
}

// uploadVoice uploads the prepared OGG and returns a PTT AudioMessage.
func uploadVoice(ctx context.Context, cl *whatsmeow.Client, o OutMsg) (*waE2E.Message, error) {
	data, err := os.ReadFile(o.Voice)
	if err != nil {
		return nil, err
	}
	up, err := cl.Upload(ctx, data, whatsmeow.MediaAudio)
	if err != nil {
		return nil, fmt.Errorf("upload voice: %w", err)
	}
	return voiceMessage(up, o.VoiceSecs, o.Waveform), nil
}

func voiceMessage(up whatsmeow.UploadResponse, secs uint32, wave []byte) *waE2E.Message {
	return &waE2E.Message{AudioMessage: &waE2E.AudioMessage{
		URL: proto.String(up.URL), DirectPath: proto.String(up.DirectPath), MediaKey: up.MediaKey,
		FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: proto.Uint64(up.FileLength),
		Mimetype: proto.String("audio/ogg; codecs=opus"), Seconds: proto.Uint32(secs), PTT: proto.Bool(true),
		Waveform: bytes.Clone(wave),
	}}
}

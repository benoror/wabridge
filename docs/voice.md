# Voice (TTS + inbound transcription)

Both features use local engines; nothing is sent to a cloud service.

- Outbound voice notes: Piper (`/opt/piper/piper` by default) or edge-tts, with a fallback chain.
  Set `"bin"` in a voice profile to point at a different binary.
- Inbound transcription: whisper.cpp (`/opt/whisper.cpp/build/bin/whisper-cli` by default, model
  `models/ggml-small.bin`, multilingual). Override with `bin` / `model` in `transcribe.json`.

Voice profiles: `~/.whatsapp-bridge/<profile>/voices/<name>.json` (see `voice.go`). Example:
[`examples/voice-es-AR.json`](examples/voice-es-AR.json) — copy it to
`~/.whatsapp-bridge/<profile>/voices/default.json` and adjust `model` to your Piper voice path.

Transcription: see `transcribe.go`. Voice lines keep `transcript:null`; a later
`{"type":"transcript_update","id","chat","transcript"|"transcript_error","lang"}` line carries the text.
Config: `<profile>/transcribe.json` (enabled, bin, model, lang `"es"|"auto"`, threads, workers, timeout_sec).
The webhook notice for a voice note waits for its transcript line.

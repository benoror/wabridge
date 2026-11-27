// wabridge: minimal WhatsApp linked-device bridge (one profile = one number).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/skip2/go-qrcode"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waMmsRetry "go.mau.fi/whatsmeow/proto/waMmsRetry"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

func die(f string, a ...any) { fmt.Fprintf(os.Stderr, f+"\n", a...); os.Exit(1) }

func profileDir(p string) string {
	home, _ := os.UserHomeDir()
	d := filepath.Join(home, ".whatsapp-bridge", p)
	if err := os.MkdirAll(d, 0o700); err != nil {
		die("mkdir: %v", err)
	}
	os.Chmod(filepath.Dir(d), 0o700)
	os.Chmod(d, 0o700)
	return d
}

func newClient(ctx context.Context, profile string) (*whatsmeow.Client, string) {
	dir := profileDir(profile)
	// Logs at WARN only; whatsmeow never logs message bodies at this level.
	log := waLog.Stdout("wa", "WARN", false)
	db := "file:" + filepath.Join(dir, "session.db") + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
	c, err := sqlstore.New(ctx, "sqlite", db, log)
	if err != nil {
		die("store: %v", err)
	}
	os.Chmod(filepath.Join(dir, "session.db"), 0o600)
	dev, err := c.GetFirstDevice(ctx)
	if err != nil {
		die("device: %v", err)
	}
	cl := whatsmeow.NewClient(dev, tapLogger{log})
	cl.EnableAutoReconnect = true
	cl.SetWebsocketHTTPClient(wsConns.Client()) // lets the watchdog hard-drop a dead socket
	return cl, dir
}

func main() {
	if len(os.Args) < 2 {
		die("usage: wabridge pair|run|send|forward|poll|typing|notify-test|inbox --profile NAME [--to JID --text TEXT] [--qr PATH]")
	}
	cmd := os.Args[1]
	if cmd == "inbox" {
		inboxCmd(os.Args[2:])
		return
	}
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	profile := fs.String("profile", "", "profile name (e.g. alice)")
	qrPath := fs.String("qr", "", "QR PNG path (default ./<profile>-qr.png)")
	pairPhone := fs.String("phone", "", "pair: link with phone number instead of QR (international digits, e.g. 5218112345678); prints an 8-character code to enter on the phone")
	to := fs.String("to", "", "recipient JID or phone number")
	text := fs.String("text", "", "message text")
	msgID := fs.String("msg-id", "", "optional WhatsApp message id to reuse (outbox only; same id is deduped)")
	replyTo := fs.String("reply-to", "", "quote this message id")
	replyChat := fs.String("reply-chat", "", "chat of the quoted message (default --to)")
	replySender := fs.String("reply-sender", "", "author JID of the quoted message (default: from inbox)")
	fromChat := fs.String("from-chat", "", "forward: source chat JID")
	fwdID := fs.String("message-id", "", "forward: source message id")
	voiceFile := fs.String("voice", "", "send an audio file as a voice note (converted to OGG/Opus)")
	say := fs.String("say", "", "send text as a voice note through --voice-profile TTS")
	voiceProfile := fs.String("voice-profile", "", "voice profile name (<profile>/voices/<name>.json)")
	imageFile := fs.String("image", "", "send an image file (JPEG/PNG, <=16 MB) as a photo, uploaded unchanged")
	docFile := fs.String("document", "", "send any file (<=100 MB) as a document")
	fileName := fs.String("file-name", "", "document: file name shown to the recipient (default: basename)")
	caption := fs.String("caption", "", "image/document caption (default: --text)")
	asPoll := fs.Bool("poll", false, "send: mark the sent message as a poll (reactions to it wake the routine); poll: mark --message-id")
	chunk := fs.Int("chunk", 0, "chunk number (1..N) of a split reply; with --msg-id gives each chunk its own stable id")
	presState := fs.String("state", PresComposing, "typing: composing|recording|paused")
	presFor := fs.Duration("for", 25*time.Second, "typing: how long to show it (refreshed every ~10s), then paused")
	fs.Parse(os.Args[2:])
	if *chunk > 0 {
		if *msgID == "" {
			die("--chunk needs --msg-id")
		}
		*msgID = chunkMsgID(*msgID, *chunk)
	}
	if *profile == "" {
		die("--profile required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if cmd == "notify-test" {
		d := profileDir(*profile)
		n := &Notifier{URL: loadWebhook(d), Key: loadWebhookKey(d), Profile: *profile}
		if n.URL == "" {
			die("no webhook_url for profile %s", *profile)
		}
		body, _ := json.Marshal(map[string]any{"profile": *profile, "test": true, "count": 0,
			"chats": []string{}, "inbox_path": filepath.Join(d, "inbox.jsonl")})
		st, err := n.Post(body)
		if err != nil {
			die("notify-test failed: %v (status %d, key: %v)", err, st, n.Key != "")
		}
		fmt.Println("notify-test ok: status", st, "key:", n.Key != "")
		return
	}
	if cmd == "poll" {
		if *fwdID == "" {
			die("poll needs --message-id (one of our sent message ids)")
		}
		if err := markPoll(profileDir(*profile), *fwdID); err != nil {
			die("poll: %v", err)
		}
		fmt.Printf("{\"ok\":true,\"poll\":%q}\n", *fwdID)
		return
	}
	if cmd == "typing" {
		if *to == "" || !validPresState(*presState) {
			die("typing needs --to and --state composing|recording|paused")
		}
		d := profileDir(*profile)
		if !runActive(d) {
			die("typing needs the bridge running")
		}
		if err := requestPresence(d, PresReq{To: *to, State: *presState, ForMs: presFor.Milliseconds()}, 5*time.Second); err != nil {
			die("typing: %v", err)
		}
		fmt.Printf("{\"ok\":true,\"state\":%q,\"for_ms\":%d}\n", *presState, presFor.Milliseconds())
		return
	}
	if cmd == "forward" {
		if *fromChat == "" || *fwdID == "" || *to == "" {
			die("forward needs --from-chat, --message-id and --to")
		}
		cmd = "send"
	}
	if cmd == "send" {
		media := *imageFile != "" || *docFile != ""
		rich := *replyTo != "" || *fwdID != "" || *voiceFile != "" || *say != "" || media || *asPoll
		if *to == "" || (*text == "" && *fwdID == "" && *voiceFile == "" && *say == "" && !media) {
			die("--to and one of --text/--voice/--say/--image/--document required")
		}
		if media && *caption == "" {
			*caption, *text = *text, "" // --text with an attachment is its caption
		}
		for _, p := range []*string{imageFile, docFile} {
			if *p != "" {
				a, err := filepath.Abs(*p) // the running bridge reads it, maybe from another cwd
				if err != nil {
					die("%v", err)
				}
				*p = a
			}
		}
		if err := (OutMsg{To: *to, Text: *text, Voice: firstNonEmpty(*voiceFile, *say), FwdID: *fwdID, Image: *imageFile,
			Document: *docFile, FileName: *fileName, Caption: *caption}).valid(); err != nil {
			die("send: %v", err)
		}
		if *say != "" && *voiceProfile == "" {
			die("--say needs --voice-profile")
		}
		if rich && !runActive(profileDir(*profile)) {
			die("--reply-to/--voice/--say/--image/--document/--poll/forward need the bridge running (they go through the outbox)")
		}
		// If run is active, hand off via outbox (don't open a second connection).
		if d := profileDir(*profile); runActive(d) {
			id := strconv.FormatInt(time.Now().UnixNano(), 36)
			if *msgID != "" {
				id = "m-" + safeName(*msgID) // same --msg-id => same outbox item (idempotent)
			}
			om := OutMsg{ID: id, To: *to, Text: *text, MsgID: *msgID, ReplyTo: *replyTo, ReplyChat: *replyChat,
				ReplySender: *replySender, FwdChat: *fromChat, FwdID: *fwdID,
				Image: *imageFile, Document: *docFile, FileName: *fileName, Caption: *caption}
			if *voiceFile != "" || *say != "" {
				vdir := filepath.Join(d, "voice")
				os.MkdirAll(vdir, 0o700)
				src := *voiceFile
				if *say != "" {
					vp, err := loadVoiceProfile(d, *voiceProfile)
					if err != nil {
						die("voice: %v", err)
					}
					src = filepath.Join(vdir, id+".wav")
					if err := synthesize(vp, *say, src); err != nil {
						die("voice: %v", err)
					}
					defer os.Remove(src)
					om.Text = "" // the spoken text is not logged as message text
				}
				om.Voice = filepath.Join(vdir, id+".ogg")
				var err error
				if om.VoiceSecs, om.Waveform, err = prepareVoice(src, om.Voice); err != nil {
					die("voice: %v", err)
				}
			}
			r, err := enqueue(outboxDir(d), om, 3*time.Minute)
			if err != nil {
				die("send: %v", err)
			}
			if *asPoll && r.OK {
				if err := markPoll(d, firstNonEmpty(r.MessageID, r.MsgID)); err != nil {
					fmt.Fprintln(os.Stderr, "poll mark failed:", err)
				}
			}
			b, _ := json.Marshal(r)
			fmt.Println(string(b))
			if !r.OK {
				os.Exit(1)
			}
			return
		}
	}
	cl, dir := newClient(ctx, *profile)
	res := &Resolver{Lookup: func(ctx context.Context, phone string) (types.JID, bool, error) {
		c, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		rs, err := cl.IsOnWhatsApp(c, []string{phone})
		if err != nil || len(rs) == 0 {
			if err == nil {
				err = fmt.Errorf("empty IsOnWhatsApp response")
			}
			return types.JID{}, false, err
		}
		return rs[0].JID, rs[0].IsIn, nil
	}}
	toJID := res.Cached // logging / echo lines only; sends go through res.Resolve

	switch cmd {
	case "pair":
		if cl.Store.ID != nil {
			die("profile %s already paired as %s", *profile, cl.Store.ID.User)
		}
		if *qrPath == "" {
			*qrPath = *profile + "-qr.png"
		}
		os.MkdirAll(filepath.Dir(*qrPath), 0o700)
		ch, _ := cl.GetQRChannel(ctx)
		phoneCodeShown := false
		if err := cl.Connect(); err != nil {
			die("connect: %v", err)
		}
		for evt := range ch {
			switch evt.Event {
			case "code":
				if *pairPhone != "" {
					if !phoneCodeShown {
						code, err := cl.PairPhone(ctx, *pairPhone, true, whatsmeow.PairClientChrome, "Chrome (Linux)")
						if err != nil {
							die("pair phone: %v", err)
						}
						phoneCodeShown = true
						fmt.Printf("pairing code: %s (WhatsApp > Linked devices > Link a device > Link with phone number instead)\n", code)
					}
					continue
				}
				if err := qrcode.WriteFile(evt.Code, qrcode.Medium, 512, *qrPath); err != nil {
					die("qr: %v", err)
				}
				os.Chmod(*qrPath, 0o600)
				fmt.Printf("QR written to %s (expires in %s)\n", *qrPath, evt.Timeout)
			case "success":
				os.Remove(*qrPath)
				fmt.Println("paired")
				time.Sleep(3 * time.Second) // let initial sync settle
				cl.Disconnect()
				return
			default:
				os.Remove(*qrPath)
				die("pairing ended: %s", evt.Event)
			}
		}
	case "run":
		if cl.Store.ID == nil {
			die("not paired; run: wabridge pair --profile %s", *profile)
		}
		lk := lockRun(dir)
		if lk == nil {
			die("run already active for profile %s", *profile)
		}
		defer lk.Close()
		f, err := os.OpenFile(filepath.Join(dir, "inbox.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			die("inbox: %v", err)
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		var encMu sync.Mutex
		write := func(v any) { encMu.Lock(); enc.Encode(v); encMu.Unlock() }
		mediaDir := filepath.Join(dir, "media")
		os.MkdirAll(mediaDir, 0o700)
		notif := &Notifier{URL: loadWebhook(dir), Key: loadWebhookKey(dir), Profile: *profile, InboxPath: f.Name(), Delay: 10 * time.Second, Cooldown: 60 * time.Second}
		seen := &SeenSet{Path: filepath.Join(dir, "seen.log")}
		if err := seen.Load(); err != nil {
			die("seen: %v", err)
		}
		defer seen.Close()
		health := &Health{Dir: dir}
		health.Set("starting", "")
		var stt *Transcriber
		if tc, err := loadTranscribeConfig(dir); err != nil {
			fmt.Fprintln(os.Stderr, "transcribe disabled:", err)
		} else if tc.Active() {
			stt = NewTranscriber(tc, write)
			fmt.Fprintln(os.Stderr, "transcribe on: lang", tc.Lang)
		}
		pcfg, err := loadPresenceConfig(dir)
		if err != nil {
			fmt.Fprintln(os.Stderr, "presence.json invalid, using defaults:", err)
		}
		pres := &Presencer{Cfg: pcfg,
			Chat: func(ctx context.Context, to, state string) error {
				if !cl.IsLoggedIn() {
					return fmt.Errorf("not connected")
				}
				st, media := types.ChatPresenceComposing, types.ChatPresenceMediaText
				switch state {
				case PresRecording:
					media = types.ChatPresenceMediaAudio
				case PresPaused:
					st = types.ChatPresencePaused
				}
				jid, err := res.Resolve(ctx, to)
				if err != nil {
					return err
				}
				return cl.SendChatPresence(ctx, jid, st, media)
			},
			Online: func(ctx context.Context, on bool) error {
				if !cl.IsLoggedIn() {
					return fmt.Errorf("not connected")
				}
				if cl.Store.PushName == "" && pcfg.PushName != "" {
					cl.Store.PushName = pcfg.PushName
				}
				if on {
					return cl.SendPresence(ctx, types.PresenceAvailable)
				}
				return cl.SendPresence(ctx, types.PresenceUnavailable)
			}}
		fmt.Fprintln(os.Stderr, "presence: auto", pcfg.auto(), "max_ms", pcfg.max().Milliseconds(), "online", pcfg.online())
		pns := &PushNameSyncer{Want: pcfg.PushName,
			Have: func() string { return cl.Store.PushName },
			Send: func(ctx context.Context, name string) error {
				c, cancel := context.WithTimeout(ctx, 20*time.Second)
				defer cancel()
				if err := cl.SendAppState(c, appstate.BuildSettingPushName(name)); err != nil {
					return err
				}
				cl.Store.PushName = name
				if err := cl.Store.Save(c); err != nil {
					fmt.Fprintln(os.Stderr, time.Now().Format("15:04:05"), "push_name: store save:", err)
				}
				return nil
			},
			Log: func(f string, a ...any) {
				fmt.Fprintf(os.Stderr, "%s "+f+"\n", append([]any{time.Now().Format("15:04:05")}, a...)...)
			}}
		gate := &ReplayGate{}
		ib := &Inbox{Dir: dir}
		logf := func(f string, a ...any) {
			fmt.Fprintf(os.Stderr, "%s "+f+"\n", append([]any{time.Now().Format("15:04:05")}, a...)...)
		}
		notif.Pending, notif.Log = ib.Pending, logf
		notif.Snapshot = ib.NotifyMark
		notif.Mark = func(mk NoticeMark) {
			if err := ib.MarkNotified(mk); err != nil {
				logf("webhook: saving notified watermark failed: %v", err)
			}
		}
		const connectSettle, offlineSettle = 15 * time.Second, 3 * time.Second
		settle := &Settler{Fn: func() { settleCheck(ib, notif, logf) }}
		pipe := &MsgPipe{Ctx: ctx, Seen: seen, Gate: gate, Write: write, Log: logf, DL: cl, MediaDir: mediaDir,
			RetryReceipt: func(info *types.MessageInfo, key []byte) error { return cl.SendMediaRetryReceipt(ctx, info, key) },
			STT:          stt, Settle: settle.Trigger, Notify: notif.Add, ConnectSettle: connectSettle, OfflineSettle: offlineSettle,
			IsPoll:          func(id string) bool { return pollMarked(dir, id) },
			DecryptReaction: func(v *events.Message) (*waE2E.ReactionMessage, error) { return cl.DecryptReaction(ctx, v) }}
		wd := NewWatchdog(cl)
		wd.Drop = func() { wsConns.DropAll() }
		health.WD = wd
		tap := wd.TransportActivity
		frameHook.Store(&tap)
		cl.AddEventHandler(func(e any) {
			health.Event()
			switch v := e.(type) {
			case *events.Message:
				wd.AppActivity()
				pipe.Handle(v)
			case *events.MediaRetry:
				// Phone re-uploaded expired media: patch direct_path, re-download, append an update line.
				m, err := loadMsg(mediaDir, v.MessageID)
				d, _, _, _ := mediaOf(m)
				if err != nil || d == nil {
					return
				}
				line := map[string]any{"id": v.MessageID, "chat": v.ChatID.String(), "type": "media_update",
					"ts": v.Timestamp.UTC().Format(time.RFC3339)}
				n, err := whatsmeow.DecryptMediaRetryNotification(v, d.GetMediaKey())
				if err == nil && n.GetResult() == waMmsRetry.MediaRetryNotification_SUCCESS {
					setDirectPath(m, n.GetDirectPath())
					var mf map[string]any
					mf, _, err = saveMedia(ctx, cl, mediaDir, v.MessageID, m)
					for k, x := range mf {
						line[k] = x
					}
				} else if err == nil {
					line["download_error"] = "media retry: " + n.GetResult().String()
				} else {
					line["download_error"] = err.Error()
				}
				write(line)
			case *events.KeepAliveTimeout:
				wd.KeepAliveFailed()
			case *events.Connected:
				replacedRetry.Reset(dir)
				health.Set("connected", "")
				go func() {
					pns.Sync(ctx) // before presence: online presence needs a push name
					pres.Connected(ctx)
				}()
				wd.KeepAliveOK()
				pipe.Connected() // (re)connect / process start: notify if anything is pending
			case *events.KeepAliveRestored:
				wd.KeepAliveOK()
			case *events.Disconnected:
				if health.State() == "connected" { // never mask stream_replaced / logged_out
					health.Set("disconnected", "socket closed; watchdog/auto-reconnect pending")
				}
			case *events.OfflineSyncPreview:
				pipe.OfflineSync(true)
			case *events.OfflineSyncCompleted:
				pipe.OfflineSync(false)
			case *events.StreamReplaced:
				// Terminal: another client connected with these keys. Reconnecting would kick it
				// off and get kicked back forever. Halt, stay up idle (no supervisor restart loop).
				// Not terminal anymore (2026-10-25): a replacement can come from a stale box copy or a
				// WhatsApp-side event and then go away, leaving us silent for hours. Retry with a
				// 60s delay doubling to a 30 min cap; state in <profile>/replaced.json.
				fmt.Fprintln(os.Stderr, time.Now().Format("15:04:05"), "stream replaced by another client; retrying with backoff")
				cl.EnableAutoReconnect = false
				health.Set("stream_replaced", "another client connected with this session; retrying with backoff")
				go bounded(4*time.Second, cl.Disconnect)
				replacedRetry.Start(ctx, dir, func() bool { return cl.IsConnected() && cl.IsLoggedIn() }, func() {
					cl.EnableAutoReconnect = true
					if err := cl.Connect(); err != nil {
						fmt.Fprintln(os.Stderr, time.Now().Format("15:04:05"), "replaced retry connect:", err)
					}
				})
			case *events.LoggedOut:
				health.Set("logged_out", "re-pair required")
				fmt.Fprintln(os.Stderr, "logged out; re-pair required")
				stop()
			}
		})
		if err := cl.Connect(); err != nil {
			die("connect: %v", err)
		}
		look := inboxLookup(f.Name(), mediaDir)
		send := func(ctx context.Context, o OutMsg) (string, error) {
			if !cl.IsLoggedIn() {
				return "", fmt.Errorf("not connected")
			}
			jid, err := res.Resolve(ctx, o.To)
			if err != nil {
				return "", err
			}
			var media *waE2E.Message
			if o.FwdID == "" {
				var err error
				uctx, cancel := context.WithTimeout(ctx, 3*time.Minute) // bounded upload
				switch {
				case o.Voice != "":
					media, err = uploadVoice(uctx, cl, o)
				case o.Image != "":
					media, err = uploadImage(uctx, cl, o)
				case o.Document != "":
					media, err = uploadDocument(uctx, cl, o)
				}
				cancel()
				if err != nil {
					return "", err
				}
			}
			own := ""
			if cl.Store.ID != nil {
				own = cl.Store.ID.ToNonAD().String()
			}
			msg, err := buildMessage(o, look, own, media)
			if err != nil {
				return "", err
			}
			// Bounded wait for the server ack; same id on every retry (WhatsApp dedupes by id).
			r, err := cl.SendMessage(ctx, jid, msg,
				whatsmeow.SendRequestExtra{ID: types.MessageID(o.MsgID), Timeout: 20 * time.Second})
			return r.ID, err
		}
		onSent := func(r Result) {
			line := map[string]any{"id": r.MessageID, "chat": toJID(r.To).String(), "from_me": true,
				"ts": r.TS, "type": "text", "text": r.Text, "via": "outbox"}
			if r.ReplyTo != "" {
				line["quoted_id"] = r.ReplyTo
			}
			if r.FwdID != "" {
				line["type"], line["is_forwarded"], line["forwarded_from"] = "forward", true, r.FwdID
			}
			if r.Voice != "" && r.FwdID == "" {
				line["type"], line["ptt"], line["duration"] = "voice", true, r.VoiceSecs
			}
			if r.FwdID == "" && (r.Image != "" || r.Document != "") {
				line["type"], line["text"], line["media_path"] = "image", r.Caption, r.Image
				if r.Document != "" {
					line["type"], line["media_path"], line["file_name"] = "document", r.Document, docName(r.OutMsg)
				}
			}
			seen.Seen(toJID(r.To), r.MessageID) // our own echo must not be re-logged
			write(line)
		}
		ob := &Outbox{Dir: outboxDir(dir), Send: send, OnSent: onSent, Backoff: 2 * time.Second,
			NewID:     func() string { return string(cl.GenerateMessageID()) },
			Ensure:    func(ctx context.Context) error { return wd.Ensure(ctx, cl.IsLoggedIn, 30*time.Second) },
			Reconnect: wd.ForceReconnect, Pending: wd.AddPending, Presence: pres}
		go ob.Watch(ctx)
		go watchLeases(ctx, 30*time.Second, func() { leaseCheck(ib, notif, logf) }) // dead runs' leases
		go pres.WatchRequests(ctx, presenceDir(dir))
		fmt.Println("running; inbox:", f.Name(), "webhook:", notif.URL != "", "webhook_auth:", notif.Key != "")
		hbDone := make(chan struct{})
		defer close(hbDone)
		go health.Run(hbDone, StatusBeat)
		wdErr := make(chan error, 1)
		go func() { wdErr <- wd.Run(ctx) }()
		select {
		case <-ctx.Done():
		case err := <-wdErr:
			fmt.Fprintln(os.Stderr, "watchdog: giving up:", err)
			bounded(3*time.Second, cl.Disconnect)
			os.Exit(1) // supervisor restarts us
		}
		// Bounded shutdown: a hung websocket close must not keep us alive past ~5s.
		if !bounded(4*time.Second, cl.Disconnect) {
			fmt.Fprintln(os.Stderr, "disconnect hung; exiting anyway")
		}
		f.Close()
		lk.Close()
		os.Exit(0)
	case "send":
		if cl.Store.ID == nil {
			die("not paired")
		}
		if err := cl.Connect(); err != nil {
			die("connect: %v", err)
		}
		defer cl.Disconnect()
		for i := 0; i < 50 && !cl.IsLoggedIn(); i++ {
			time.Sleep(200 * time.Millisecond)
		}
		jid, err := res.Resolve(ctx, *to)
		if err != nil {
			die("send: %v", err)
		}
		r, err := cl.SendMessage(ctx, jid, &waE2E.Message{Conversation: proto.String(*text)})
		if err != nil {
			die("send: %v", err)
		}
		fmt.Println("sent", r.ID)
		if f, err := os.OpenFile(filepath.Join(dir, "inbox.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600); err == nil {
			json.NewEncoder(f).Encode(map[string]any{"id": r.ID, "chat": jid.String(), "from_me": true,
				"ts": time.Now().UTC().Format(time.RFC3339), "type": "text", "text": *text, "via": "direct"})
			f.Close()
		}
	default:
		die("unknown command %s", cmd)
	}
}

package main

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"strings"

	"go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Downloader is the slice of *whatsmeow.Client we need (fakeable in tests).
type Downloader interface {
	Download(ctx context.Context, msg whatsmeow.DownloadableMessage) ([]byte, error)
}

// mediaOf returns the downloadable part of m plus its mime, caption and file name.
func mediaOf(m *waE2E.Message) (d whatsmeow.DownloadableMessage, mt, caption, fname string) {
	switch {
	case m.GetImageMessage() != nil:
		x := m.GetImageMessage()
		return x, x.GetMimetype(), x.GetCaption(), ""
	case m.GetDocumentMessage() != nil:
		x := m.GetDocumentMessage()
		return x, x.GetMimetype(), x.GetCaption(), x.GetFileName()
	case m.GetDocumentWithCaptionMessage().GetMessage().GetDocumentMessage() != nil:
		x := m.GetDocumentWithCaptionMessage().GetMessage().GetDocumentMessage()
		return x, x.GetMimetype(), x.GetCaption(), x.GetFileName()
	case m.GetVideoMessage() != nil:
		x := m.GetVideoMessage()
		return x, x.GetMimetype(), x.GetCaption(), ""
	case m.GetPtvMessage() != nil: // round video note
		x := m.GetPtvMessage()
		return x, x.GetMimetype(), x.GetCaption(), ""
	case m.GetAudioMessage() != nil:
		x := m.GetAudioMessage()
		return x, x.GetMimetype(), "", ""
	case m.GetStickerMessage() != nil:
		x := m.GetStickerMessage()
		return x, x.GetMimetype(), "", ""
	}
	return nil, "", "", ""
}

func extFor(mt, fname string) string {
	if e := filepath.Ext(fname); e != "" && len(e) <= 6 && !strings.ContainsAny(e, "/\\") {
		return strings.ToLower(e)
	}
	base := strings.TrimSpace(strings.SplitN(mt, ";", 2)[0])
	switch base { // stable picks; mime.ExtensionsByType order varies by system
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/webp":
		return ".webp"
	case "application/pdf":
		return ".pdf"
	case "video/mp4":
		return ".mp4"
	case "audio/ogg":
		return ".ogg"
	}
	if exts, _ := mime.ExtensionsByType(base); len(exts) > 0 {
		return exts[0]
	}
	return ".bin"
}

// saveMedia stores the raw message proto (for later re-download) and the
// decrypted media. Returns inbox fields; nil if m carries no media.
func saveMedia(ctx context.Context, dl Downloader, mediaDir, id string, m *waE2E.Message) (map[string]any, whatsmeow.DownloadableMessage, error) {
	d, mt, caption, fname := mediaOf(m)
	if d == nil {
		return nil, nil, nil
	}
	f := map[string]any{"mime": mt, "caption": caption}
	if fname != "" {
		f["file_name"] = fname
	}
	if err := os.MkdirAll(mediaDir, 0o700); err != nil {
		f["download_error"] = err.Error()
		return f, d, err
	}
	safe := filepath.Base(id)
	if raw, err := proto.Marshal(m); err == nil {
		os.WriteFile(filepath.Join(mediaDir, safe+".msg.pb"), raw, 0o600)
	}
	data, err := dl.Download(ctx, d)
	if err == nil {
		p := filepath.Join(mediaDir, safe+extFor(mt, fname))
		if err = os.WriteFile(p, data, 0o600); err == nil {
			f["media_path"] = p
			return f, d, nil
		}
	}
	f["download_error"] = err.Error()
	return f, d, err
}

func isExpired(err error) bool {
	return errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) || errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410)
}

// loadMsg reads a stored message proto back.
func loadMsg(mediaDir, id string) (*waE2E.Message, error) {
	raw, err := os.ReadFile(filepath.Join(mediaDir, filepath.Base(id)+".msg.pb"))
	if err != nil {
		return nil, err
	}
	m := &waE2E.Message{}
	return m, proto.Unmarshal(raw, m)
}

func setDirectPath(m *waE2E.Message, p string) {
	switch {
	case m.GetImageMessage() != nil:
		m.ImageMessage.DirectPath = &p
	case m.GetDocumentMessage() != nil:
		m.DocumentMessage.DirectPath = &p
	case m.GetDocumentWithCaptionMessage().GetMessage().GetDocumentMessage() != nil:
		m.DocumentWithCaptionMessage.Message.DocumentMessage.DirectPath = &p
	case m.GetVideoMessage() != nil:
		m.VideoMessage.DirectPath = &p
	case m.GetPtvMessage() != nil:
		m.PtvMessage.DirectPath = &p
	case m.GetAudioMessage() != nil:
		m.AudioMessage.DirectPath = &p
	case m.GetStickerMessage() != nil:
		m.StickerMessage.DirectPath = &p
	}
}

// describe gives non-downloadable "media" messages a descriptive type plus a
// few safe fields (no free text). E.g. an album parent precedes its photos.
func describe(m *waE2E.Message) (string, map[string]any) {
	switch {
	case m.GetAlbumMessage() != nil:
		a := m.GetAlbumMessage()
		return "album", map[string]any{"album_images": a.GetExpectedImageCount(), "album_videos": a.GetExpectedVideoCount()}
	case m.GetContactMessage() != nil:
		return "contact", nil
	case m.GetContactsArrayMessage() != nil:
		return "contacts", map[string]any{"count": len(m.GetContactsArrayMessage().GetContacts())}
	case m.GetLocationMessage() != nil:
		l := m.GetLocationMessage()
		return "location", map[string]any{"lat": l.GetDegreesLatitude(), "lng": l.GetDegreesLongitude()}
	case m.GetLiveLocationMessage() != nil:
		return "live_location", nil
	case m.GetPollCreationMessage() != nil, m.GetPollCreationMessageV3() != nil:
		return "poll", nil
	}
	// Unknown: name the first populated top-level field (e.g. "eventMessage").
	var name string
	m.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		if fd.Message() != nil {
			name = string(fd.JSONName())
			return false
		}
		return true
	})
	if name == "" {
		return "media", nil
	}
	return "media", map[string]any{"subtype": fmt.Sprint(name)}
}

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

type fakeDL struct {
	data []byte
	err  error
}

func (f fakeDL) Download(context.Context, whatsmeow.DownloadableMessage) ([]byte, error) {
	return f.data, f.err
}

func TestSaveMediaImage(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "media")
	m := &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Mimetype: proto.String("image/jpeg"), Caption: proto.String("taxi")}}
	f, _, err := saveMedia(context.Background(), fakeDL{data: []byte("JPG")}, dir, "ID1", m)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "ID1.jpg")
	if f["media_path"] != p || f["mime"] != "image/jpeg" || f["caption"] != "taxi" {
		t.Fatalf("fields %v", f)
	}
	for _, q := range []string{p, filepath.Join(dir, "ID1.msg.pb")} {
		st, err := os.Stat(q)
		if err != nil || st.Mode().Perm() != 0o600 {
			t.Fatalf("%s: %v %v", q, err, st)
		}
	}
	if st, _ := os.Stat(dir); st.Mode().Perm() != 0o700 {
		t.Fatalf("dir perm %v", st.Mode())
	}
	back, err := loadMsg(dir, "ID1")
	if err != nil || back.GetImageMessage().GetCaption() != "taxi" {
		t.Fatalf("loadMsg %v", err)
	}
}

func TestSaveMediaPDFAndError(t *testing.T) {
	dir := t.TempDir()
	m := &waE2E.Message{DocumentWithCaptionMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{
		DocumentMessage: &waE2E.DocumentMessage{Mimetype: proto.String("application/pdf"), FileName: proto.String("Factura.PDF")}}}}
	f, _, _ := saveMedia(context.Background(), fakeDL{data: []byte("%PDF")}, dir, "D", m)
	if f["media_path"] != filepath.Join(dir, "D.pdf") || f["file_name"] != "Factura.PDF" {
		t.Fatalf("fields %v", f)
	}
	f, _, err := saveMedia(context.Background(), fakeDL{err: whatsmeow.ErrMediaDownloadFailedWith410}, dir, "E", m)
	if err == nil || f["download_error"] == nil || f["media_path"] != nil || !isExpired(err) {
		t.Fatalf("fields %v err %v", f, err)
	}
	if isExpired(errors.New("x")) {
		t.Fatal("isExpired")
	}
}

func TestSaveMediaNone(t *testing.T) {
	f, _, err := saveMedia(context.Background(), fakeDL{}, t.TempDir(), "T", &waE2E.Message{Conversation: proto.String("hi")})
	if f != nil || err != nil {
		t.Fatal("text message produced media fields")
	}
}

func TestSetDirectPath(t *testing.T) {
	m := &waE2E.Message{ImageMessage: &waE2E.ImageMessage{}}
	setDirectPath(m, "/new")
	if m.GetImageMessage().GetDirectPath() != "/new" {
		t.Fatal("direct path not set")
	}
}

func TestDescribe(t *testing.T) {
	ty, f := describe(&waE2E.Message{AlbumMessage: &waE2E.AlbumMessage{ExpectedImageCount: proto.Uint32(2)}})
	if ty != "album" || f["album_images"] != uint32(2) {
		t.Fatal(ty, f)
	}
	if ty, _ := describe(&waE2E.Message{LocationMessage: &waE2E.LocationMessage{}}); ty != "location" {
		t.Fatal(ty)
	}
	if ty, f := describe(&waE2E.Message{EventMessage: &waE2E.EventMessage{}}); ty != "media" || f["subtype"] != "eventMessage" {
		t.Fatal(ty, f)
	}
	if d, _, _, _ := mediaOf(&waE2E.Message{PtvMessage: &waE2E.VideoMessage{Mimetype: proto.String("video/mp4")}}); d == nil {
		t.Fatal("ptv should be downloadable")
	}
}

package main

import (
	"bytes"
	"encoding/json"
	"image"
	"image/color"
	"image/jpeg"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow"
)

func writeJPEG(t *testing.T, path string, w, h int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			img.Set(x, y, color.RGBA{uint8(x), uint8(y), 80, 255})
		}
	}
	var b bytes.Buffer
	jpeg.Encode(&b, img, nil)
	if err := os.WriteFile(path, b.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestMimeFor(t *testing.T) {
	for name, want := range map[string]string{
		"a.pdf": "application/pdf", "A.PDF": "application/pdf", "t.jpg": "image/jpeg", "t.JPEG": "image/jpeg",
		"p.png": "image/png", "x.docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		"x.xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "f.xml": "application/xml",
	} {
		if got := mimeFor(name, nil); got != want {
			t.Errorf("%s: %s want %s", name, got, want)
		}
	}
	if got := mimeFor("noext", []byte("%PDF-1.4\n")); got != "application/pdf" {
		t.Errorf("sniff pdf: %s", got)
	}
	if got := mimeFor("blob.zzq", []byte{0, 1, 2, 3}); got != "application/octet-stream" {
		t.Errorf("unknown: %s", got)
	}
}

func TestImageMime(t *testing.T) {
	jpg := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0, 0x10, 'J', 'F', 'I', 'F'}
	if m, err := imageMime("ticket.jpg", jpg); err != nil || m != "image/jpeg" {
		t.Fatalf("jpg: %s %v", m, err)
	}
	if m, err := imageMime("mislabeled.png", jpg); err != nil || m != "image/jpeg" {
		t.Fatalf("bytes win: %s %v", m, err)
	}
	if _, err := imageMime("doc.pdf", []byte("%PDF-1.4")); err == nil || !strings.Contains(err.Error(), "--document") {
		t.Fatalf("pdf as image must fail: %v", err)
	}
}

func TestOutboxMediaParse(t *testing.T) {
	raw := `{"id":"f1","to":"528112345678","image":"/tmp/t.jpg","caption":"hola"}`
	var m OutMsg
	if err := json.Unmarshal([]byte(raw), &m); err != nil || m.Image != "/tmp/t.jpg" || m.Caption != "hola" {
		t.Fatalf("image parse: %+v %v", m, err)
	}
	raw = `{"id":"f2","to":"528112345678","document":"/tmp/c.pdf","file_name":"CSF.pdf"}`
	m = OutMsg{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil || m.Document != "/tmp/c.pdf" || m.FileName != "CSF.pdf" || docName(m) != "CSF.pdf" {
		t.Fatalf("doc parse: %+v %v", m, err)
	}
	if docName(OutMsg{Document: "/a/b/csf_2026.pdf"}) != "csf_2026.pdf" {
		t.Fatal("default name = basename")
	}
	b, _ := json.Marshal(OutMsg{ID: "x", To: "y", Text: "t"})
	for _, k := range []string{"image", "document", "file_name", "caption"} {
		if strings.Contains(string(b), `"`+k+`"`) {
			t.Fatalf("%s must be omitted when empty: %s", k, b)
		}
	}
}

func TestValidateMedia(t *testing.T) {
	d := t.TempDir()
	jpg := filepath.Join(d, "t.jpg")
	writeJPEG(t, jpg, 40, 30)
	pdf := filepath.Join(d, "c.pdf")
	os.WriteFile(pdf, []byte("%PDF-1.4\n1 0 obj << /Type /Page >> endobj\n"), 0o600)
	empty := filepath.Join(d, "e.pdf")
	os.WriteFile(empty, nil, 0o600)
	big := filepath.Join(d, "big.jpg")
	f, _ := os.Create(big)
	f.Write([]byte{0xFF, 0xD8, 0xFF, 0xE0})
	f.Truncate(maxImageBytes + 1) // sparse
	f.Close()
	ok := []OutMsg{
		{To: "a", Image: jpg}, {To: "a", Image: jpg, Caption: "c"},
		{To: "a", Document: pdf}, {To: "a", Document: pdf, FileName: "x.pdf", Caption: "c"},
		{To: "a", Document: big}, // 16 MB+ is fine as a document
	}
	for _, m := range ok {
		if err := m.valid(); err != nil {
			t.Errorf("%+v: %v", m, err)
		}
	}
	bad := map[string]OutMsg{
		"no such file":         {To: "a", Image: filepath.Join(d, "missing.jpg")},
		"exceeds the 16 MB":    {To: "a", Image: big},
		"empty file":           {To: "a", Document: empty},
		"not a regular file":   {To: "a", Document: d},
		"only one of":          {To: "a", Image: jpg, Document: pdf},
		"use --document":       {To: "a", Image: pdf},
		"file_name needs":      {To: "a", Image: jpg, FileName: "x"},
		"caption needs":        {To: "a", Text: "t", Caption: "c"},
		"one of text/voice/im": {To: "a"},
	}
	for want, m := range bad {
		if err := m.valid(); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: got %v", want, err)
		}
	}
	if _, err := checkFile("document", big, 10); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Errorf("doc size cap: %v", err)
	}
}

func TestImageAndDocumentMessage(t *testing.T) {
	d := t.TempDir()
	data := writeJPEG(t, filepath.Join(d, "t.jpg"), 400, 300)
	w, h, thumb := imageMeta(data)
	if w != 400 || h != 300 || len(thumb) == 0 {
		t.Fatalf("meta %d %d %d", w, h, len(thumb))
	}
	if c, _, err := image.DecodeConfig(bytes.NewReader(thumb)); err != nil || c.Width != 72 || c.Height != 54 {
		t.Fatalf("thumb %+v %v", c, err)
	}
	up := whatsmeow.UploadResponse{URL: "u", DirectPath: "/d", MediaKey: []byte{1}, FileEncSHA256: []byte{2}, FileSHA256: []byte{3}, FileLength: 9}
	m := imageMessage(up, "image/jpeg", "prueba", w, h, thumb)
	im := m.GetImageMessage()
	if im.GetCaption() != "prueba" || im.GetWidth() != 400 || im.GetFileLength() != 9 || im.GetDirectPath() != "/d" || im.GetMimetype() != "image/jpeg" {
		t.Fatalf("image msg %v", im)
	}
	if imageMessage(up, "image/png", "", 0, 0, nil).GetImageMessage().Caption != nil {
		t.Fatal("empty caption must be unset")
	}
	dm := documentMessage(up, "prueba.pdf", "application/pdf", "", 2).GetDocumentMessage()
	if dm.GetFileName() != "prueba.pdf" || dm.GetTitle() != "prueba" || dm.GetPageCount() != 2 || dm.GetMimetype() != "application/pdf" {
		t.Fatalf("doc msg %v", dm)
	}
	wc := documentMessage(up, "a.pdf", "application/pdf", "cap", 0)
	if wc.GetDocumentWithCaptionMessage().GetMessage().GetDocumentMessage().GetCaption() != "cap" {
		t.Fatalf("doc caption wrap %v", wc)
	}
	// reply context attaches to both shapes
	for _, mm := range []*OutMsg{{To: "g@g.us", Image: "x", ReplyTo: "Q1"}, {To: "g@g.us", Document: "x", Caption: "c", ReplyTo: "Q1"}} {
		media := m
		if mm.Document != "" {
			media = wc
		}
		out, err := buildMessage(*mm, testStore(t), "", media)
		if err != nil || contextOf(out).GetStanzaID() != "Q1" {
			t.Fatalf("reply on media: %v %v", out, err)
		}
	}
}

func TestPdfPages(t *testing.T) {
	pdf := []byte("%PDF-1.4\n<< /Type /Pages /Count 2 >>\n<< /Type /Page >>\n<</Type/Page/Parent 1 0 R>>\n")
	if n := pdfPages(pdf); n != 2 {
		t.Fatalf("pages %d", n)
	}
	if pdfPages([]byte("not a pdf /Type /Page ")) != 0 {
		t.Fatal("non-pdf")
	}
}

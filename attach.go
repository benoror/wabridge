package main

// Image and document sending (send --image / --document, outbox "image" / "document").
// The file is uploaded byte-for-byte (never recompressed); the image gets width/height and a
// small JPEG thumbnail when decodable (best-effort), the document gets FileName/Title/Mimetype
// and, for PDFs, a best-effort PageCount.

import (
	"bytes"
	"context"
	"fmt"
	"image"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

const (
	maxImageBytes    = 16 << 20  // WhatsApp image limit
	maxDocumentBytes = 100 << 20 // WhatsApp document limit (bridge cap)
)

var extMime = map[string]string{
	".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png", ".webp": "image/webp", ".gif": "image/gif",
	".pdf": "application/pdf", ".txt": "text/plain", ".csv": "text/csv", ".xml": "application/xml",
	".json": "application/json", ".zip": "application/zip",
	".doc": "application/msword", ".xls": "application/vnd.ms-excel", ".ppt": "application/vnd.ms-powerpoint",
	".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
	".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
	".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
	".mp4":  "video/mp4", ".mp3": "audio/mpeg", ".ogg": "audio/ogg", ".m4a": "audio/mp4",
}

// mimeFor picks a mimetype by extension, then content sniffing, else octet-stream.
func mimeFor(name string, head []byte) string {
	ext := strings.ToLower(filepath.Ext(name))
	if m, ok := extMime[ext]; ok {
		return m
	}
	if m := mime.TypeByExtension(ext); m != "" {
		return strings.TrimSpace(strings.SplitN(m, ";", 2)[0])
	}
	if len(head) > 0 {
		if m := strings.SplitN(http.DetectContentType(head), ";", 2)[0]; m != "application/octet-stream" {
			return m
		}
	}
	return "application/octet-stream"
}

// imageMime returns the mimetype for an image file; only types WhatsApp shows as photos.
func imageMime(path string, head []byte) (string, error) {
	m := mimeFor(path, head)
	if sn := http.DetectContentType(head); strings.HasPrefix(sn, "image/") {
		m = sn // trust the bytes over the extension
	}
	if m != "image/jpeg" && m != "image/png" {
		return "", fmt.Errorf("image %s: unsupported type %s (send JPEG/PNG, or use --document)", filepath.Base(path), m)
	}
	return m, nil
}

// checkFile validates that path is a readable regular file no larger than max.
func checkFile(kind, path string, max int64) (int64, error) {
	st, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", kind, err)
	}
	if !st.Mode().IsRegular() {
		return 0, fmt.Errorf("%s %s: not a regular file", kind, path)
	}
	if st.Size() == 0 {
		return 0, fmt.Errorf("%s %s: empty file", kind, path)
	}
	if st.Size() > max {
		return 0, fmt.Errorf("%s %s: %.1f MB exceeds the %d MB limit", kind, filepath.Base(path), float64(st.Size())/(1<<20), max>>20)
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", kind, err)
	}
	f.Close()
	return st.Size(), nil
}

func readHead(path string) []byte {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	b := make([]byte, 512)
	n, _ := f.Read(b)
	return b[:n]
}

// validateMedia checks the attachment fields of an outbox item.
func (m OutMsg) validateMedia() error {
	n := 0
	for _, s := range []string{m.Voice, m.Image, m.Document, m.FwdID} {
		if s != "" {
			n++
		}
	}
	if n > 1 {
		return fmt.Errorf("only one of voice/image/document/forward per message")
	}
	if m.Image != "" {
		if _, err := checkFile("image", m.Image, maxImageBytes); err != nil {
			return err
		}
		if _, err := imageMime(m.Image, readHead(m.Image)); err != nil {
			return err
		}
	}
	if m.Document != "" {
		if _, err := checkFile("document", m.Document, maxDocumentBytes); err != nil {
			return err
		}
	}
	if (m.FileName != "") && m.Document == "" {
		return fmt.Errorf("file_name needs document")
	}
	if m.Caption != "" && m.Image == "" && m.Document == "" {
		return fmt.Errorf("caption needs image or document")
	}
	return nil
}

// imageMeta returns width/height and a small JPEG thumbnail (best-effort; zero/nil on failure).
func imageMeta(data []byte) (w, h uint32, thumb []byte) {
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return 0, 0, nil
	}
	w, h = uint32(cfg.Width), uint32(cfg.Height)
	if cfg.Width*cfg.Height > 50_000_000 {
		return w, h, nil // don't decode giant images just for a thumbnail
	}
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return w, h, nil
	}
	return w, h, thumbnail(img, 72)
}

// thumbnail box-downscales img to fit max px and encodes JPEG.
func thumbnail(img image.Image, max int) []byte {
	b := img.Bounds()
	sw, sh := b.Dx(), b.Dy()
	if sw == 0 || sh == 0 {
		return nil
	}
	tw, th := max, max
	if sw >= sh {
		th = sh * max / sw
	} else {
		tw = sw * max / sh
	}
	if tw < 1 {
		tw = 1
	}
	if th < 1 {
		th = 1
	}
	if sw <= tw {
		tw, th = sw, sh
	}
	out := image.NewRGBA(image.Rect(0, 0, tw, th))
	for y := 0; y < th; y++ {
		y0, y1 := b.Min.Y+y*sh/th, b.Min.Y+(y+1)*sh/th
		for x := 0; x < tw; x++ {
			x0, x1 := b.Min.X+x*sw/tw, b.Min.X+(x+1)*sw/tw
			var r, g, bl, n uint64
			// sample up to 4x4 points per cell (cheap, good enough for a preview)
			for yy := y0; yy < y1; yy += max1((y1-y0)/4, 1) {
				for xx := x0; xx < x1; xx += max1((x1-x0)/4, 1) {
					cr, cg, cb, _ := img.At(xx, yy).RGBA()
					r, g, bl, n = r+uint64(cr), g+uint64(cg), bl+uint64(cb), n+1
				}
			}
			if n == 0 {
				continue
			}
			i := out.PixOffset(x, y)
			out.Pix[i], out.Pix[i+1], out.Pix[i+2], out.Pix[i+3] = uint8(r/n>>8), uint8(g/n>>8), uint8(bl/n>>8), 255
		}
	}
	var buf bytes.Buffer
	if jpeg.Encode(&buf, out, &jpeg.Options{Quality: 60}) != nil {
		return nil
	}
	return buf.Bytes()
}

func max1(a, b int) int {
	if a > b {
		return a
	}
	return b
}

var pdfPageRe = regexp.MustCompile(`/Type\s*/Page[^s]`)

// pdfPages counts page objects (best-effort; 0 if unknown, e.g. compressed object streams).
func pdfPages(data []byte) uint32 {
	if !bytes.HasPrefix(data, []byte("%PDF")) {
		return 0
	}
	return uint32(len(pdfPageRe.FindAllIndex(data, -1)))
}

// docName is the file name shown to the recipient (default: basename of the path).
func docName(o OutMsg) string {
	if o.FileName != "" {
		return filepath.Base(o.FileName)
	}
	return filepath.Base(o.Document)
}

// uploadImage uploads o.Image unchanged and returns an ImageMessage.
func uploadImage(ctx context.Context, cl *whatsmeow.Client, o OutMsg) (*waE2E.Message, error) {
	if _, err := checkFile("image", o.Image, maxImageBytes); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(o.Image)
	if err != nil {
		return nil, err
	}
	mt, err := imageMime(o.Image, data)
	if err != nil {
		return nil, err
	}
	up, err := cl.Upload(ctx, data, whatsmeow.MediaImage)
	if err != nil {
		return nil, fmt.Errorf("upload image: %w", err)
	}
	w, h, thumb := imageMeta(data)
	return imageMessage(up, mt, o.Caption, w, h, thumb), nil
}

func imageMessage(up whatsmeow.UploadResponse, mt, caption string, w, h uint32, thumb []byte) *waE2E.Message {
	im := &waE2E.ImageMessage{
		URL: proto.String(up.URL), DirectPath: proto.String(up.DirectPath), MediaKey: up.MediaKey,
		FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: proto.Uint64(up.FileLength),
		Mimetype: proto.String(mt), JPEGThumbnail: thumb,
	}
	if caption != "" {
		im.Caption = proto.String(caption)
	}
	if w > 0 && h > 0 {
		im.Width, im.Height = proto.Uint32(w), proto.Uint32(h)
	}
	return &waE2E.Message{ImageMessage: im}
}

// uploadDocument uploads o.Document unchanged and returns a DocumentMessage (wrapped in
// DocumentWithCaptionMessage when there is a caption, as the official clients do).
func uploadDocument(ctx context.Context, cl *whatsmeow.Client, o OutMsg) (*waE2E.Message, error) {
	if _, err := checkFile("document", o.Document, maxDocumentBytes); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(o.Document)
	if err != nil {
		return nil, err
	}
	up, err := cl.Upload(ctx, data, whatsmeow.MediaDocument)
	if err != nil {
		return nil, fmt.Errorf("upload document: %w", err)
	}
	name := docName(o)
	head := data
	if len(head) > 512 {
		head = head[:512]
	}
	return documentMessage(up, name, mimeFor(name, head), o.Caption, pdfPages(data)), nil
}

func documentMessage(up whatsmeow.UploadResponse, name, mt, caption string, pages uint32) *waE2E.Message {
	dm := &waE2E.DocumentMessage{
		URL: proto.String(up.URL), DirectPath: proto.String(up.DirectPath), MediaKey: up.MediaKey,
		FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256, FileLength: proto.Uint64(up.FileLength),
		Mimetype: proto.String(mt), FileName: proto.String(name),
		Title: proto.String(strings.TrimSuffix(name, filepath.Ext(name))),
	}
	if pages > 0 {
		dm.PageCount = proto.Uint32(pages)
	}
	if caption == "" {
		return &waE2E.Message{DocumentMessage: dm}
	}
	dm.Caption = proto.String(caption)
	return &waE2E.Message{DocumentWithCaptionMessage: &waE2E.FutureProofMessage{
		Message: &waE2E.Message{DocumentMessage: dm}}}
}

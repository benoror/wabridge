package main

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/jpeg"
	"os"
	"path/filepath"
	"testing"
)

func TestPreparePhotoCropsAndShrinks(t *testing.T) {
	src := image.NewRGBA(image.Rect(0, 0, 1600, 1000))
	for y := 0; y < 1000; y++ {
		for x := 0; x < 1600; x++ {
			src.Set(x, y, color.RGBA{200, 10, 10, 255})
		}
	}
	var in bytes.Buffer
	jpeg.Encode(&in, src, nil)
	out, err := preparePhoto(in.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	im, err := jpeg.Decode(bytes.NewReader(out))
	if err != nil {
		t.Fatal(err)
	}
	if b := im.Bounds(); b.Dx() != 640 || b.Dy() != 640 {
		t.Fatalf("got %v", b)
	}
	if r, _, _, _ := im.At(320, 320).RGBA(); r>>8 < 180 {
		t.Fatalf("color lost: r=%d", r>>8)
	}
}

func TestPhotoRequestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	img := filepath.Join(dir, "a.jpg")
	var b bytes.Buffer
	jpeg.Encode(&b, image.NewRGBA(image.Rect(0, 0, 200, 200)), nil)
	os.WriteFile(img, b.Bytes(), 0o600)
	var got []byte
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go WatchPhotoRequests(ctx, photoDir(dir), func(_ context.Context, p []byte) (string, error) { got = p; return "123", nil })
	res, err := requestPhoto(dir, PhotoReq{Image: img}, 5e9)
	if err != nil || !res.OK || res.PictureID != "123" || len(got) == 0 {
		t.Fatalf("res=%+v err=%v got=%d", res, err, len(got))
	}
	res, _ = requestPhoto(dir, PhotoReq{Image: filepath.Join(dir, "missing.jpg")}, 5e9)
	if res.OK || res.Error == "" {
		t.Fatalf("missing file should fail: %+v", res)
	}
}

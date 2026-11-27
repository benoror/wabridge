package main

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/jpeg"
	_ "image/png"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Profile photo: `wabridge set-photo --profile NAME --image PATH` drops a request in
// <profile>/photo/; the running bridge (single WhatsApp session) applies it and writes
// <id>.result.json, which the CLI waits for.

// PhotoReq is a set-photo request; Image empty means remove the photo.
type PhotoReq struct {
	Image  string `json:"image"`
	Remove bool   `json:"remove,omitempty"`
}

// PhotoRes is the bridge's answer.
type PhotoRes struct {
	OK        bool   `json:"ok"`
	PictureID string `json:"picture_id,omitempty"`
	Error     string `json:"error,omitempty"`
}

const photoSide = 640 // WhatsApp profile photos are square; 640 is what the apps upload

func photoDir(dir string) string {
	d := filepath.Join(dir, "photo")
	os.MkdirAll(d, 0o700)
	return d
}

// preparePhoto center-crops to a square, downsizes to at most photoSide (area average)
// and re-encodes as JPEG, which is the only format WhatsApp accepts here.
func preparePhoto(raw []byte) ([]byte, error) { return preparePhotoSide(raw, photoSide) }

// preparePhotoSide is preparePhoto with a custom maximum side (96 for the preview).
func preparePhotoSide(raw []byte, maxSide int) ([]byte, error) {
	src, _, err := image.Decode(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	b := src.Bounds()
	side := min(b.Dx(), b.Dy())
	if side < 96 {
		return nil, fmt.Errorf("image too small (%dx%d)", b.Dx(), b.Dy())
	}
	x0, y0 := b.Min.X+(b.Dx()-side)/2, b.Min.Y+(b.Dy()-side)/2
	out := min(side, maxSide)
	dst := image.NewRGBA(image.Rect(0, 0, out, out))
	for y := 0; y < out; y++ {
		sy0, sy1 := y0+y*side/out, y0+(y+1)*side/out
		for x := 0; x < out; x++ {
			sx0, sx1 := x0+x*side/out, x0+(x+1)*side/out
			var r, g, bl, n uint64
			for sy := sy0; sy < max(sy1, sy0+1); sy++ {
				for sx := sx0; sx < max(sx1, sx0+1); sx++ {
					cr, cg, cb, _ := src.At(sx, sy).RGBA()
					r, g, bl, n = r+uint64(cr), g+uint64(cg), bl+uint64(cb), n+1
				}
			}
			i := dst.PixOffset(x, y)
			dst.Pix[i], dst.Pix[i+1], dst.Pix[i+2], dst.Pix[i+3] = uint8(r/n>>8), uint8(g/n>>8), uint8(bl/n>>8), 255
		}
	}
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, dst, &jpeg.Options{Quality: 90}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// WatchPhotoRequests applies set-photo requests with set (nil bytes = remove).
func WatchPhotoRequests(ctx context.Context, dir string, set func(context.Context, []byte) (string, error)) {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			drainPhoto(ctx, dir, set)
		}
	}
}

func drainPhoto(ctx context.Context, dir string, set func(context.Context, []byte) (string, error)) {
	names, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	sort.Strings(names)
	for _, n := range names {
		if strings.HasSuffix(n, ".result.json") {
			continue
		}
		var r PhotoReq
		err := readJSON(n, &r)
		os.Remove(n)
		res := applyPhoto(ctx, r, err, set)
		atomicWrite(strings.TrimSuffix(n, ".json")+".result.json", res)
		fmt.Fprintf(os.Stderr, "%s set-photo: ok=%v id=%s err=%s\n", time.Now().Format("15:04:05"), res.OK, res.PictureID, res.Error)
	}
}

func applyPhoto(ctx context.Context, r PhotoReq, readErr error, set func(context.Context, []byte) (string, error)) PhotoRes {
	if readErr != nil {
		return PhotoRes{Error: "bad request: " + readErr.Error()}
	}
	var img []byte
	if !r.Remove {
		raw, err := os.ReadFile(r.Image)
		if err != nil {
			return PhotoRes{Error: err.Error()}
		}
		if img, err = preparePhoto(raw); err != nil {
			return PhotoRes{Error: err.Error()}
		}
	}
	c, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	id, err := set(c, img)
	if err != nil {
		return PhotoRes{Error: err.Error()}
	}
	return PhotoRes{OK: true, PictureID: id}
}

// requestPhoto is the CLI side: drop the request and wait for the bridge's result.
func requestPhoto(dir string, r PhotoReq, wait time.Duration) (PhotoRes, error) {
	pd := photoDir(dir)
	base := filepath.Join(pd, strconv.FormatInt(time.Now().UnixNano(), 36))
	if err := atomicWrite(base+".json", r); err != nil {
		return PhotoRes{}, err
	}
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		var res PhotoRes
		if err := readJSON(base+".result.json", &res); err == nil {
			os.Remove(base + ".result.json")
			return res, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	if _, err := os.Stat(base + ".json"); err == nil {
		os.Remove(base + ".json")
		return PhotoRes{}, fmt.Errorf("bridge did not pick up the request (is it running a build with set-photo?)")
	}
	return PhotoRes{}, fmt.Errorf("bridge took the request but no result within %s; check its log", wait)
}

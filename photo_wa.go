package main

import (
	"context"
	"errors"
	"fmt"

	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

// setOwnPhoto sets (img != nil) or removes the account's own profile photo. whatsmeow only has
// SetGroupPhoto, whose Target attribute the server ignores for your own JID (it times out), so
// this sends the IQ the way WhatsApp Web does: to s.whatsapp.net with no target, carrying the
// 640px image plus a 96px preview.
func setOwnPhoto(ctx context.Context, cl *whatsmeow.Client, img []byte) (string, error) {
	var content any
	if img != nil {
		prev, err := preparePhotoSide(img, 96)
		if err != nil {
			return "", fmt.Errorf("preview: %w", err)
		}
		content = []waBinary.Node{
			{Tag: "picture", Attrs: waBinary.Attrs{"type": "image"}, Content: img},
			{Tag: "picture", Attrs: waBinary.Attrs{"type": "preview"}, Content: prev},
		}
	}
	resp, err := cl.DangerousInternals().SendIQ(ctx, whatsmeow.DangerousInfoQuery{
		Namespace: "w:profile:picture",
		Type:      "set",
		To:        types.ServerJID,
		Content:   content,
	})
	if errors.Is(err, whatsmeow.ErrIQNotAcceptable) {
		return "", fmt.Errorf("WhatsApp rejected the image format: %w", err)
	} else if err != nil {
		return "", err
	}
	if img == nil {
		return "remove", nil
	}
	if id, ok := resp.GetChildByTag("picture").Attrs["id"].(string); ok {
		return id, nil
	}
	return "set", nil
}

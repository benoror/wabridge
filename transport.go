package main

import (
	"sync/atomic"

	waLog "go.mau.fi/whatsmeow/util/log"
)

// Frame-level transport signal.
//
// whatsmeow has no public per-frame callback, but Client.handleFrame logs every
// successfully decoded inbound frame (messages, receipts, IQ responses including
// keepalive pongs, acks, stream end) via cli.recvLog.Debugf, where recvLog is
// log.Sub("Recv") taken in NewClient. tapLogger hands back a "Recv" sub-logger
// whose Debugf only bumps the transport timestamp and DISCARDS the line (it would
// otherwise contain message XML), so this is a real read-side frame signal with no
// content ever logged. Everything else delegates to the wrapped logger.
var frameHook atomic.Pointer[func()]

func onFrame() {
	if f := frameHook.Load(); f != nil {
		(*f)()
	}
}

type tapLogger struct{ waLog.Logger }

func (t tapLogger) Sub(m string) waLog.Logger {
	if m == "Recv" {
		return recvTap{t.Logger.Sub(m)}
	}
	return tapLogger{t.Logger.Sub(m)}
}

type recvTap struct{ base waLog.Logger }

func (r recvTap) Debugf(string, ...any)     { onFrame() }
func (r recvTap) Infof(string, ...any)      {}
func (r recvTap) Warnf(m string, a ...any)  { r.base.Warnf(m, a...) }
func (r recvTap) Errorf(m string, a ...any) { r.base.Errorf(m, a...) }
func (r recvTap) Sub(m string) waLog.Logger { return recvTap{r.base.Sub(m)} }

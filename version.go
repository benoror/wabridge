package main

import "runtime/debug"

// version is set by release builds (-ldflags "-X main.version=vX.Y.Z"). A `go install
// github.com/benoror/wabridge@vX.Y.Z` build reports the module version instead.
var version = ""

func versionString() string {
	if version != "" {
		return version
	}
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return bi.Main.Version
	}
	return "dev"
}

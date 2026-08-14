// Package releaseinfo exposes provenance injected by the trusted release build.
//
// Go does not emit vcs.* settings for this repository layout because the only
// Go module intentionally lives below the Git repository root. The release
// builder therefore binds the monorepo revision both to the Go build ID and to
// these linker variables.
package releaseinfo

import "strings"

var (
	revision = ""
	clean    = "false"
)

// Revision returns the exact clean monorepo revision, or false for development
// binaries and malformed release builds.
func Revision() (string, bool) {
	if clean != "true" || len(revision) != 40 || strings.IndexFunc(revision, func(current rune) bool {
		return !('0' <= current && current <= '9') &&
			!('a' <= current && current <= 'f')
	}) >= 0 {
		return "", false
	}
	return revision, true
}

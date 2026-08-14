package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/YouToco/vane/server/internal/testgate"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: testpolicyscan ABSOLUTE_SERVER_ROOT")
		os.Exit(2)
	}
	root, err := filepath.Abs(os.Args[1])
	if err != nil || root != filepath.Clean(os.Args[1]) {
		fmt.Fprintln(os.Stderr, "test policy root must be an exact absolute path")
		os.Exit(78)
	}
	violations, err := testgate.Scan(root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "test policy scan failed: %v\n", err)
		os.Exit(78)
	}
	if len(violations) != 0 {
		for _, violation := range violations {
			fmt.Fprintln(os.Stderr, violation)
		}
		os.Exit(78)
	}
	allowlist := filepath.Join(filepath.Dir(root), "tools", "testpolicy", "skip-allowlist.json")
	if err := testgate.ValidateAllowlist(allowlist, time.Now()); err != nil {
		fmt.Fprintf(os.Stderr, "test skip allowlist is invalid: %v\n", err)
		os.Exit(78)
	}
}

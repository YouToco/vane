package main

import (
	"os"
	"strings"
	"testing"
)

func TestActivatedListenerRequiresExactlyOneSystemdSocket(t *testing.T) {
	t.Setenv("LISTEN_PID", "0")
	t.Setenv("LISTEN_FDS", "0")
	listener, err := activatedListener()
	if listener != nil {
		listener.Close()
		t.Fatal("listener unexpectedly created without systemd activation")
	}
	if err == nil || !strings.Contains(err.Error(), "exactly one systemd socket") {
		t.Fatalf("activatedListener error=%v", err)
	}

	t.Setenv("LISTEN_PID", "not-"+strings.Repeat("1", len(os.Getenv("LISTEN_PID"))))
	if _, err := activatedListener(); err == nil {
		t.Fatal("malformed LISTEN_PID unexpectedly accepted")
	}
}

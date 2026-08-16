//go:build linux

package sandbox

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLinuxSOPEERCREDAllowsCurrentUIDAndRejectsWrongUID(t *testing.T) {
	currentUID := uint32(os.Geteuid())
	testLinuxPeerCredential(t, currentUID, true)
	wrongUID := currentUID + 1
	if wrongUID == currentUID {
		wrongUID = currentUID - 1
	}
	testLinuxPeerCredential(t, wrongUID, false)
}

func TestLinuxNetNSRejectsCurrentNamespace(t *testing.T) {
	if err := inspectNetworkNamespace("/proc/self/ns/net"); err == nil ||
		!strings.Contains(err.Error(), "current host namespace") {
		t.Fatalf("current namespace mutation err=%v", err)
	}
}

func testLinuxPeerCredential(t *testing.T, approvedUID uint32, wantAuthorized bool) {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	uid, gid, ok := fileOwner(info)
	if !ok {
		t.Fatal("socket parent owner unavailable")
	}
	request := testRequest(t, "real-peer-credential", 17, 23)
	service, err := NewService(ServiceConfig{MaxInputBytes: 1024,
		AllowedPolicyDigests: map[string]struct{}{request.PolicyDigest: {}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	socket := filepath.Join(directory, "sandboxd.sock")
	daemon := Daemon{SocketPath: socket, Socket: SocketContract{
		ParentUID: uid, ParentGID: gid, ParentMode: 0o700,
		SocketUID: os.Geteuid(), SocketGID: os.Getegid(), SocketMode: 0o660,
		allowUnprivilegedForTest: true,
	}, Service: service, Authorizer: UIDAuthorizer{UID: approvedUID}}
	go func() { done <- daemon.Serve(ctx) }()
	waitForSocket(t, socket)
	result, executeErr := (Client{SocketPath: socket}).Execute(t.Context(), request)
	if wantAuthorized {
		if executeErr == nil || executeErr.Error() != "dark_foundation" || result.Status != "disabled" {
			t.Fatalf("real current-UID peer result=%+v err=%v", result, executeErr)
		}
	} else if executeErr == nil || executeErr.Error() == "dark_foundation" {
		t.Fatalf("wrong-UID peer was authorized result=%+v err=%v", result, executeErr)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

//go:build linux

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExecLauncherTimeoutKillsForkedProcessGroup(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "escaped-child")
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := (execLauncher{}).Run(ctx, LaunchPlan{Executable: "/bin/sh",
		Arguments: []string{"-c", fmt.Sprintf("(sleep 0.4; touch %q) & wait", marker)},
		WorkDir:   t.TempDir(), OutputLimit: 1024})
	if err == nil || time.Since(started) > 2*time.Second {
		t.Fatalf("fork timeout did not terminate promptly err=%v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("fork escaped process-group kill: %v", err)
	}
}

func TestExecLauncherOutputCapKillsProducer(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, err := (execLauncher{}).Run(ctx, LaunchPlan{Executable: "/bin/sh",
		Arguments: []string{"-c", "while :; do printf 0123456789; done"},
		WorkDir:   t.TempDir(), OutputLimit: 128})
	if !errors.Is(err, ErrOutputLimit) {
		t.Fatalf("unbounded producer err=%v", err)
	}
}

func TestVersionProbeHasTimeoutAndOutputCap(t *testing.T) {
	directory := t.TempDir()
	hanging := filepath.Join(directory, "hanging-version")
	if err := os.WriteFile(hanging, []byte("#!/bin/sh\nsleep 30\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if err := verifyVersion(ctx, hanging, "Firecracker", "v1.15.1"); err == nil ||
		!strings.Contains(err.Error(), "timed out") {
		t.Fatalf("hanging version probe err=%v", err)
	}
	large := filepath.Join(directory, "large-version")
	if err := os.WriteFile(large, []byte("#!/bin/sh\nwhile :; do printf 0123456789; done\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := verifyVersion(t.Context(), large, "Firecracker", "v1.15.1"); !errors.Is(err, ErrOutputLimit) {
		t.Fatalf("unbounded version output err=%v", err)
	}
}

func TestProductionArtifactRejectsDynamicELF(t *testing.T) {
	if err := verifyStaticELF("/bin/sh"); err == nil {
		t.Fatal("dynamic system shell accepted as static Firecracker artifact")
	}
}

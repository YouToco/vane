package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPreparedRetentionControlPrimesThenDrainsBeforeEvidence(t *testing.T) {
	payload, err := os.ReadFile("../../deploy/agent-first-retention-prepared-control.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(payload)
	for _, exact := range []string{
		"exec 8>/run/lock/vane-backend-control-plane.lock",
		"flock 8",
		`"$collector" "$phase"`,
		`--operation-id "$operation_id"`,
		`$(readlink -f "$0") == "$control"`,
		"run_collector prime-clock \"$prime_output\"",
		"stop_attempted=true",
		"systemctl stop vane.service",
		"systemctl mask --runtime vane.service",
		"restart_authorized=true",
		"run_collector prepared \"$prepared_output\"",
		"unsafe drain left vane stopped; operator recovery is required",
		"/opt/vane/bin/gate -env /opt/vane/env/server.env",
	} {
		if strings.Count(script, exact) != 1 {
			t.Fatalf("prepared control exact fragment count differs: %q", exact)
		}
	}
	prime := strings.Index(script, "run_collector prime-clock")
	stopAttempted := strings.Index(script, "stop_attempted=true")
	stop := strings.Index(script, "systemctl stop vane.service")
	mask := strings.Index(script, "systemctl mask --runtime vane.service")
	restart := strings.Index(script, "restart_authorized=true")
	prepared := strings.Index(script, "run_collector prepared")
	if !(prime < stopAttempted && stopAttempted < stop && stop < mask &&
		mask < restart && restart < prepared) {
		t.Fatalf("unsafe prepared control order prime=%d attempted=%d stop=%d mask=%d restart=%d prepared=%d",
			prime, stopAttempted, stop, mask, restart, prepared)
	}
}

func TestPreparedRetentionControlReportsStopThatCompletedWithError(t *testing.T) {
	payload, err := os.ReadFile("../../deploy/agent-first-retention-prepared-control.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(payload)
	start := strings.Index(script, "service_stopped=false")
	endMarker := "trap cleanup EXIT"
	end := strings.Index(script[start:], endMarker)
	if start < 0 || end < 0 {
		t.Fatal("prepared cleanup state machine is unavailable")
	}
	cleanupStateMachine := script[start : start+end]
	temp := t.TempDir()
	prime := filepath.Join(temp, "prime")
	prepared := filepath.Join(temp, "prepared")
	command := "set -u\n" +
		"prime_output=" + shellSingleQuoteForTest(prime) + "\n" +
		"prepared_output=" + shellSingleQuoteForTest(prepared) + "\n" +
		"systemctl() {\n" +
		"  printf '%s\\n' \"$*\" >>\"$CONTROL_LOG\"\n" +
		"  if [[ $1 == is-active ]]; then printf 'inactive\\n'; return 3; fi\n" +
		"  return 0\n" +
		"}\n" + cleanupStateMachine + "\n" +
		"stop_attempted=true\n" +
		"restart_authorized=false\n" +
		"set +e\n(exit 23)\ncleanup\n"
	logPath := filepath.Join(temp, "systemctl.log")
	cmd := exec.Command("bash")
	cmd.Env = append(os.Environ(), "CONTROL_LOG="+logPath)
	cmd.Stdin = strings.NewReader(command)
	output, err := cmd.CombinedOutput()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() == 0 {
		t.Fatalf("cleanup status=%v output=%s", err, output)
	}
	if !strings.Contains(string(output),
		"unsafe drain left vane stopped; operator recovery is required") {
		t.Fatalf("stop-lost response was not surfaced: %s", output)
	}
	log, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(log), "start vane.service") {
		t.Fatalf("unproven drain restarted service: %s", log)
	}
}

func shellSingleQuoteForTest(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

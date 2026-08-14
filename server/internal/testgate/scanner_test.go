package testgate

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCapabilityGateModes(t *testing.T) {
	tests := []struct {
		name       string
		full       bool
		wantExitOK bool
		wantOutput string
	}{
		{name: "quick skips", wantExitOK: true, wantOutput: "SKIP"},
		{name: "full fails", full: true, wantExitOK: false, wantOutput: "full gate requires capability unavailable [database]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := exec.Command(os.Args[0], "-test.v", "-test.run=^TestCapabilityGateSubprocess$")
			command.Env = append(os.Environ(), "VANE_TESTGATE_SUBPROCESS=1", "VANE_FULL_GATE=0")
			if test.full {
				command.Env[len(command.Env)-1] = "VANE_FULL_GATE=1"
			}
			output, err := command.CombinedOutput()
			if (err == nil) != test.wantExitOK {
				t.Fatalf("exit error=%v output=%s", err, output)
			}
			if !strings.Contains(string(output), test.wantOutput) {
				t.Fatalf("output=%s, want substring %q", output, test.wantOutput)
			}
		})
	}
}

func TestCapabilityGateSubprocess(t *testing.T) {
	if os.Getenv("VANE_TESTGATE_SUBPROCESS") != "1" {
		return
	}
	Database(t)
	t.Fatal("capability gate unexpectedly returned")
}

func TestScannerMutationMatrix(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   int
	}{
		{name: "direct", source: `package sample; import "testing"; func TestX(t *testing.T) { t.Skip("x") }`, want: 1},
		{name: "import alias", source: `package sample; import tt "testing"; func TestX(t *tt.T) { t.Skipf("%s", "x") }`, want: 1},
		{name: "type alias", source: `package sample; import "testing"; type testT = testing.T; func TestX(t *testT) { t.SkipNow() }`, want: 1},
		{name: "promoted method", source: `package sample; import "testing"; type wrapped struct{ *testing.T }; func TestX(t *testing.T) { w := wrapped{t}; w.Skip("x") }`, want: 1},
		{name: "method expression", source: `package sample; import "testing"; func TestX(t *testing.T) { (*testing.T).Skip(t, "x") }`, want: 1},
		{name: "method value", source: `package sample; import "testing"; func TestX(t *testing.T) { skip := t.Skip; skip("x") }`, want: 1},
		{name: "package method expression", source: `package sample; import "testing"; var skip = (*testing.T).Skip`, want: 1},
		{name: "benchmark", source: `package sample; import "testing"; func BenchmarkX(b *testing.B) { b.SkipNow() }`, want: 1},
		{name: "fuzz", source: `package sample; import "testing"; func FuzzX(f *testing.F) { f.Skip("x") }`, want: 1},
		{name: "custom method is safe", source: `package sample; type fake struct{}; func (fake) Skip(...any) {}; func f() { fake{}.Skip("x") }`, want: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "sample_test.go"), []byte(test.source), 0o600); err != nil {
				t.Fatal(err)
			}
			violations, err := Scan(root)
			if err != nil {
				t.Fatal(err)
			}
			if len(violations) != test.want {
				t.Fatalf("violations=%v, want %d", violations, test.want)
			}
		})
	}
}

func TestScannerDoesNotPermitExtraSkipInSealedFile(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "internal", "testgate")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	source := `package testgate
import "testing"
func unavailable(t testing.TB) { t.Skip("sealed") }
func backdoor(t testing.TB) { t.Skip("not sealed") }
`
	if err := os.WriteFile(filepath.Join(dir, "capability.go"), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	violations, err := Scan(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 1 || violations[0].Method != "Skip" {
		t.Fatalf("violations=%v, want only backdoor", violations)
	}
}

func TestSkipAllowlistValidationMutations(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "allowlist.json")
	now := time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		content string
		wantErr string
	}{
		{name: "empty", content: `{"schema":"vane.test-skip-allowlist/v1","entries":[]}`},
		{name: "unknown field", content: `{"schema":"vane.test-skip-allowlist/v1","entries":[],"escape":true}`, wantErr: "unknown field"},
		{name: "trailing object", content: `{"schema":"vane.test-skip-allowlist/v1","entries":[]} {}`, wantErr: "trailing JSON value"},
		{name: "nil entries", content: `{"schema":"vane.test-skip-allowlist/v1","entries":null}`, wantErr: "must be an array"},
		{name: "expired", content: `{"schema":"vane.test-skip-allowlist/v1","entries":[{"file":"server/x_test.go","test":"TestX","owner":"team","reason":"issue","expires":"2026-08-13"}]}`, wantErr: "expired"},
		{name: "escape", content: `{"schema":"vane.test-skip-allowlist/v1","entries":[{"file":"../x_test.go","test":"TestX","owner":"team","reason":"issue","expires":"2026-08-14"}]}`, wantErr: "escapes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(test.content), 0o600); err != nil {
				t.Fatal(err)
			}
			err := ValidateAllowlist(path, now)
			if test.wantErr == "" && err != nil {
				t.Fatal(err)
			}
			if test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
				t.Fatalf("error=%v, want substring %q", err, test.wantErr)
			}
		})
	}
}

func TestRepositoryHasNoDirectTestingSkips(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	violations, err := Scan(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		var lines []string
		for _, violation := range violations {
			lines = append(lines, violation.String())
		}
		t.Fatalf("direct test skips found:\n%s", strings.Join(lines, "\n"))
	}
}

func TestSkipAllowlistIsValid(t *testing.T) {
	path := filepath.Clean(filepath.Join("..", "..", "..", "tools", "testpolicy", "skip-allowlist.json"))
	if err := ValidateAllowlist(path, time.Now()); err != nil {
		t.Fatal(err)
	}
}

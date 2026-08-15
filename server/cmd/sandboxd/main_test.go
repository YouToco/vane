package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigRequiresPermanentDarkMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sandboxd.json")
	payload := `{"mode":"dark","feature_enabled":false,"socket_path":"/run/vane-sandbox/sandboxd.sock",
      "socket_parent_uid":0,"socket_parent_gid":1,"socket_parent_mode":488,
      "socket_uid":0,"socket_gid":1,"socket_mode":432,
      "vane_server_uid":1001,"max_input_bytes":1024,
      "allowed_policy_sha256":["` + strings.Repeat("a", 64) + `"],
      "firecracker":{"production":true,"isolation_slots":1,
        "jailer_uid_start":20000,"jailer_gid_start":20000}}`
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	configuration, err := loadConfig(path, false)
	if err != nil || configuration.FeatureEnabled || configuration.Mode != "dark" {
		t.Fatalf("dark config=%+v err=%v", configuration, err)
	}
	enabled := strings.Replace(payload, `"feature_enabled":false`, `"feature_enabled":true`, 1)
	if err := os.WriteFile(path, []byte(enabled), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path, false); err == nil {
		t.Fatal("feature-enabled sandboxd config accepted")
	}
	uppercase := strings.Replace(payload, strings.Repeat("a", 64), strings.Repeat("A", 64), 1)
	if err := os.WriteFile(path, []byte(uppercase), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path, false); err == nil {
		t.Fatal("uppercase policy digest accepted")
	}
}

func TestRunHasNoImplicitCommandOrPortableSuccess(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(t.Context(), nil, &stdout, &stderr); code != 2 {
		t.Fatalf("empty sandboxd command code=%d stderr=%q", code, stderr.String())
	}
}

func TestConfigFileContractRejectsSymlinkHardlinkAndLooseMode(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "sandboxd.json")
	if err := os.WriteFile(path, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyConfigPath(path, false); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(directory, "sandboxd-link.json")
	if err := os.Symlink(path, symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyConfigPath(symlink, false); err == nil {
		t.Fatal("symlink config accepted")
	}
	hardlink := filepath.Join(directory, "sandboxd-hard.json")
	if err := os.Link(path, hardlink); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyConfigPath(path, false); err == nil {
		t.Fatal("hard-linked config accepted")
	}
	if err := os.Remove(hardlink); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyConfigPath(path, false); err == nil {
		t.Fatal("loosely readable config accepted")
	}
}

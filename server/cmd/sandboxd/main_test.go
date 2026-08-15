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
      "max_connections":16,"max_wire_bytes":262144,
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
	for name, mutation := range map[string]string{
		"mode":       strings.Replace(payload, `"mode":"dark"`, `"mode":"live"`, 1),
		"enabled":    strings.Replace(payload, `"feature_enabled":false`, `"feature_enabled":true`, 1),
		"production": strings.Replace(payload, `"production":true`, `"production":false`, 1),
	} {
		if err := os.WriteFile(path, []byte(mutation), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadConfig(path, false); err == nil {
			t.Fatalf("%s sandboxd mutation accepted", name)
		}
	}
	uppercase := strings.Replace(payload, strings.Repeat("a", 64), strings.Repeat("A", 64), 1)
	if err := os.WriteFile(path, []byte(uppercase), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path, false); err == nil {
		t.Fatal("uppercase policy digest accepted")
	}
}

func TestConfigRejectsDaemonResourceLimitMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sandboxd.json")
	payload := `{"mode":"dark","feature_enabled":false,"socket_path":"/run/vane-sandbox/sandboxd.sock",
      "socket_parent_uid":0,"socket_parent_gid":1,"socket_parent_mode":488,
      "socket_uid":0,"socket_gid":1,"socket_mode":432,"vane_server_uid":1001,
      "max_input_bytes":1024,"max_connections":16,"max_wire_bytes":262144,
      "allowed_policy_sha256":["` + strings.Repeat("a", 64) + `"],
      "firecracker":{"production":true,"isolation_slots":1,
        "jailer_uid_start":20000,"jailer_gid_start":20000}}`
	for name, mutation := range map[string]string{
		"zero-connections": strings.Replace(payload, `"max_connections":16`, `"max_connections":0`, 1),
		"too-many":         strings.Replace(payload, `"max_connections":16`, `"max_connections":65`, 1),
		"wire-too-small":   strings.Replace(payload, `"max_wire_bytes":262144`, `"max_wire_bytes":4095`, 1),
		"wire-too-large":   strings.Replace(payload, `"max_wire_bytes":262144`, `"max_wire_bytes":262145`, 1),
		"input-over-wire":  strings.Replace(payload, `"max_input_bytes":1024`, `"max_input_bytes":262145`, 1),
		"input-envelope":   strings.Replace(payload, `"max_input_bytes":1024`, `"max_input_bytes":65537`, 1),
	} {
		if err := os.WriteFile(path, []byte(mutation), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadConfig(path, false); err == nil {
			t.Fatalf("daemon resource mutation %s accepted", name)
		}
	}
}

func TestConfigRejectsDuplicateKeysAndOversizedWhitespace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sandboxd.json")
	duplicate := `{"mode":"dark","mode":"dark"}`
	if err := os.WriteFile(path, []byte(duplicate), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path, false); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate security key err=%v", err)
	}
	oversized := `{"mode":"dark"}` + strings.Repeat(" ", maxConfigBytes)
	if err := os.WriteFile(path, []byte(oversized), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path, false); err == nil || !strings.Contains(err.Error(), "size") {
		t.Fatalf("oversized whitespace config err=%v", err)
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

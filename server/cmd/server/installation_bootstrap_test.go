package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/YouToco/vane/server/config"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

func TestInstallationTokenFileIsSecretAndRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bootstrap-token")
	token, err := generateInstallationToken()
	if err != nil {
		t.Fatal(err)
	}
	if len(token) < 40 || strings.ContainsAny(token, "= \t\r\n") {
		t.Fatalf("unexpected token encoding: len=%d", len(token))
	}
	if err := writeInstallationTokenExclusive(path, token); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("token file mode=%o want=600", info.Mode().Perm())
	}
	got, err := readInstallationToken(path)
	if err != nil || got != token {
		t.Fatalf("read token=%q err=%v", got, err)
	}
	if err := writeInstallationTokenExclusive(path, token); !os.IsExist(err) {
		t.Fatalf("exclusive create must reject overwrite, got %v", err)
	}

	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readInstallationToken(link); err == nil {
		t.Fatal("symlink token path must be rejected")
	}
}

func TestInstallationTokenPathPrecedence(t *testing.T) {
	t.Setenv("STATE_DIRECTORY", "/tmp/vane-state:/tmp/ignored")
	t.Setenv("VANE_SETUP_TOKEN_FILE", "")
	if got := installationTokenPath(); got != "/tmp/vane-state/bootstrap-token" {
		t.Fatalf("systemd state path=%q", got)
	}
	t.Setenv("VANE_SETUP_TOKEN_FILE", "/tmp/explicit-token")
	if got := installationTokenPath(); got != "/tmp/explicit-token" {
		t.Fatalf("explicit path=%q", got)
	}
}

func TestInstallationTokenFileValidationReplacementAndWait(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bootstrap-token")
	if _, err := readInstallationToken(path); !os.IsNotExist(err) {
		t.Fatalf("missing token error=%v", err)
	}
	if err := os.WriteFile(path, []byte("short\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readInstallationToken(path); err == nil {
		t.Fatal("short token must be rejected")
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readInstallationToken(path); err == nil {
		t.Fatal("world-readable token must be rejected")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(strings.Repeat("x", 513)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readInstallationToken(path); err == nil {
		t.Fatal("oversized token must be rejected")
	}

	first := strings.Repeat("a", 43)
	second := strings.Repeat("b", 43)
	if err := os.WriteFile(path, []byte(first+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := replaceInstallationToken(path, second); err != nil {
		t.Fatal(err)
	}
	if got, err := readInstallationToken(path); err != nil || got != second {
		t.Fatalf("replacement=%q err=%v", got, err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if waitInstallationRotation(canceled, time.Hour) {
		t.Fatal("canceled rotation wait must stop")
	}
	if !waitInstallationRotation(context.Background(), time.Millisecond) {
		t.Fatal("elapsed rotation wait must continue")
	}
	blockedParent := filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(blockedParent, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeInstallationTokenExclusive(
		filepath.Join(blockedParent, "token"), second); err == nil {
		t.Fatal("token create below a regular file must fail")
	}
	if err := replaceInstallationToken(filepath.Join(dir, "missing", "token"), second); err == nil {
		t.Fatal("replacement in a missing directory must fail")
	}
	directoryTarget := filepath.Join(dir, "directory-target")
	if err := os.Mkdir(directoryTarget, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := replaceInstallationToken(directoryTarget, second); err == nil {
		t.Fatal("replacement must not overwrite a directory")
	}
}

func TestEnsureInstallationBootstrapRotatesAndClosesAfterOwner(t *testing.T) {
	databaseURL := installationBootstrapScratchDatabase(t)
	ctx := t.Context()
	if err := store.Migrate(ctx, databaseURL); err != nil {
		t.Fatal(err)
	}
	st, err := store.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	direct, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(direct.Close)
	path := filepath.Join(t.TempDir(), "bootstrap-token")

	prepared, err := ensureInstallationBootstrap(ctx, st, path)
	if err != nil || !prepared.SetupRequired || !prepared.TokenAccepted {
		t.Fatalf("initial prepare=%+v err=%v", prepared, err)
	}
	first, err := readInstallationToken(path)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := ensureInstallationBootstrap(ctx, st, path)
	if err != nil || !replayed.TokenAccepted || !replayed.ExpiresAt.Equal(prepared.ExpiresAt) {
		t.Fatalf("replay=%+v err=%v", replayed, err)
	}
	if _, err := direct.Exec(ctx, `UPDATE settings
		SET value=jsonb_set(value::jsonb, '{expires_at}', to_jsonb($2::text))
		WHERE key=$1`, "installation_bootstrap_v1",
		time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	rotated, err := ensureInstallationBootstrap(ctx, st, path)
	if err != nil || !rotated.TokenAccepted || !rotated.ExpiresAt.After(time.Now()) {
		t.Fatalf("rotated=%+v err=%v", rotated, err)
	}
	second, err := readInstallationToken(path)
	if err != nil || second == first {
		t.Fatalf("token did not rotate: first=%q second=%q err=%v", first, second, err)
	}
	waitCtx, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	rotateInstallationTokenUntilClaimed(waitCtx, st, path)

	badPath := filepath.Join(t.TempDir(), "bad-token")
	if err := os.WriteFile(badPath, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	retryCtx, cancelRetry := context.WithCancel(context.Background())
	cancelRetry()
	rotateInstallationTokenUntilClaimed(retryCtx, st, badPath)

	owner, err := st.UpsertUserByOpenID(ctx,
		fmt.Sprintf("bootstrap-owner-%d", time.Now().UnixNano()), "owner")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.AddMembership(ctx, int64(types.SingleTenantID), owner.ID,
		types.MembershipRoleOwner); err != nil {
		t.Fatal(err)
	}
	closed, err := ensureInstallationBootstrap(ctx, st, path)
	if err != nil || closed.SetupRequired {
		t.Fatalf("installed prepare=%+v err=%v", closed, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("consumed host token still exists: %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	rotateInstallationTokenUntilClaimed(canceled, st, path)
	cfg := &config.Config{}
	cfg.Server.Addr = "127.0.0.1:0"
	if err := runInstallationSetupMode(canceled, cfg, st); err != nil {
		t.Fatalf("canceled setup server=%v", err)
	}
}

func TestRunInstallationSetupModeClaimsAndRestarts(t *testing.T) {
	databaseURL := installationBootstrapScratchDatabase(t)
	ctx := t.Context()
	if err := store.Migrate(ctx, databaseURL); err != nil {
		t.Fatal(err)
	}
	st, err := store.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	tokenPath := filepath.Join(t.TempDir(), "bootstrap-token")
	t.Setenv("VANE_SETUP_TOKEN_FILE", tokenPath)
	if _, err := ensureInstallationBootstrap(ctx, st, tokenPath); err != nil {
		t.Fatal(err)
	}
	token, err := readInstallationToken(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	cfg := &config.Config{}
	cfg.Server.Addr = addr
	result := make(chan error, 1)
	go func() { result <- runInstallationSetupMode(ctx, cfg, st) }()
	client := &http.Client{Timeout: 2 * time.Second}
	baseURL := "http://" + addr
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, requestErr := client.Get(baseURL + "/api/setup/status")
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("setup server did not become ready: %v", requestErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	payload, _ := json.Marshal(map[string]string{
		"token": token, "email": "first-owner@example.com",
		"password": "first-owner-password",
	})
	response, err := client.Post(baseURL+"/api/setup/claim", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("claim status=%d body=%s", response.StatusCode, raw)
	}
	select {
	case err := <-result:
		if !errors.Is(err, errInstallationClaimed) {
			t.Fatalf("setup mode result=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("setup mode did not request restart")
	}
	if required, err := st.InstallationSetupRequired(ctx); err != nil || required {
		t.Fatalf("claim did not install owner: required=%v err=%v", required, err)
	}
}

func TestRunInstallationSetupModeReturnsListenFailure(t *testing.T) {
	databaseURL := installationBootstrapScratchDatabase(t)
	ctx := t.Context()
	if err := store.Migrate(ctx, databaseURL); err != nil {
		t.Fatal(err)
	}
	st, err := store.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	t.Setenv("VANE_SETUP_TOKEN_FILE", filepath.Join(t.TempDir(), "bootstrap-token"))
	cfg := &config.Config{}
	cfg.Server.Addr = "127.0.0.1:-1"
	if err := runInstallationSetupMode(ctx, cfg, st); err == nil {
		t.Fatal("invalid listen address must fail")
	}
}

func TestRunFirstInstallStartsBeforeExternalRuntime(t *testing.T) {
	databaseURL := installationBootstrapScratchDatabase(t)
	if err := store.Migrate(t.Context(), databaseURL); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VANE_DB_URL", databaseURL)
	t.Setenv("VANE_SERVER_ADDR", "127.0.0.1:-1")
	t.Setenv("VANE_SETUP_TOKEN_FILE", filepath.Join(t.TempDir(), "bootstrap-token"))
	err := run()
	if err == nil || !strings.Contains(err.Error(), "invalid port") {
		t.Fatalf("first-install run error=%v", err)
	}
	for _, forbidden := range []string{"Owner Agent", "Temporal", "LLM", "Telegram", "飞书"} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("external runtime started before first owner: %v", err)
		}
	}
}

func TestPrintInstallationTokenAndDefaultPath(t *testing.T) {
	t.Setenv("VANE_SETUP_TOKEN_FILE", "")
	t.Setenv("STATE_DIRECTORY", "")
	if got := installationTokenPath(); got != defaultInstallationTokenPath {
		t.Fatalf("default path=%q", got)
	}
	path := filepath.Join(t.TempDir(), "bootstrap-token")
	token := strings.Repeat("c", 43)
	if err := writeInstallationTokenExclusive(path, token); err != nil {
		t.Fatal(err)
	}
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	original := os.Stdout
	os.Stdout = writeEnd
	err = printInstallationToken(path)
	os.Stdout = original
	_ = writeEnd.Close()
	raw, readErr := io.ReadAll(readEnd)
	_ = readEnd.Close()
	if err != nil || readErr != nil || string(raw) != token+"\n" {
		t.Fatalf("printed=%q err=%v readErr=%v", raw, err, readErr)
	}
	if err := printInstallationToken(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("missing setup token must fail")
	}
}

func TestMainPrintsSetupTokenWithoutStartingServer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bootstrap-token")
	token := strings.Repeat("d", 43)
	if err := writeInstallationTokenExclusive(path, token); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VANE_SETUP_TOKEN_FILE", path)
	originalArgs := os.Args
	originalStdout := os.Stdout
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Args = []string{originalArgs[0], "setup-token"}
	os.Stdout = writeEnd
	main()
	os.Args = originalArgs
	os.Stdout = originalStdout
	_ = writeEnd.Close()
	raw, readErr := io.ReadAll(readEnd)
	_ = readEnd.Close()
	if readErr != nil || string(raw) != token+"\n" {
		t.Fatalf("main output=%q readErr=%v", raw, readErr)
	}
}

func TestRunInstalledInstanceReachesOwnerRuntimeGate(t *testing.T) {
	databaseURL := installationBootstrapScratchDatabase(t)
	ctx := t.Context()
	if err := store.Migrate(ctx, databaseURL); err != nil {
		t.Fatal(err)
	}
	st, err := store.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := st.UpsertUserByOpenID(ctx,
		fmt.Sprintf("installed-owner-%d", time.Now().UnixNano()), "owner")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.AddMembership(ctx, int64(types.SingleTenantID), owner.ID,
		types.MembershipRoleOwner); err != nil {
		t.Fatal(err)
	}
	st.Close()
	t.Setenv("VANE_DB_URL", databaseURL)
	t.Setenv("VANE_PIPELINE_RESEARCH_V3_RUNTIME_ENABLED", "false")
	t.Setenv("VANE_SETUP_TOKEN_FILE", filepath.Join(t.TempDir(), "bootstrap-token"))
	err = run()
	if err == nil || !strings.Contains(err.Error(), "Owner Agent 启动 Gate") {
		t.Fatalf("installed runtime gate error=%v", err)
	}
}

func installationBootstrapScratchDatabase(t *testing.T) string {
	t.Helper()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		requireDatabaseCapability(t)
	}
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	databaseName := fmt.Sprintf("vane_bootstrap_cmd_%d", time.Now().UnixNano())
	adminURL := *parsed
	adminURL.Path = "/postgres"
	pool, err := pgxpool.New(t.Context(), adminURL.String())
	if err != nil {
		t.Fatal(err)
	}
	quoted := `"` + strings.ReplaceAll(databaseName, `"`, `""`) + `"`
	if _, err := pool.Exec(t.Context(), "CREATE DATABASE "+quoted); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()
	scratch := *parsed
	scratch.Path = "/" + databaseName
	t.Cleanup(func() {
		cleanup, err := pgxpool.New(context.Background(), adminURL.String())
		if err != nil {
			return
		}
		defer cleanup.Close()
		_, _ = cleanup.Exec(context.Background(), "DROP DATABASE IF EXISTS "+quoted+" WITH (FORCE)")
	})
	return scratch.String()
}

func TestInstallationTokenDigestShape(t *testing.T) {
	token, err := generateInstallationToken()
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(token))
	if len(digest) != sha256.Size {
		t.Fatalf("digest size=%d", len(digest))
	}
}

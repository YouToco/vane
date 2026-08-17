package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/YouToco/vane/server/api"
	"github.com/YouToco/vane/server/config"
	"github.com/YouToco/vane/server/store"
)

const (
	defaultInstallationTokenPath = "/var/lib/vane/bootstrap-token"
	installationTokenTTL         = 30 * time.Minute
	installationTokenBytes       = 32
)

var errInstallationClaimed = errors.New("installation setup claimed; restart required")

func installationTokenPath() string {
	if explicit := strings.TrimSpace(os.Getenv("VANE_SETUP_TOKEN_FILE")); explicit != "" {
		return explicit
	}
	if stateDir := strings.TrimSpace(os.Getenv("STATE_DIRECTORY")); stateDir != "" {
		// systemd may encode multiple directories separated by ':'. Vane owns the
		// first StateDirectory declared by its unit.
		return filepath.Join(strings.Split(stateDir, ":")[0], "bootstrap-token")
	}
	return defaultInstallationTokenPath
}

func generateInstallationToken() (string, error) {
	raw := make([]byte, installationTokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate installation token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func readInstallationToken(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return "", fmt.Errorf("bootstrap token file must be regular and mode 0600")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, 513))
	if err != nil {
		return "", err
	}
	if len(raw) > 512 {
		return "", fmt.Errorf("bootstrap token file is too large")
	}
	token := strings.TrimSpace(string(raw))
	if len(token) < 32 || len(token) > 256 || strings.ContainsAny(token, " \t\r\n") {
		return "", fmt.Errorf("bootstrap token file contains an invalid token")
	}
	return token, nil
}

func writeInstallationTokenExclusive(path, token string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		_ = f.Close()
		if !ok {
			_ = os.Remove(path)
		}
	}()
	if _, err := io.WriteString(f, token+"\n"); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	ok = true
	return nil
}

func replaceInstallationToken(path, token string) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".bootstrap-token-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if _, err := io.WriteString(tmp, token+"\n"); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	ok = true
	return nil
}

func ensureInstallationBootstrap(
	ctx context.Context,
	st *store.Store,
	path string,
) (store.InstallationBootstrapPreparation, error) {
	required, err := st.InstallationSetupRequired(ctx)
	if err != nil {
		return store.InstallationBootstrapPreparation{}, err
	}
	if !required {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.Warn("setup: 删除已消费的本地初始化令牌失败", "err", err)
		}
		return store.InstallationBootstrapPreparation{SetupRequired: false}, nil
	}

	token, err := readInstallationToken(path)
	if errors.Is(err, os.ErrNotExist) {
		token, err = generateInstallationToken()
		if err == nil {
			err = writeInstallationTokenExclusive(path, token)
			if errors.Is(err, os.ErrExist) {
				token, err = readInstallationToken(path)
			}
		}
	}
	if err != nil {
		return store.InstallationBootstrapPreparation{},
			fmt.Errorf("prepare host-local bootstrap token: %w", err)
	}
	digest := sha256.Sum256([]byte(token))
	prepared, err := st.PrepareInstallationBootstrap(
		ctx, digest[:], time.Now().Add(installationTokenTTL))
	if err != nil || !prepared.SetupRequired {
		return prepared, err
	}
	if !prepared.TokenAccepted {
		rotated, genErr := generateInstallationToken()
		if genErr != nil {
			return store.InstallationBootstrapPreparation{}, genErr
		}
		if err := replaceInstallationToken(path, rotated); err != nil {
			return store.InstallationBootstrapPreparation{},
				fmt.Errorf("rotate host-local bootstrap token: %w", err)
		}
		digest = sha256.Sum256([]byte(rotated))
		prepared, err = st.PrepareInstallationBootstrap(
			ctx, digest[:], time.Now().Add(installationTokenTTL))
		if err != nil || !prepared.TokenAccepted {
			return store.InstallationBootstrapPreparation{},
				errors.Join(err, errors.New("rotated bootstrap token was not accepted"))
		}
	}
	return prepared, nil
}

func rotateInstallationTokenUntilClaimed(
	ctx context.Context,
	st *store.Store,
	path string,
) {
	for {
		prepared, err := ensureInstallationBootstrap(ctx, st, path)
		if err != nil {
			slog.Error("setup: 自动轮换初始化令牌失败，将重试", "err", err)
			if !waitInstallationRotation(ctx, 5*time.Second) {
				return
			}
			continue
		}
		if !prepared.SetupRequired {
			return
		}
		delay := time.Until(prepared.ExpiresAt) + 100*time.Millisecond
		if delay < time.Second {
			delay = time.Second
		}
		if !waitInstallationRotation(ctx, delay) {
			return
		}
	}
}

func waitInstallationRotation(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func runInstallationSetupMode(
	ctx context.Context,
	cfg *config.Config,
	st *store.Store,
) error {
	setupCtx, cancelSetup := context.WithCancel(ctx)
	defer cancelSetup()
	go rotateInstallationTokenUntilClaimed(
		setupCtx, st, installationTokenPath())
	claimed := make(chan struct{}, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("GET /readyz", handleReadyz(st))
	api.MountInstallationSetup(mux, api.Deps{
		Store: st, Auth: st, InstallationSetup: st,
		Origin: cfg.Dashboard.Origin,
		SetupClaimed: func() {
			select {
			case claimed <- struct{}{}:
			default:
			}
		},
	})
	srv := &http.Server{
		Addr: cfg.Server.Addr, Handler: mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		slog.Info("Vane 首次初始化服务启动", "addr", cfg.Server.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()
	select {
	case <-ctx.Done():
	case err := <-serveErr:
		return err
	case <-claimed:
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown setup server: %w", err)
		}
		return errInstallationClaimed
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func printInstallationToken(path string) error {
	token, err := readInstallationToken(path)
	if err != nil {
		return fmt.Errorf("read setup token: %w", err)
	}
	fmt.Println(token)
	return nil
}

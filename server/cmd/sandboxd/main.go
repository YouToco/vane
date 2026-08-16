// sandboxd is an intentionally dark, out-of-process Firecracker control
// plane. It is not imported or started by the Vane server.
package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"github.com/YouToco/vane/server/sandbox"
)

const maxConfigBytes = 1 << 20

type config struct {
	Mode                 string                    `json:"mode"`
	FeatureEnabled       bool                      `json:"feature_enabled"`
	SocketPath           string                    `json:"socket_path"`
	SocketParentUID      uint32                    `json:"socket_parent_uid"`
	SocketParentGID      uint32                    `json:"socket_parent_gid"`
	SocketParentMode     uint32                    `json:"socket_parent_mode"`
	SocketUID            int                       `json:"socket_uid"`
	SocketGID            int                       `json:"socket_gid"`
	SocketMode           uint32                    `json:"socket_mode"`
	VaneServerUID        uint32                    `json:"vane_server_uid"`
	MaxInputBytes        int                       `json:"max_input_bytes"`
	MaxConnections       int                       `json:"max_connections"`
	MaxWireBytes         int                       `json:"max_wire_bytes"`
	AllowedPolicyDigests []string                  `json:"allowed_policy_sha256"`
	Firecracker          sandbox.FirecrackerConfig `json:"firecracker"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, arguments []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("sandboxd", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "root-owned sandboxd JSON configuration")
	if err := flags.Parse(arguments); err != nil || *configPath == "" || flags.NArg() != 1 {
		fmt.Fprintln(stderr, "usage: sandboxd -config <path> self-test|serve-dark")
		return 2
	}
	configuration, err := loadConfig(*configPath, true)
	if err != nil {
		fmt.Fprintf(stderr, "sandboxd config rejected: %v\n", err)
		return 1
	}
	backend, err := sandbox.NewFirecrackerBackend(configuration.Firecracker, nil)
	if err != nil {
		fmt.Fprintf(stderr, "sandboxd backend rejected: %v\n", err)
		return 1
	}
	if err := backend.Preflight(ctx); err != nil {
		fmt.Fprintf(stderr, "sandboxd preflight failed: %v\n", err)
		return 1
	}
	switch flags.Arg(0) {
	case "self-test":
		_ = json.NewEncoder(stdout).Encode(map[string]any{
			"status": "dark-ready", "goos": runtime.GOOS,
			"execution_enabled": false, "guest_io_implemented": false,
		})
		return 0
	case "serve-dark":
		policies := make(map[string]struct{}, len(configuration.AllowedPolicyDigests))
		for _, digest := range configuration.AllowedPolicyDigests {
			policies[digest] = struct{}{}
		}
		service, err := sandbox.NewService(sandbox.ServiceConfig{
			MaxInputBytes: configuration.MaxInputBytes, AllowedPolicyDigests: policies,
		})
		if err != nil {
			fmt.Fprintf(stderr, "sandboxd service rejected: %v\n", err)
			return 1
		}
		daemon := sandbox.Daemon{SocketPath: configuration.SocketPath,
			Socket: sandbox.SocketContract{
				ParentUID: configuration.SocketParentUID, ParentGID: configuration.SocketParentGID,
				ParentMode: os.FileMode(configuration.SocketParentMode),
				SocketUID:  configuration.SocketUID, SocketGID: configuration.SocketGID,
				SocketMode: os.FileMode(configuration.SocketMode),
			}, Service: service, Authorizer: sandbox.UIDAuthorizer{UID: configuration.VaneServerUID},
			MaxConnections: configuration.MaxConnections, MaxWireBytes: configuration.MaxWireBytes}
		if err := daemon.Serve(ctx); err != nil {
			fmt.Fprintf(stderr, "sandboxd stopped unsafely: %v\n", err)
			return 1
		}
		return 0
	default:
		fmt.Fprintln(stderr, "sandboxd command must be self-test or serve-dark")
		return 2
	}
}

func loadConfig(path string, requireRoot bool) (config, error) {
	var value config
	trustedInfo, err := verifyConfigPath(path, requireRoot)
	if err != nil {
		return value, err
	}
	file, err := os.Open(path)
	if err != nil {
		return value, err
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil || !os.SameFile(trustedInfo, openedInfo) || openedInfo.Size() != trustedInfo.Size() {
		return value, errors.New("sandboxd config inode changed while opening")
	}
	payload, err := io.ReadAll(file)
	if err != nil || int64(len(payload)) != openedInfo.Size() {
		return value, errors.New("sandboxd config size changed while reading")
	}
	if err := rejectDuplicateJSONKeys(payload); err != nil {
		return value, err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, err
	}
	if decoder.InputOffset() != int64(len(payload)) {
		return value, errors.New("sandboxd config has trailing or oversized content")
	}
	if value.Mode != "dark" || value.FeatureEnabled || !value.Firecracker.Production {
		return value, errors.New("sandboxd must remain dark with production preflight enabled")
	}
	if !filepath.IsAbs(value.SocketPath) || value.SocketParentUID != 0 || value.SocketUID != 0 ||
		value.SocketGID < 0 || (value.SocketParentMode != 0o700 && value.SocketParentMode != 0o750) ||
		value.SocketMode != 0o660 || value.MaxInputBytes < 1 ||
		value.MaxConnections < 1 || value.MaxConnections > sandbox.MaxConnectionsLimit ||
		value.MaxWireBytes < sandbox.MinWireBytesLimit || value.MaxWireBytes > sandbox.MaxWireBytesLimit ||
		value.MaxInputBytes > value.MaxWireBytes/sandbox.MaxInputWireDivisor ||
		value.VaneServerUID == 0 || len(value.AllowedPolicyDigests) == 0 {
		return value, errors.New("sandboxd authority or resource contract is incomplete")
	}
	for _, digest := range value.AllowedPolicyDigests {
		decoded, err := hex.DecodeString(digest)
		if err != nil || len(decoded) != 32 || strings.ToLower(digest) != digest {
			return value, errors.New("sandboxd allowed policy digest is invalid")
		}
	}
	if err := value.Firecracker.BindServiceIdentities(value.VaneServerUID, value.SocketUID, value.SocketGID); err != nil {
		return value, err
	}
	return value, nil
}

func verifyConfigPath(path string, requireRoot bool) (os.FileInfo, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("sandboxd config path must be absolute")
	}
	leaf, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if leaf.Mode()&os.ModeSymlink != 0 || !leaf.Mode().IsRegular() ||
		(leaf.Mode().Perm() != 0o600 && leaf.Mode().Perm() != 0o400) {
		return nil, errors.New("sandboxd config must be a non-symlink regular file with mode 0600 or 0400")
	}
	if leaf.Size() < 1 || leaf.Size() > maxConfigBytes {
		return nil, errors.New("sandboxd config size is outside the closed limit")
	}
	if stat, ok := leaf.Sys().(*syscall.Stat_t); !ok || stat.Nlink != 1 ||
		(requireRoot && stat.Uid != 0) {
		return nil, errors.New("sandboxd config owner/link contract differs")
	}
	if requireRoot {
		for current := filepath.Dir(filepath.Clean(path)); ; current = filepath.Dir(current) {
			info, err := os.Lstat(current)
			if err != nil {
				return nil, err
			}
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok || stat.Uid != 0 || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
				info.Mode().Perm()&0o022 != 0 {
				return nil, errors.New("sandboxd config parent chain is not root-owned and protected")
			}
			if current == string(filepath.Separator) {
				break
			}
		}
	}
	return leaf, nil
}

func rejectDuplicateJSONKeys(payload []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	var walkValue func() error
	walkValue = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delimiter {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("sandboxd config object key is not a string")
				}
				if _, exists := seen[key]; exists {
					return fmt.Errorf("sandboxd config contains duplicate key %q", key)
				}
				seen[key] = struct{}{}
				if err := walkValue(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := walkValue(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		default:
			return errors.New("sandboxd config has unexpected delimiter")
		}
	}
	return walkValue()
}

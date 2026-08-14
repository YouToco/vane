package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/YouToco/vane/internal/agentfirstaudit"
	"github.com/YouToco/vane/store"
)

const (
	migrationCredentialDirectoryEnv = "CREDENTIALS_DIRECTORY"
	migrationDatabaseCredential     = "migration_db_url"
	collectorTimeout                = 30 * time.Minute
)

type options struct {
	temporalHost      string
	temporalNamespace string
	temporalTaskQueue string
	operationID       string
	releaseReceipt    string
	evidenceDirectory string
	liveVaneBinary    string
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "agentfirstretention:", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	parsed, err := parseOptions(arguments)
	if err != nil {
		return err
	}
	sourceRevision, err := buildSourceRevision()
	if err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve collector executable: %w", err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		return fmt.Errorf("resolve collector executable authority: %w", err)
	}
	release, err := agentfirstaudit.ReadVerifiedReleaseReceipt(
		parsed.releaseReceipt, executable, parsed.liveVaneBinary)
	if err != nil {
		return err
	}
	if release.SourceRevision() != sourceRevision {
		return errors.New("release receipt source differs from collector build")
	}
	databaseURL, err := migrationDatabaseURL()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, collectorTimeout)
	defer cancel()
	database, err := store.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("open migration-owner Store: %w", err)
	}
	defer database.Close()
	temporalClient, err := client.Dial(client.Options{
		HostPort: parsed.temporalHost, Namespace: parsed.temporalNamespace,
	})
	if err != nil {
		return fmt.Errorf("connect Temporal: %w", err)
	}
	defer temporalClient.Close()
	result, err := agentfirstaudit.CollectBaseline(ctx, database, temporalClient,
		agentfirstaudit.BaselineCollectorRequest{
			Namespace: parsed.temporalNamespace, TaskQueue: parsed.temporalTaskQueue,
			OperationID: parsed.operationID, SourceRevision: sourceRevision,
			Release: release, EvidenceDirectory: parsed.evidenceDirectory,
		})
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		SchemaVersion  string `json:"schema_version"`
		EventID        int64  `json:"event_id"`
		PayloadDigest  string `json:"payload_digest"`
		EvidenceDigest string `json:"evidence_digest"`
		EvidencePath   string `json:"evidence_path"`
		NotBeforeUTC   string `json:"not_before_utc"`
		ExpiresAtUTC   string `json:"expires_at_utc"`
	}{
		SchemaVersion: "vane.agent-first-retention-baseline-result/v1",
		EventID:       result.Event.ID, PayloadDigest: result.Event.PayloadDigest,
		EvidenceDigest: result.Manifest.Digest, EvidencePath: result.EvidencePath,
		NotBeforeUTC: result.Event.TemporalServerWitness.Add(
			time.Duration(result.Event.RetentionSeconds) * time.Second).UTC().Format(time.RFC3339Nano),
		ExpiresAtUTC: result.Event.ExpiresAt.UTC().Format(time.RFC3339Nano),
	})
}

func parseOptions(arguments []string) (options, error) {
	var parsed options
	if len(arguments) == 0 || arguments[0] != "baseline" {
		return options{}, errors.New("baseline subcommand is required")
	}
	set := flag.NewFlagSet("agentfirstretention", flag.ContinueOnError)
	set.SetOutput(os.Stderr)
	set.StringVar(&parsed.temporalHost, "temporal-host", "", "Temporal frontend host:port")
	set.StringVar(&parsed.temporalNamespace, "temporal-namespace", "", "Temporal namespace")
	set.StringVar(&parsed.temporalTaskQueue, "temporal-task-queue", "", "production task queue")
	set.StringVar(&parsed.operationID, "operation-id", "", "stable UUID for this baseline")
	set.StringVar(&parsed.releaseReceipt, "release-receipt", "", "absolute trusted receipt path")
	set.StringVar(&parsed.evidenceDirectory, "evidence-directory", "", "absolute evidence directory")
	set.StringVar(&parsed.liveVaneBinary, "live-vane-binary", "", "absolute live vane binary")
	if err := set.Parse(arguments[1:]); err != nil {
		return options{}, err
	}
	if set.NArg() != 0 ||
		!boundedOption(parsed.temporalHost, 512) ||
		!boundedOption(parsed.temporalNamespace, 255) ||
		!boundedOption(parsed.temporalTaskQueue, 255) ||
		!canonicalUUID(parsed.operationID) ||
		!canonicalAbsolute(parsed.releaseReceipt) ||
		!canonicalAbsolute(parsed.evidenceDirectory) ||
		!canonicalAbsolute(parsed.liveVaneBinary) {
		return options{}, errors.New("baseline options are invalid")
	}
	return parsed, nil
}

func migrationDatabaseURL() (string, error) {
	directory := strings.TrimSpace(os.Getenv(migrationCredentialDirectoryEnv))
	if !canonicalAbsolute(directory) {
		return "", errors.New("migration database credential directory is unavailable")
	}
	directoryInfo, err := os.Lstat(directory)
	if err != nil || !safeCredentialAuthority(directoryInfo, true) {
		return "", errors.New("migration database credential directory authority is unsafe")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return "", errors.New("open migration database credential directory")
	}
	defer root.Close()
	info, err := root.Lstat(migrationDatabaseCredential)
	if err != nil || !safeCredentialAuthority(info, false) ||
		info.Size() <= 0 || info.Size() > 16<<10 {
		return "", errors.New("migration database credential authority is unsafe")
	}
	payload, err := root.ReadFile(migrationDatabaseCredential)
	if err != nil {
		return "", errors.New("read migration database credential")
	}
	value := strings.TrimSpace(string(payload))
	if value == "" {
		return "", errors.New("migration database credential is empty")
	}
	return value, nil
}

func safeCredentialAuthority(info os.FileInfo, directory bool) bool {
	forbiddenPermissions := os.FileMode(0o077)
	if directory {
		forbiddenPermissions = 0o022
	}
	if info == nil || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm()&forbiddenPermissions != 0 ||
		(directory && !info.IsDir()) || (!directory && !info.Mode().IsRegular()) {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && (stat.Uid == 0 || int(stat.Uid) == os.Geteuid())
}

func buildSourceRevision() (string, error) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", errors.New("collector build information is unavailable")
	}
	revision, modified := "", ""
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			modified = setting.Value
		}
	}
	if !validLowerHex(revision, 40) || modified != "false" {
		return "", errors.New("collector build revision is dirty or invalid")
	}
	return revision, nil
}

func canonicalUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for index, current := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if current != '-' {
				return false
			}
			continue
		}
		if !((current >= '0' && current <= '9') || (current >= 'a' && current <= 'f')) {
			return false
		}
	}
	return value[14] >= '1' && value[14] <= '5' && strings.ContainsRune("89ab", rune(value[19]))
}

func validLowerHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, current := range value {
		if !((current >= '0' && current <= '9') || (current >= 'a' && current <= 'f')) {
			return false
		}
	}
	return true
}

func boundedOption(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && strings.TrimSpace(value) == value
}

func canonicalAbsolute(value string) bool {
	return filepath.IsAbs(value) && filepath.Clean(value) == value
}

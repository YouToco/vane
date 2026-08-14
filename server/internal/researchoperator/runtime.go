// Package researchoperator contains the deliberately small process boundary
// shared by the one-shot Research V3 operator commands. It must not grow into
// the long-lived server configuration surface.
package researchoperator

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/YouToco/vane/server/types"
)

const (
	ExactTaskIDEnv              = "VANE_RESEARCH_OPERATOR_EXACT_TASK_ID"
	MigrationDatabaseURLEnv     = "VANE_MIGRATION_DB_URL"
	CredentialDirectoryEnv      = "CREDENTIALS_DIRECTORY"
	MigrationDatabaseCredential = "migration_db_url"
	TemporalHostEnv             = "VANE_TEMPORAL_HOST"
	TemporalNamespaceEnv        = "VANE_TEMPORAL_NAMESPACE"
	TemporalTaskQueueEnv        = "VANE_TEMPORAL_TASK_QUEUE"
)

type TemporalConfig struct {
	Host      string
	Namespace string
	TaskQueue string
}

func RequireExactTask(taskID string) error {
	exact := strings.TrimSpace(os.Getenv(ExactTaskIDEnv))
	if exact == "" || exact != taskID {
		return errors.New("task is outside the transient Research V3 operator authority")
	}
	return nil
}

func MigrationDatabaseURL() (string, error) {
	if value := strings.TrimSpace(os.Getenv(MigrationDatabaseURLEnv)); value != "" {
		return value, nil
	}
	directory := strings.TrimSpace(os.Getenv(CredentialDirectoryEnv))
	if directory == "" {
		return "", errors.New("migration-owner database credential is unavailable")
	}
	payload, err := os.ReadFile(filepath.Join(directory, MigrationDatabaseCredential))
	if err != nil {
		return "", errors.New("read migration-owner database credential")
	}
	value := strings.TrimSpace(string(payload))
	if value == "" {
		return "", errors.New("migration-owner database credential is empty")
	}
	return value, nil
}

func LoadTemporalConfig() (TemporalConfig, error) {
	config := TemporalConfig{
		Host:      envOrDefault(TemporalHostEnv, "127.0.0.1:7233"),
		Namespace: envOrDefault(TemporalNamespaceEnv, "default"),
		TaskQueue: envOrDefault(TemporalTaskQueueEnv, "vane-push"),
	}
	for _, value := range []string{config.Host, config.Namespace, config.TaskQueue} {
		if value == "" || strings.TrimSpace(value) != value || len(value) > 512 {
			return TemporalConfig{}, types.NewAppError(types.CodeValidation,
				"Research V3 operator Temporal configuration is invalid", types.ErrValidation)
		}
	}
	return config, nil
}

func envOrDefault(name, fallback string) string {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}
	return value
}

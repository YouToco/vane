package researchgateway

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/YouToco/vane/server/credentialvault"
)

type ProcessConfigV1 struct {
	DatabaseURL string
	AllowedUID  uint32
	Vault       *credentialvault.Vault
}

var credentialNameV1 = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

func LoadProcessConfigV1() (ProcessConfigV1, error) {
	for _, forbidden := range []string{"VANE_DB_URL", "VANE_DB_RESEARCH_RUNTIME_URL",
		"VANE_DB_RESEARCH_CONTROL_URL",
		"VANE_DB_RESEARCH_GATEWAY_RUNTIME_URL", "VANE_DB_RESEARCH_CAPABILITY_KEY_HEX",
		"VANE_GATEWAY_DB_URL", "VANE_GATEWAY_LLM_API_KEY", "VANE_LLM_API_KEY",
		"VANE_FETCH_EXA_API_KEY", "VANE_FETCH_TIKHUB_API_KEY", "VANE_LLM_AGENT_API_KEY",
		"VANE_GATEWAY_LLM_ROUTES_JSON"} {
		if strings.TrimSpace(os.Getenv(forbidden)) != "" {
			return ProcessConfigV1{}, fmt.Errorf(
				"research gateway refuses environment credential %s", forbidden)
		}
	}
	credentialDirectory := strings.TrimSpace(os.Getenv("CREDENTIALS_DIRECTORY"))
	if credentialDirectory == "" {
		return ProcessConfigV1{}, errors.New("research gateway credential directory is required")
	}
	readCredential := func(name string) (string, error) {
		if !credentialNameV1.MatchString(name) || filepath.Base(name) != name {
			return "", errors.New("research gateway credential name is invalid")
		}
		payload, err := os.ReadFile(filepath.Join(credentialDirectory, name))
		if err != nil {
			return "", fmt.Errorf("research gateway credential %s is unavailable", name)
		}
		value := strings.TrimSpace(string(payload))
		if value == "" {
			return "", fmt.Errorf("research gateway credential %s is empty", name)
		}
		return value, nil
	}
	databaseURL, err := readCredential("gateway_db_url")
	if err != nil {
		return ProcessConfigV1{}, err
	}
	activeKey, err := readCredential("credential_vault_active_key")
	if err != nil {
		return ProcessConfigV1{}, err
	}
	retiredKeys := ""
	retiredPath := filepath.Join(credentialDirectory, "credential_vault_retired_keys")
	if payload, readErr := os.ReadFile(retiredPath); readErr == nil {
		retiredKeys = strings.TrimSpace(string(payload))
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return ProcessConfigV1{}, errors.New(
			"research gateway credential credential_vault_retired_keys is unavailable")
	}
	keyring, err := credentialvault.ParseKeyring(
		strings.TrimSpace(os.Getenv("VANE_CREDENTIAL_VAULT_ACTIVE_KEY_ID")),
		activeKey, retiredKeys)
	if err != nil {
		return ProcessConfigV1{}, errors.New("research gateway credential vault keyring is invalid")
	}
	vault, err := credentialvault.New(keyring)
	if err != nil {
		return ProcessConfigV1{}, errors.New("research gateway credential vault is invalid")
	}
	uid64, err := strconv.ParseUint(strings.TrimSpace(os.Getenv("VANE_GATEWAY_ALLOWED_UID")), 10, 32)
	if err != nil || uid64 == 0 {
		return ProcessConfigV1{}, errors.New("research gateway allowed UID is required")
	}
	return ProcessConfigV1{
		DatabaseURL: databaseURL, AllowedUID: uint32(uid64), Vault: vault,
	}, nil
}

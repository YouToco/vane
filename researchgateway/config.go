package researchgateway

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/runtimepolicy"
)

type ProcessRouteV1 struct {
	Provider      runtimepolicy.ModelProviderIDV1
	Endpoint      runtimepolicy.EndpointRefV1
	CredentialRef runtimepolicy.CredentialRefV1
	LLM           config.LLMConfig
}

type ProcessConfigV1 struct {
	DatabaseURL string
	AllowedUID  uint32
	Routes      []ProcessRouteV1
}

type processRouteWireV1 struct {
	Provider             string `json:"provider"`
	EndpointID           string `json:"endpoint_id"`
	EndpointGeneration   int64  `json:"endpoint_generation"`
	CredentialID         string `json:"credential_id"`
	CredentialGeneration int64  `json:"credential_generation"`
	BaseURL              string `json:"base_url"`
}

var credentialNameV1 = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

func LoadProcessConfigV1() (ProcessConfigV1, error) {
	for _, forbidden := range []string{"VANE_DB_URL", "VANE_DB_RESEARCH_RUNTIME_URL",
		"VANE_DB_RESEARCH_GATEWAY_RUNTIME_URL", "VANE_DB_RESEARCH_CAPABILITY_KEY_HEX",
		"VANE_GATEWAY_DB_URL", "VANE_GATEWAY_LLM_API_KEY", "VANE_LLM_API_KEY",
		"VANE_FETCH_EXA_API_KEY", "VANE_FETCH_TIKHUB_API_KEY", "VANE_LLM_AGENT_API_KEY"} {
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
	uid64, err := strconv.ParseUint(strings.TrimSpace(os.Getenv("VANE_GATEWAY_ALLOWED_UID")), 10, 32)
	if err != nil || uid64 == 0 {
		return ProcessConfigV1{}, errors.New("research gateway allowed UID is required")
	}
	var routeWires []processRouteWireV1
	rawRoutes := []byte(strings.TrimSpace(os.Getenv("VANE_GATEWAY_LLM_ROUTES_JSON")))
	if len(rawRoutes) == 0 || strictjson.Decode(rawRoutes, &routeWires) != nil ||
		len(routeWires) == 0 || len(routeWires) > 8 {
		return ProcessConfigV1{}, errors.New("research gateway route registry is invalid")
	}
	result := ProcessConfigV1{DatabaseURL: databaseURL, AllowedUID: uint32(uid64),
		Routes: make([]ProcessRouteV1, 0, len(routeWires))}
	seen := make(map[string]struct{}, len(routeWires))
	for _, wire := range routeWires {
		endpointURL, parseErr := url.Parse(wire.BaseURL)
		key := fmt.Sprintf("%s/%s/%d/%s/%d", wire.Provider, wire.EndpointID,
			wire.EndpointGeneration, wire.CredentialID, wire.CredentialGeneration)
		_, duplicate := seen[key]
		if parseErr != nil || endpointURL.Scheme != "https" ||
			endpointURL.Hostname() != "api.deepseek.com" || endpointURL.Port() != "" ||
			(endpointURL.Path != "" && endpointURL.Path != "/v1") ||
			endpointURL.User != nil || endpointURL.RawQuery != "" || endpointURL.Fragment != "" ||
			wire.Provider != string(runtimepolicy.ModelProviderDeepSeekV1) ||
			wire.EndpointID != string(runtimepolicy.EndpointIDDeepSeekCompatiblePrimaryV1) ||
			wire.CredentialID != string(runtimepolicy.CredentialIDLLMPrimaryV1) ||
			wire.EndpointGeneration <= 0 || wire.CredentialGeneration <= 0 || duplicate {
			return ProcessConfigV1{}, errors.New("research gateway retained route is invalid")
		}
		// Credential purpose is not configurable data. Derive the only permitted
		// systemd credential name from the frozen credential generation so route
		// JSON can never exfiltrate gateway_db_url as a Provider bearer token.
		credentialName := fmt.Sprintf("llm_api_key_gen%d", wire.CredentialGeneration)
		apiKey, readErr := readCredential(credentialName)
		if readErr != nil {
			return ProcessConfigV1{}, readErr
		}
		seen[key] = struct{}{}
		result.Routes = append(result.Routes, ProcessRouteV1{
			Provider: runtimepolicy.ModelProviderIDV1(wire.Provider),
			Endpoint: runtimepolicy.EndpointRefV1{
				ID: runtimepolicy.EndpointIDV1(wire.EndpointID), Generation: wire.EndpointGeneration},
			CredentialRef: runtimepolicy.CredentialRefV1{
				ID: runtimepolicy.CredentialIDV1(wire.CredentialID), Generation: wire.CredentialGeneration},
			LLM: config.LLMConfig{Provider: wire.Provider, BaseURL: wire.BaseURL,
				APIKey: apiKey, MaxConcurrent: 8},
		})
	}
	return result, nil
}

package config

import (
	"strings"
	"testing"
)

func readyResearchV3ShadowConfig() Config {
	return Config{
		DB: DBConfig{
			URL: "postgres://owner", ResearchRuntimeURL: "postgres://runtime",
			ResearchControlURL:       "postgres://control",
			ResearchCapabilityKeyID:  "active-v3",
			ResearchCapabilityKeyHex: strings.Repeat("42", 32),
		},
		LLM: LLMConfig{
			CompiledEndpointGeneration: 1, CompiledCredentialGeneration: 1,
		},
		Fetch: FetchConfig{
			ExaAPIKey: "exa-test", CompiledExaCredentialGeneration: 1,
			CompiledTikHubCredentialGeneration: 1,
		},
		Pipeline: PipelineConfig{ResearchV3ShadowCanaryScheduleID: "task-v3"},
	}
}

func TestResearchV3AuthorityRequiresExactShadowedTask(t *testing.T) {
	cfg := readyResearchV3ShadowConfig()
	cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID = " task-v3 "
	cfg.Pipeline.PushEffectRecoveryCanaryScheduleID = " task-v3 "
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID != "task-v3" {
		t.Fatalf("trimmed authority=%q", cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID)
	}

	cfg = readyResearchV3ShadowConfig()
	cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID = "task-other"
	cfg.Pipeline.PushEffectRecoveryCanaryScheduleID = "task-other"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "同一任务") {
		t.Fatalf("different authority returned %v", err)
	}

	cfg = readyResearchV3ShadowConfig()
	cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID = "   "
	if err := cfg.Validate(); err == nil {
		t.Fatal("whitespace-only authority was accepted")
	}
	for name, authorityID := range map[string]string{
		"oversize": strings.Repeat("x", 256),
		"control":  "task-v3\nother",
		"format":   "task-v3\u200bother",
	} {
		t.Run(name+" authority rejected", func(t *testing.T) {
			cfg := readyResearchV3ShadowConfig()
			cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID = authorityID
			cfg.Pipeline.PushEffectRecoveryCanaryScheduleID = authorityID
			if err := cfg.Validate(); err == nil {
				t.Fatalf("invalid authority ID %q was accepted", authorityID)
			}
		})
	}

	t.Run("authority requires same-task durable recovery", func(t *testing.T) {
		cfg := readyResearchV3ShadowConfig()
		cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID = "task-v3"
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "push effect recovery") {
			t.Fatalf("authority without recovery returned %v", err)
		}

		cfg = readyResearchV3ShadowConfig()
		cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID = "task-v3"
		cfg.Pipeline.PushEffectRecoveryCanaryScheduleID = "task-other"
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "同一任务") {
			t.Fatalf("authority with cross-task recovery returned %v", err)
		}
	})

	t.Run("V3 recovery does not require legacy compiled rollout", func(t *testing.T) {
		cfg := readyResearchV3ShadowConfig()
		cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID = "task-v3"
		cfg.Pipeline.PushEffectRecoveryCanaryScheduleID = "task-v3"
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestResearchV3ShadowRequiresCompleteRuntime(t *testing.T) {
	t.Run("ready shadow", func(t *testing.T) {
		cfg := readyResearchV3ShadowConfig()
		cfg.Pipeline.ResearchV3ShadowCanaryScheduleID = " task-v3 "
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		if cfg.Pipeline.ResearchV3ShadowCanaryScheduleID != "task-v3" {
			t.Fatalf("trimmed shadow=%q", cfg.Pipeline.ResearchV3ShadowCanaryScheduleID)
		}
	})

	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "runtime URL", mutate: func(c *Config) { c.DB.ResearchRuntimeURL = "" }},
		{name: "control URL", mutate: func(c *Config) { c.DB.ResearchControlURL = "" }},
		{name: "active key ID", mutate: func(c *Config) { c.DB.ResearchCapabilityKeyID = "" }},
		{name: "active key hex", mutate: func(c *Config) { c.DB.ResearchCapabilityKeyHex = "" }},
		{name: "valid key hex", mutate: func(c *Config) { c.DB.ResearchCapabilityKeyHex = strings.Repeat("z", 64) }},
		{name: "LLM endpoint generation", mutate: func(c *Config) { c.LLM.CompiledEndpointGeneration = -1 }},
		{name: "LLM credential generation", mutate: func(c *Config) { c.LLM.CompiledCredentialGeneration = -1 }},
		{name: "Exa generation", mutate: func(c *Config) { c.Fetch.CompiledExaCredentialGeneration = -1 }},
		{name: "Exa key", mutate: func(c *Config) { c.Fetch.ExaAPIKey = " " }},
	}
	for _, tc := range tests {
		t.Run("missing "+tc.name, func(t *testing.T) {
			cfg := readyResearchV3ShadowConfig()
			tc.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatalf("shadow accepted missing %s", tc.name)
			}
		})
	}

	t.Run("hard dark needs no V3 runtime", func(t *testing.T) {
		cfg := Config{DB: DBConfig{URL: "postgres://owner"}}
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("blank shadow rejected", func(t *testing.T) {
		cfg := readyResearchV3ShadowConfig()
		cfg.Pipeline.ResearchV3ShadowCanaryScheduleID = "   "
		if err := cfg.Validate(); err == nil {
			t.Fatal("whitespace-only shadow was accepted")
		}
	})
	for name, shadowID := range map[string]string{
		"oversize": strings.Repeat("x", 256),
		"control":  "task-v3\nother",
		"format":   "task-v3\u200bother",
	} {
		t.Run(name+" shadow rejected", func(t *testing.T) {
			cfg := readyResearchV3ShadowConfig()
			cfg.Pipeline.ResearchV3ShadowCanaryScheduleID = shadowID
			if err := cfg.Validate(); err == nil {
				t.Fatalf("invalid shadow ID %q was accepted", shadowID)
			}
		})
	}
}

func TestEnvOnlyResearchV3ShadowCanary(t *testing.T) {
	clearVaneEnv(t)
	skipIfSystemConfigExists(t)
	t.Chdir(t.TempDir())
	t.Setenv("VANE_DB_URL", "postgres://owner")
	t.Setenv("VANE_DB_RESEARCH_RUNTIME_URL", "postgres://runtime")
	t.Setenv("VANE_DB_RESEARCH_CONTROL_URL", "postgres://control")
	t.Setenv("VANE_DB_RESEARCH_CAPABILITY_KEY_ID", "active-v3")
	t.Setenv("VANE_DB_RESEARCH_CAPABILITY_KEY_HEX", strings.Repeat("42", 32))
	t.Setenv("VANE_FETCH_EXA_API_KEY", "exa-test")
	t.Setenv("VANE_PIPELINE_RESEARCH_V3_SHADOW_CANARY_SCHEDULE_ID", " task-v3 ")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Pipeline.ResearchV3ShadowCanaryScheduleID != "task-v3" ||
		cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID != "" {
		t.Fatalf("env-only Research V3 shadow/authority=%q/%q",
			cfg.Pipeline.ResearchV3ShadowCanaryScheduleID,
			cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID)
	}
}

func TestEnvOnlyResearchV3AuthorityRequiresSameShadow(t *testing.T) {
	clearVaneEnv(t)
	skipIfSystemConfigExists(t)
	t.Chdir(t.TempDir())
	t.Setenv("VANE_DB_URL", "postgres://owner")
	t.Setenv("VANE_DB_RESEARCH_RUNTIME_URL", "postgres://runtime")
	t.Setenv("VANE_DB_RESEARCH_CONTROL_URL", "postgres://control")
	t.Setenv("VANE_DB_RESEARCH_CAPABILITY_KEY_ID", "active-v3")
	t.Setenv("VANE_DB_RESEARCH_CAPABILITY_KEY_HEX", strings.Repeat("42", 32))
	t.Setenv("VANE_FETCH_EXA_API_KEY", "exa-test")
	t.Setenv("VANE_PIPELINE_RESEARCH_V3_SHADOW_CANARY_SCHEDULE_ID", "task-v3")
	t.Setenv("VANE_PIPELINE_RESEARCH_V3_AUTHORITY_CANARY_SCHEDULE_ID", "task-v3")
	t.Setenv("VANE_PIPELINE_PUSH_EFFECT_RECOVERY_CANARY_SCHEDULE_ID", "task-v3")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Pipeline.ResearchV3ShadowCanaryScheduleID != "task-v3" ||
		cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID != "task-v3" {
		t.Fatalf("env authority/shadow=%q/%q",
			cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID,
			cfg.Pipeline.ResearchV3ShadowCanaryScheduleID)
	}
}

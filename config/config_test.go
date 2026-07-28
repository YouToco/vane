package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func TestRunOutcomeRolloutValidation(t *testing.T) {
	tests := []struct {
		name            string
		compiled        bool
		compiledCanary  string
		compiledAll     bool
		enabled         bool
		outcomeCanary   string
		outcomeAllowAll bool
		wantErr         bool
	}{
		{name: "off"},
		{
			name: "exact nested canary", compiled: true,
			compiledCanary: "task-a", enabled: true,
			outcomeCanary: " task-a ",
		},
		{
			name:     "compiled all permits exact outcome canary",
			compiled: true, compiledAll: true, enabled: true,
			outcomeCanary: "task-a",
		},
		{
			name:     "both allow all",
			compiled: true, compiledAll: true,
			enabled: true, outcomeAllowAll: true,
		},
		{
			name: "requires compiled", enabled: true,
			outcomeCanary: "task-a", wantErr: true,
		},
		{
			name: "outside compiled canary", compiled: true,
			compiledCanary: "task-a", enabled: true,
			outcomeCanary: "task-b", wantErr: true,
		},
		{
			name: "missing second key", compiled: true,
			compiledAll: true, enabled: true, wantErr: true,
		},
		{
			name: "canary and all conflict", compiled: true,
			compiledAll: true, enabled: true,
			outcomeCanary: "task-a", outcomeAllowAll: true, wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := Config{
				DB: DBConfig{URL: "postgres://test"},
				Pipeline: PipelineConfig{
					CompiledRuntimeEnabled:          test.compiled,
					CompiledRuntimeCanaryScheduleID: test.compiledCanary,
					CompiledRuntimeAllowAll:         test.compiledAll,
					RunOutcomeEnabled:               test.enabled,
					RunOutcomeCanaryScheduleID:      test.outcomeCanary,
					RunOutcomeAllowAll:              test.outcomeAllowAll,
				},
			}
			err := cfg.Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, wantErr=%t", err, test.wantErr)
			}
			if err == nil && test.outcomeCanary != "" &&
				cfg.Pipeline.RunOutcomeCanaryScheduleID !=
					strings.TrimSpace(test.outcomeCanary) {
				t.Fatalf("outcome canary = %q",
					cfg.Pipeline.RunOutcomeCanaryScheduleID)
			}
		})
	}
}

func TestCanonicalBriefRolloutValidation(t *testing.T) {
	tests := []struct {
		name           string
		compiled       bool
		compiledCanary string
		compiledAll    bool
		outcome        bool
		outcomeCanary  string
		outcomeAll     bool
		brief          bool
		briefCanary    string
		briefAll       bool
		effectCanary   string
		recoveryCanary string
		wantErr        bool
	}{
		{name: "off"},
		{
			name:     "exact nested canary",
			compiled: true, compiledCanary: "task-a",
			outcome: true, outcomeCanary: "task-a",
			brief: true, briefCanary: " task-a ",
			effectCanary: "task-a", recoveryCanary: "task-a",
		},
		{
			name:     "outcome all permits exact brief canary",
			compiled: true, compiledAll: true,
			outcome: true, outcomeAll: true,
			brief: true, briefCanary: "task-a",
			effectCanary: "task-a", recoveryCanary: "task-a",
		},
		{
			name:     "all layers allow all",
			compiled: true, compiledAll: true,
			outcome: true, outcomeAll: true,
			brief: true, briefAll: true,
			wantErr: true,
		},
		{
			name:     "requires outcome",
			compiled: true, compiledCanary: "task-a",
			brief: true, briefCanary: "task-a", wantErr: true,
		},
		{
			name:     "outside outcome canary",
			compiled: true, compiledAll: true,
			outcome: true, outcomeCanary: "task-a",
			brief: true, briefCanary: "task-b", wantErr: true,
		},
		{
			name:     "missing second key",
			compiled: true, compiledAll: true,
			outcome: true, outcomeAll: true,
			brief: true, wantErr: true,
		},
		{
			name:     "canary and all conflict",
			compiled: true, compiledAll: true,
			outcome: true, outcomeAll: true,
			brief: true, briefCanary: "task-a", briefAll: true,
			wantErr: true,
		},
		{
			name:     "outside push effect fresh canary",
			compiled: true, compiledAll: true,
			outcome: true, outcomeAll: true,
			brief: true, briefCanary: "task-a",
			effectCanary: "task-b", recoveryCanary: "task-a",
			wantErr: true,
		},
		{
			name:     "outside push effect recovery canary",
			compiled: true, compiledAll: true,
			outcome: true, outcomeAll: true,
			brief: true, briefCanary: "task-a",
			effectCanary: "task-a", recoveryCanary: "task-b",
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := Config{
				DB: DBConfig{URL: "postgres://test"},
				Pipeline: PipelineConfig{
					CompiledRuntimeEnabled:             test.compiled,
					CompiledRuntimeCanaryScheduleID:    test.compiledCanary,
					CompiledRuntimeAllowAll:            test.compiledAll,
					RunOutcomeEnabled:                  test.outcome,
					RunOutcomeCanaryScheduleID:         test.outcomeCanary,
					RunOutcomeAllowAll:                 test.outcomeAll,
					CanonicalBriefEnabled:              test.brief,
					CanonicalBriefCanaryScheduleID:     test.briefCanary,
					CanonicalBriefAllowAll:             test.briefAll,
					PushEffectCanaryScheduleID:         test.effectCanary,
					PushEffectRecoveryCanaryScheduleID: test.recoveryCanary,
				},
			}
			err := cfg.Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, wantErr=%t", err, test.wantErr)
			}
			if err == nil && test.briefCanary != "" &&
				cfg.Pipeline.CanonicalBriefCanaryScheduleID !=
					strings.TrimSpace(test.briefCanary) {
				t.Fatalf("brief canary = %q",
					cfg.Pipeline.CanonicalBriefCanaryScheduleID)
			}
		})
	}
}

func TestStructuredInsightRolloutValidation(t *testing.T) {
	base := func() Config {
		return Config{
			DB: DBConfig{URL: "postgres://test"},
			Dashboard: DashboardConfig{
				Origin: "https://vane.example",
			},
			Pipeline: PipelineConfig{
				CompiledRuntimeEnabled:                    true,
				CompiledRuntimeCanaryScheduleID:           "task-a",
				RunOutcomeEnabled:                         true,
				RunOutcomeCanaryScheduleID:                "task-a",
				CanonicalBriefEnabled:                     true,
				CanonicalBriefCanaryScheduleID:            "task-a",
				CanonicalBriefRendererCanaryScheduleID:    "task-a",
				StructuredInsightRendererEnabled:          true,
				StructuredInsightRendererCanaryScheduleID: "task-a",
				PushEffectCanaryScheduleID:                "task-a",
				PushEffectRecoveryCanaryScheduleID:        "task-a",
			},
		}
	}
	t.Run("exact nested canary", func(t *testing.T) {
		cfg := base()
		cfg.Pipeline.StructuredInsightEnabled = true
		cfg.Pipeline.StructuredInsightCanaryScheduleID = " task-a "
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		if cfg.Pipeline.StructuredInsightCanaryScheduleID != "task-a" {
			t.Fatalf("canary = %q", cfg.Pipeline.StructuredInsightCanaryScheduleID)
		}
	})
	t.Run("requires canonical brief", func(t *testing.T) {
		cfg := base()
		cfg.Pipeline.CanonicalBriefEnabled = false
		cfg.Pipeline.StructuredInsightEnabled = true
		cfg.Pipeline.StructuredInsightCanaryScheduleID = "task-a"
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected nesting error")
		}
	})
	t.Run("mismatched canary", func(t *testing.T) {
		cfg := base()
		cfg.Pipeline.StructuredInsightEnabled = true
		cfg.Pipeline.StructuredInsightCanaryScheduleID = "task-b"
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected canary mismatch")
		}
	})
	t.Run("requires independent renderer", func(t *testing.T) {
		cfg := base()
		cfg.Pipeline.StructuredInsightEnabled = true
		cfg.Pipeline.StructuredInsightCanaryScheduleID = "task-a"
		cfg.Pipeline.StructuredInsightRendererEnabled = false
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected renderer nesting error")
		}
	})
	t.Run("renderer must match canonical renderer", func(t *testing.T) {
		cfg := base()
		cfg.Pipeline.StructuredInsightRendererCanaryScheduleID = "task-b"
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected renderer canary mismatch")
		}
	})
	t.Run("canary and allow all conflict", func(t *testing.T) {
		cfg := base()
		cfg.Pipeline.StructuredInsightEnabled = true
		cfg.Pipeline.StructuredInsightCanaryScheduleID = "task-a"
		cfg.Pipeline.StructuredInsightAllowAll = true
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected rollout conflict")
		}
	})
}

func TestStructuredEventEvidenceRolloutValidation(t *testing.T) {
	base := func() Config {
		return Config{
			DB: DBConfig{URL: "postgres://test"},
			Dashboard: DashboardConfig{
				Origin: "https://vane.example",
			},
			Pipeline: PipelineConfig{
				CompiledRuntimeEnabled:                    true,
				CompiledRuntimeCanaryScheduleID:           "task-a",
				RunOutcomeEnabled:                         true,
				RunOutcomeCanaryScheduleID:                "task-a",
				CanonicalBriefEnabled:                     true,
				CanonicalBriefCanaryScheduleID:            "task-a",
				CanonicalBriefRendererCanaryScheduleID:    "task-a",
				StructuredInsightEnabled:                  true,
				StructuredInsightCanaryScheduleID:         "task-a",
				StructuredInsightRendererEnabled:          true,
				StructuredInsightRendererCanaryScheduleID: "task-a",
				PushEffectCanaryScheduleID:                "task-a",
				PushEffectRecoveryCanaryScheduleID:        "task-a",
				ObservationShadowCanaryScheduleID:         "task-a",
				ObservationAuthorityCanaryScheduleID:      "task-a",
			},
		}
	}
	t.Run("exact nested canary", func(t *testing.T) {
		cfg := base()
		cfg.Pipeline.StructuredEventEvidenceEnabled = true
		cfg.Pipeline.StructuredEventEvidenceCanaryScheduleID = " task-a "
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		if cfg.Pipeline.StructuredEventEvidenceCanaryScheduleID != "task-a" {
			t.Fatalf("canary = %q",
				cfg.Pipeline.StructuredEventEvidenceCanaryScheduleID)
		}
	})
	t.Run("requires structured insight", func(t *testing.T) {
		cfg := base()
		cfg.Pipeline.StructuredInsightEnabled = false
		cfg.Pipeline.StructuredEventEvidenceEnabled = true
		cfg.Pipeline.StructuredEventEvidenceCanaryScheduleID = "task-a"
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected nesting error")
		}
	})
	t.Run("mismatched canary", func(t *testing.T) {
		cfg := base()
		cfg.Pipeline.StructuredEventEvidenceEnabled = true
		cfg.Pipeline.StructuredEventEvidenceCanaryScheduleID = "task-b"
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected canary mismatch")
		}
	})
	t.Run("requires exact observation authority", func(t *testing.T) {
		cfg := base()
		cfg.Pipeline.ObservationAuthorityCanaryScheduleID = ""
		cfg.Pipeline.StructuredEventEvidenceEnabled = true
		cfg.Pipeline.StructuredEventEvidenceCanaryScheduleID = "task-a"
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected observation authority nesting error")
		}
	})
	t.Run("canary and allow all conflict", func(t *testing.T) {
		cfg := base()
		cfg.Pipeline.StructuredEventEvidenceEnabled = true
		cfg.Pipeline.StructuredEventEvidenceCanaryScheduleID = "task-a"
		cfg.Pipeline.StructuredEventEvidenceAllowAll = true
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected rollout conflict")
		}
	})
}

func TestExecutiveBriefRolloutValidation(t *testing.T) {
	base := func() Config {
		return Config{
			DB:        DBConfig{URL: "postgres://test"},
			Dashboard: DashboardConfig{Origin: "https://vane.example"},
			Pipeline: PipelineConfig{
				CompiledRuntimeEnabled:                    true,
				CompiledRuntimeCanaryScheduleID:           "task-a",
				RunOutcomeEnabled:                         true,
				RunOutcomeCanaryScheduleID:                "task-a",
				CanonicalBriefEnabled:                     true,
				CanonicalBriefCanaryScheduleID:            "task-a",
				CanonicalBriefRendererCanaryScheduleID:    "task-a",
				StructuredInsightEnabled:                  true,
				StructuredInsightCanaryScheduleID:         "task-a",
				StructuredInsightRendererEnabled:          true,
				StructuredInsightRendererCanaryScheduleID: "task-a",
				StructuredEventEvidenceEnabled:            true,
				StructuredEventEvidenceCanaryScheduleID:   "task-a",
				ObservationShadowCanaryScheduleID:         "task-a",
				ObservationAuthorityCanaryScheduleID:      "task-a",
				PushEffectCanaryScheduleID:                "task-a",
				PushEffectRecoveryCanaryScheduleID:        "task-a",
			},
		}
	}
	t.Run("exact nested canary", func(t *testing.T) {
		cfg := base()
		cfg.Pipeline.ExecutiveBriefEnabled = true
		cfg.Pipeline.ExecutiveBriefCanaryScheduleID = " task-a "
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		if cfg.Pipeline.ExecutiveBriefCanaryScheduleID != "task-a" {
			t.Fatalf("canary = %q",
				cfg.Pipeline.ExecutiveBriefCanaryScheduleID)
		}
	})
	t.Run("dark synthesis and independent channel canaries", func(t *testing.T) {
		cfg := base()
		cfg.Pipeline.ExecutiveBriefEnabled = true
		cfg.Pipeline.ExecutiveBriefCanaryScheduleID = "task-a"
		if err := cfg.Validate(); err != nil {
			t.Fatalf("dark synthesis rejected: %v", err)
		}
		cfg.Pipeline.ExecutiveBriefWebCanaryScheduleID = " task-a "
		cfg.Pipeline.ExecutiveBriefRendererCanaryScheduleID = "task-a"
		if err := cfg.Validate(); err != nil {
			t.Fatalf("channel canaries rejected: %v", err)
		}
		if cfg.Pipeline.ExecutiveBriefWebCanaryScheduleID != "task-a" {
			t.Fatalf("Web canary=%q",
				cfg.Pipeline.ExecutiveBriefWebCanaryScheduleID)
		}
	})
	t.Run("channel canary outside synthesis is rejected", func(t *testing.T) {
		cfg := base()
		cfg.Pipeline.ExecutiveBriefEnabled = true
		cfg.Pipeline.ExecutiveBriefCanaryScheduleID = "task-a"
		cfg.Pipeline.ExecutiveBriefWebCanaryScheduleID = "task-b"
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected Web rollout nesting error")
		}
	})
	t.Run("renderer without Web canary is rejected", func(t *testing.T) {
		cfg := base()
		cfg.Pipeline.ExecutiveBriefEnabled = true
		cfg.Pipeline.ExecutiveBriefCanaryScheduleID = "task-a"
		cfg.Pipeline.ExecutiveBriefRendererCanaryScheduleID = "task-a"
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected renderer/Web rollout nesting error")
		}
	})
	t.Run("requires structured event evidence", func(t *testing.T) {
		cfg := base()
		cfg.Pipeline.StructuredEventEvidenceEnabled = false
		cfg.Pipeline.ExecutiveBriefEnabled = true
		cfg.Pipeline.ExecutiveBriefCanaryScheduleID = "task-a"
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected nesting error")
		}
	})
	t.Run("mismatched canary", func(t *testing.T) {
		cfg := base()
		cfg.Pipeline.ExecutiveBriefEnabled = true
		cfg.Pipeline.ExecutiveBriefCanaryScheduleID = "task-b"
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected canary mismatch")
		}
	})
	t.Run("canary and allow all conflict", func(t *testing.T) {
		cfg := base()
		cfg.Pipeline.ExecutiveBriefEnabled = true
		cfg.Pipeline.ExecutiveBriefCanaryScheduleID = "task-a"
		cfg.Pipeline.ExecutiveBriefAllowAll = true
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected rollout conflict")
		}
	})
}

func TestCanonicalBriefRendererRolloutValidation(t *testing.T) {
	valid := func() Config {
		return Config{
			DB: DBConfig{URL: "postgres://test"},
			Dashboard: DashboardConfig{
				Origin: "https://vane.example",
			},
			Pipeline: PipelineConfig{
				CompiledRuntimeEnabled:             true,
				CompiledRuntimeCanaryScheduleID:    "task-a",
				RunOutcomeEnabled:                  true,
				RunOutcomeCanaryScheduleID:         "task-a",
				CanonicalBriefEnabled:              true,
				CanonicalBriefCanaryScheduleID:     "task-a",
				PushEffectCanaryScheduleID:         "task-a",
				PushEffectRecoveryCanaryScheduleID: "task-a",
			},
		}
	}
	t.Run("exact nested canary", func(t *testing.T) {
		cfg := valid()
		cfg.Pipeline.CanonicalBriefRendererCanaryScheduleID =
			" task-a "
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		if cfg.Pipeline.CanonicalBriefRendererCanaryScheduleID !=
			"task-a" {
			t.Fatalf("renderer canary=%q",
				cfg.Pipeline.CanonicalBriefRendererCanaryScheduleID)
		}
	})
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"whitespace", func(c *Config) {
			c.Pipeline.CanonicalBriefRendererCanaryScheduleID = " "
		}},
		{"outside writer", func(c *Config) {
			c.Pipeline.CanonicalBriefRendererCanaryScheduleID = "task-b"
		}},
		{"writer disabled", func(c *Config) {
			c.Pipeline.CanonicalBriefRendererCanaryScheduleID = "task-a"
			c.Pipeline.CanonicalBriefEnabled = false
		}},
		{"bad dashboard origin", func(c *Config) {
			c.Pipeline.CanonicalBriefRendererCanaryScheduleID = "task-a"
			c.Dashboard.Origin = "https://user@example.com/path?x=1"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid()
			test.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() unexpectedly succeeded")
			}
		})
	}
}

func TestEnvOnlyCanonicalBriefRendererCanary(t *testing.T) {
	clearVaneEnv(t)
	skipIfSystemConfigExists(t)
	t.Chdir(t.TempDir())
	t.Setenv("VANE_DB_URL", "postgres://env")
	t.Setenv("VANE_PIPELINE_COMPILED_RUNTIME_ENABLED", "true")
	t.Setenv("VANE_PIPELINE_COMPILED_RUNTIME_CANARY_SCHEDULE_ID", "task-a")
	t.Setenv("VANE_PIPELINE_RUN_OUTCOME_ENABLED", "true")
	t.Setenv("VANE_PIPELINE_RUN_OUTCOME_CANARY_SCHEDULE_ID", "task-a")
	t.Setenv("VANE_PIPELINE_CANONICAL_BRIEF_ENABLED", "true")
	t.Setenv("VANE_PIPELINE_CANONICAL_BRIEF_CANARY_SCHEDULE_ID", "task-a")
	t.Setenv("VANE_PIPELINE_PUSH_EFFECT_CANARY_SCHEDULE_ID", "task-a")
	t.Setenv("VANE_PIPELINE_PUSH_EFFECT_RECOVERY_CANARY_SCHEDULE_ID", "task-a")
	t.Setenv(
		"VANE_PIPELINE_CANONICAL_BRIEF_RENDERER_CANARY_SCHEDULE_ID",
		" task-a ",
	)
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Pipeline.CanonicalBriefRendererCanaryScheduleID !=
		"task-a" {
		t.Fatalf("env-only renderer canary=%q",
			cfg.Pipeline.CanonicalBriefRendererCanaryScheduleID)
	}
}

func TestSnapshotV2ShadowCanaryScheduleIDValidation(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "off"},
		{name: "exact", value: "push-shadow-canary"},
		{name: "trimmed", value: "  push-shadow-canary  "},
		{name: "whitespace", value: "  ", wantErr: true},
		{name: "control", value: "push\nshadow", wantErr: true},
		{name: "format control", value: "push\u200bshadow", wantErr: true},
		{name: "too long", value: strings.Repeat("a", 256), wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := Config{DB: DBConfig{URL: "postgres://test"}}
			cfg.Pipeline.SnapshotV2ShadowCanaryScheduleID = test.value
			err := cfg.Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v", err)
			}
			if !test.wantErr && test.value != "" &&
				cfg.Pipeline.SnapshotV2ShadowCanaryScheduleID !=
					strings.TrimSpace(test.value) {
				t.Fatalf("canary id = %q",
					cfg.Pipeline.SnapshotV2ShadowCanaryScheduleID)
			}
		})
	}
}

func TestSnapshotV2ReadAuditCanaryValidation(t *testing.T) {
	tests := []struct {
		name           string
		compiled       bool
		compiledCanary string
		allowAll       bool
		shadow         string
		readAudit      string
		want           string
		wantErr        bool
	}{
		{name: "off"},
		{
			name: "exact match", compiled: true,
			compiledCanary: "push-shadow-canary",
			shadow:         "push-shadow-canary", readAudit: " push-shadow-canary ",
			want: "push-shadow-canary",
		},
		{
			name: "compiled rollout task differs", compiled: true,
			compiledCanary: "push-compiled-other",
			shadow:         "push-shadow-canary", readAudit: "push-shadow-canary",
			wantErr: true,
		},
		{
			name: "compiled allow all contains audit task", compiled: true,
			allowAll: true,
			shadow:   "push-shadow-canary", readAudit: "push-shadow-canary",
			want: "push-shadow-canary",
		},
		{
			name: "compiled disabled", shadow: "push-shadow-canary",
			readAudit: "push-shadow-canary", wantErr: true,
		},
		{
			name: "writer differs", compiled: true, shadow: "push-shadow-canary",
			readAudit: "push-other", wantErr: true,
		},
		{
			name: "writer missing", compiled: true,
			readAudit: "push-shadow-canary", wantErr: true,
		},
		{
			name: "whitespace", compiled: true, shadow: "push-shadow-canary",
			readAudit: " \t ", wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := Config{
				DB: DBConfig{URL: "postgres://test"},
				Pipeline: PipelineConfig{
					CompiledRuntimeEnabled:              test.compiled,
					CompiledRuntimeCanaryScheduleID:     test.compiledCanary,
					CompiledRuntimeAllowAll:             test.allowAll,
					SnapshotV2ShadowCanaryScheduleID:    test.shadow,
					SnapshotV2ReadAuditCanaryScheduleID: test.readAudit,
				},
			}
			err := cfg.Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, wantErr=%v", err, test.wantErr)
			}
			if !test.wantErr &&
				cfg.Pipeline.SnapshotV2ReadAuditCanaryScheduleID != test.want {
				t.Fatalf("read audit canary = %q, want %q",
					cfg.Pipeline.SnapshotV2ReadAuditCanaryScheduleID, test.want)
			}
		})
	}
}

func TestPushEffectCanaryValidation(t *testing.T) {
	tests := []struct {
		name           string
		compiled       bool
		compiledCanary string
		allowAll       bool
		effectCanary   string
		want           string
		wantErr        bool
	}{
		{name: "off"},
		{
			name: "exact compiled task", compiled: true,
			compiledCanary: "push-effect-task",
			effectCanary:   " push-effect-task ",
			want:           "push-effect-task",
		},
		{
			name: "compiled allow all contains exact task", compiled: true,
			allowAll: true, effectCanary: "push-effect-task",
			want: "push-effect-task",
		},
		{
			name:         "compiled disabled",
			effectCanary: "push-effect-task", wantErr: true,
		},
		{
			name: "outside compiled rollout", compiled: true,
			compiledCanary: "other-task",
			effectCanary:   "push-effect-task",
			wantErr:        true,
		},
		{
			name: "whitespace", compiled: true,
			compiledCanary: "push-effect-task",
			effectCanary:   " \t ",
			wantErr:        true,
		},
		{
			name: "control", compiled: true, allowAll: true,
			effectCanary: "push\neffect", wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := Config{
				DB: DBConfig{URL: "postgres://test"},
				Pipeline: PipelineConfig{
					CompiledRuntimeEnabled:          test.compiled,
					CompiledRuntimeCanaryScheduleID: test.compiledCanary,
					CompiledRuntimeAllowAll:         test.allowAll,
					PushEffectCanaryScheduleID:      test.effectCanary,
				},
			}
			err := cfg.Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, wantErr=%v", err, test.wantErr)
			}
			if !test.wantErr &&
				cfg.Pipeline.PushEffectCanaryScheduleID != test.want {
				t.Fatalf("push effect canary = %q, want %q",
					cfg.Pipeline.PushEffectCanaryScheduleID, test.want)
			}
		})
	}
}

func TestPushEffectCanaryLoadsFromPureEnvironment(t *testing.T) {
	clearVaneEnv(t)
	skipIfSystemConfigExists(t)
	t.Chdir(t.TempDir())
	t.Setenv("VANE_DB_URL", "postgres://env")
	t.Setenv("VANE_PIPELINE_COMPILED_RUNTIME_ENABLED", "true")
	t.Setenv(
		"VANE_PIPELINE_COMPILED_RUNTIME_CANARY_SCHEDULE_ID",
		"push-effect-task",
	)
	t.Setenv(
		"VANE_PIPELINE_PUSH_EFFECT_CANARY_SCHEDULE_ID",
		"push-effect-task",
	)

	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Pipeline.PushEffectCanaryScheduleID != "push-effect-task" {
		t.Fatalf("pure env push effect canary = %q",
			cfg.Pipeline.PushEffectCanaryScheduleID)
	}
}

func TestPushEffectRecoveryCanaryIsIndependentAndInsideCompiledRollout(
	t *testing.T,
) {
	tests := []struct {
		name     string
		pipeline PipelineConfig
		want     string
		wantErr  bool
	}{
		{name: "default off"},
		{
			name: "recovery only exact task",
			pipeline: PipelineConfig{
				CompiledRuntimeEnabled:             true,
				CompiledRuntimeCanaryScheduleID:    "task-recovery",
				PushEffectRecoveryCanaryScheduleID: " task-recovery ",
			},
			want: "task-recovery",
		},
		{
			name: "recovery independent from different fresh send",
			pipeline: PipelineConfig{
				CompiledRuntimeEnabled:             true,
				CompiledRuntimeAllowAll:            true,
				PushEffectCanaryScheduleID:         "task-fresh",
				PushEffectRecoveryCanaryScheduleID: "task-recovery",
			},
			want: "task-recovery",
		},
		{
			name: "compiled disabled",
			pipeline: PipelineConfig{
				PushEffectRecoveryCanaryScheduleID: "task-recovery",
			},
			wantErr: true,
		},
		{
			name: "outside exact compiled rollout",
			pipeline: PipelineConfig{
				CompiledRuntimeEnabled:             true,
				CompiledRuntimeCanaryScheduleID:    "task-other",
				PushEffectRecoveryCanaryScheduleID: "task-recovery",
			},
			wantErr: true,
		},
		{
			name: "whitespace",
			pipeline: PipelineConfig{
				CompiledRuntimeEnabled:             true,
				CompiledRuntimeCanaryScheduleID:    "task-recovery",
				PushEffectRecoveryCanaryScheduleID: " \t ",
			},
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := Config{
				DB:       DBConfig{URL: "postgres://test"},
				Pipeline: test.pipeline,
			}
			err := cfg.Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("Validate() error=%v wantErr=%v", err, test.wantErr)
			}
			if !test.wantErr &&
				cfg.Pipeline.PushEffectRecoveryCanaryScheduleID != test.want {
				t.Fatalf("recovery canary=%q want=%q",
					cfg.Pipeline.PushEffectRecoveryCanaryScheduleID,
					test.want)
			}
		})
	}
}

func TestPushEffectRecoveryCanaryLoadsFromPureEnvironment(t *testing.T) {
	clearVaneEnv(t)
	skipIfSystemConfigExists(t)
	t.Chdir(t.TempDir())
	t.Setenv("VANE_DB_URL", "postgres://env")
	t.Setenv("VANE_PIPELINE_COMPILED_RUNTIME_ENABLED", "true")
	t.Setenv(
		"VANE_PIPELINE_COMPILED_RUNTIME_CANARY_SCHEDULE_ID",
		"task-recovery",
	)
	t.Setenv(
		"VANE_PIPELINE_PUSH_EFFECT_RECOVERY_CANARY_SCHEDULE_ID",
		"task-recovery",
	)
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Pipeline.PushEffectRecoveryCanaryScheduleID != "task-recovery" ||
		cfg.Pipeline.PushEffectCanaryScheduleID != "" {
		t.Fatalf("pipeline=%+v", cfg.Pipeline)
	}
}

func TestObservationCanaryValidation(t *testing.T) {
	tests := []struct {
		name          string
		compiled      bool
		compiledID    string
		allowAll      bool
		shadow        string
		authority     string
		wantShadow    string
		wantAuthority string
		wantErr       bool
	}{
		{name: "off"},
		{
			name: "shadow only", compiled: true, compiledID: "push-observe",
			shadow: " push-observe ", wantShadow: "push-observe",
		},
		{
			name: "authority exact", compiled: true, compiledID: "push-observe",
			shadow: "push-observe", authority: "push-observe",
			wantShadow: "push-observe", wantAuthority: "push-observe",
		},
		{
			name: "allow all compiled", compiled: true, allowAll: true,
			shadow: "push-observe", authority: "push-observe",
			wantShadow: "push-observe", wantAuthority: "push-observe",
		},
		{name: "disabled", shadow: "push-observe", wantErr: true},
		{
			name: "outside compiled canary", compiled: true,
			compiledID: "push-other", shadow: "push-observe", wantErr: true,
		},
		{
			name: "authority differs", compiled: true, compiledID: "push-observe",
			shadow: "push-observe", authority: "push-other", wantErr: true,
		},
		{
			name: "authority requires shadow", compiled: true, compiledID: "push-observe",
			authority: "push-observe", wantErr: true,
		},
		{
			name: "shadow whitespace only", compiled: true, compiledID: "push-observe",
			shadow: " \t\n ", wantErr: true,
		},
		{
			name: "authority whitespace only", compiled: true, compiledID: "push-observe",
			shadow: "push-observe", authority: " \t\n ", wantErr: true,
		},
		{
			name: "invalid shadow identifier", compiled: true, compiledID: "push-observe",
			shadow: "push\nobserve", wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := Config{
				DB: DBConfig{URL: "postgres://test"},
				Pipeline: PipelineConfig{
					CompiledRuntimeEnabled:               test.compiled,
					CompiledRuntimeCanaryScheduleID:      test.compiledID,
					CompiledRuntimeAllowAll:              test.allowAll,
					ObservationShadowCanaryScheduleID:    test.shadow,
					ObservationAuthorityCanaryScheduleID: test.authority,
				},
			}
			err := cfg.Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("Validate() error=%v wantErr=%v", err, test.wantErr)
			}
			if !test.wantErr &&
				(cfg.Pipeline.ObservationShadowCanaryScheduleID != test.wantShadow ||
					cfg.Pipeline.ObservationAuthorityCanaryScheduleID != test.wantAuthority) {
				t.Fatalf("observation canaries = (%q,%q), want (%q,%q)",
					cfg.Pipeline.ObservationShadowCanaryScheduleID,
					cfg.Pipeline.ObservationAuthorityCanaryScheduleID,
					test.wantShadow, test.wantAuthority)
			}
		})
	}
}

// clearVaneEnv 清掉本进程可能残留的 VANE_ 环境变量，保证测试相互隔离。
// 通过 t.Setenv("", …) 之外的 os.Unsetenv 也能配合 t.Setenv 的自动恢复：
// 先 t.Setenv 注册恢复点，再 os.Unsetenv 真正清空。
func clearVaneEnv(t *testing.T) {
	t.Helper()
	for _, kv := range os.Environ() {
		key, _, ok := strings.Cut(kv, "=")
		if !ok || !strings.HasPrefix(key, "VANE_") {
			continue
		}
		t.Setenv(key, "") // 注册测试结束后的自动恢复
		os.Unsetenv(key)
	}
}

// skipIfSystemConfigExists 跳过依赖"探测不到任何配置文件"前提的测试：
// t.Chdir 只能隔离相对路径候选，defaultConfigPaths 里的绝对路径
// /opt/vane/config/config.yaml 在部署过 Vane 的机器上会被静默读入，
// 使断言给出误导性结果。
func skipIfSystemConfigExists(t *testing.T) {
	t.Helper()
	for _, p := range defaultConfigPaths {
		if !filepath.IsAbs(p) {
			continue
		}
		if _, err := os.Stat(p); err == nil {
			t.Skipf("检测到系统级配置 %s，本测试前提不成立，跳过", p)
		}
	}
}

// writeTempConfig 在临时目录写一个 yaml 配置文件并返回路径。
func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("写临时配置失败: %v", err)
	}
	return path
}

// TestLoadFromFile 验证 yaml 文件字段正确映射到 Config struct。
func TestLoadFromFile(t *testing.T) {
	clearVaneEnv(t)
	path := writeTempConfig(t, `
server:
  addr: ":9090"
db:
  url: "postgres://yaml:5432/vane"
temporal:
  host: "temporal.local:7233"
  namespace: "prod"
  task_queue: "vane-test"
llm:
  provider: "openai"
  base_url: "https://api.openai.com"
  api_key: "yaml-llm-key"
  model: "gpt-x"
  agent_provider: "kimi"
  agent_base_url: "https://api.moonshot.cn/v1"
  agent_api_key: "yaml-agent-key"
  agent_model: "kimi-k2.6"
  max_concurrent: 9
  compiled_endpoint_generation: 2
  compiled_credential_generation: 6
fetch:
  tikhub_api_key: "yaml-tikhub"
  compiled_exa_credential_generation: 3
  compiled_tikhub_credential_generation: 5
  timeout_seconds: 30
  max_response_mb: 8
pipeline:
  playbook_prompts_enabled: true
  playbook_prompt_canary_schedule_id: "schedule-yaml"
  playbook_prompts_allow_all: false
agent:
  max_turns: 15
  token_budget_daily: 50000
log:
  level: "debug"
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"server.addr", cfg.Server.Addr, ":9090"},
		{"db.url", cfg.DB.URL, "postgres://yaml:5432/vane"},
		{"temporal.host", cfg.Temporal.Host, "temporal.local:7233"},
		{"temporal.namespace", cfg.Temporal.Namespace, "prod"},
		{"temporal.task_queue", cfg.Temporal.TaskQueue, "vane-test"},
		{"llm.provider", cfg.LLM.Provider, "openai"},
		{"llm.base_url", cfg.LLM.BaseURL, "https://api.openai.com"},
		{"llm.api_key", cfg.LLM.APIKey, "yaml-llm-key"},
		{"llm.model", cfg.LLM.Model, "gpt-x"},
		{"llm.agent_provider", cfg.LLM.AgentProvider, "kimi"},
		{"llm.agent_base_url", cfg.LLM.AgentBaseURL, "https://api.moonshot.cn/v1"},
		{"llm.agent_api_key", cfg.LLM.AgentAPIKey, "yaml-agent-key"},
		{"llm.agent_model", cfg.LLM.AgentModel, "kimi-k2.6"},
		{"llm.max_concurrent", cfg.LLM.MaxConcurrent, 9},
		{"llm.compiled_endpoint_generation", cfg.LLM.CompiledEndpointGeneration, int64(2)},
		{"llm.compiled_credential_generation", cfg.LLM.CompiledCredentialGeneration, int64(6)},
		{"fetch.tikhub_api_key", cfg.Fetch.TikhubAPIKey, "yaml-tikhub"},
		{"fetch.compiled_exa_credential_generation", cfg.Fetch.CompiledExaCredentialGeneration, int64(3)},
		{"fetch.compiled_tikhub_credential_generation", cfg.Fetch.CompiledTikHubCredentialGeneration, int64(5)},
		{"fetch.timeout_seconds", cfg.Fetch.TimeoutSeconds, 30},
		{"fetch.max_response_mb", cfg.Fetch.MaxResponseMB, 8},
		{"pipeline.playbook_prompts_enabled", cfg.Pipeline.PlaybookPromptsEnabled, true},
		{"pipeline.playbook_prompt_canary_schedule_id", cfg.Pipeline.PlaybookPromptCanaryScheduleID, "schedule-yaml"},
		{"pipeline.playbook_prompts_allow_all", cfg.Pipeline.PlaybookPromptsAllowAll, false},
		{"agent.max_turns", cfg.Agent.MaxTurns, 15},
		{"agent.token_budget_daily", cfg.Agent.TokenBudgetDaily, 50000},
		{"log.level", cfg.Log.Level, "debug"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, 期望 %v", c.name, c.got, c.want)
		}
	}
}

// TestEnvOverridesFile 验证环境变量覆盖 yaml 中已有的值（含所有显式绑定的敏感键）。
func TestEnvOverridesFile(t *testing.T) {
	clearVaneEnv(t)
	path := writeTempConfig(t, `
db:
  url: "postgres://from-yaml"
llm:
  api_key: "yaml-key"
fetch:
  tikhub_api_key: "yaml-tikhub-key"
pipeline:
  playbook_prompts_enabled: false
  playbook_prompt_canary_schedule_id: "schedule-yaml"
  playbook_prompts_allow_all: true
log:
  level: "info"
`)

	t.Setenv("VANE_DB_URL", "postgres://from-env")
	t.Setenv("VANE_LLM_API_KEY", "env-key")
	t.Setenv("VANE_LLM_AGENT_PROVIDER", "kimi")
	t.Setenv("VANE_LLM_AGENT_BASE_URL", "https://api.moonshot.cn/v1")
	t.Setenv("VANE_LLM_AGENT_API_KEY", "env-agent-key")
	t.Setenv("VANE_LLM_AGENT_MODEL", "kimi-k2.6")
	t.Setenv("VANE_LLM_COMPILED_ENDPOINT_GENERATION", "11")
	t.Setenv("VANE_LLM_COMPILED_CREDENTIAL_GENERATION", "13")
	t.Setenv("VANE_FETCH_TIKHUB_API_KEY", "env-tikhub-key")
	t.Setenv("VANE_FETCH_COMPILED_EXA_CREDENTIAL_GENERATION", "7")
	t.Setenv("VANE_FETCH_COMPILED_TIKHUB_CREDENTIAL_GENERATION", "9")
	t.Setenv("VANE_PIPELINE_PLAYBOOK_PROMPTS_ENABLED", "true")
	t.Setenv("VANE_PIPELINE_PLAYBOOK_PROMPT_CANARY_SCHEDULE_ID", "schedule-env")
	t.Setenv("VANE_PIPELINE_PLAYBOOK_PROMPTS_ALLOW_ALL", "false")
	t.Setenv("VANE_LOG_LEVEL", "warn") // 非敏感键走 AutomaticEnv + 默认值注册

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"db.url", cfg.DB.URL, "postgres://from-env"},
		{"llm.api_key", cfg.LLM.APIKey, "env-key"},
		{"llm.agent_provider", cfg.LLM.AgentProvider, "kimi"},
		{"llm.agent_base_url", cfg.LLM.AgentBaseURL, "https://api.moonshot.cn/v1"},
		{"llm.agent_api_key", cfg.LLM.AgentAPIKey, "env-agent-key"},
		{"llm.agent_model", cfg.LLM.AgentModel, "kimi-k2.6"},
		{"llm.compiled_endpoint_generation", cfg.LLM.CompiledEndpointGeneration, int64(11)},
		{"llm.compiled_credential_generation", cfg.LLM.CompiledCredentialGeneration, int64(13)},
		{"fetch.tikhub_api_key", cfg.Fetch.TikhubAPIKey, "env-tikhub-key"},
		{"fetch.compiled_exa_credential_generation", cfg.Fetch.CompiledExaCredentialGeneration, int64(7)},
		{"fetch.compiled_tikhub_credential_generation", cfg.Fetch.CompiledTikHubCredentialGeneration, int64(9)},
		{"pipeline.playbook_prompts_enabled", cfg.Pipeline.PlaybookPromptsEnabled, true},
		{"pipeline.playbook_prompt_canary_schedule_id", cfg.Pipeline.PlaybookPromptCanaryScheduleID, "schedule-env"},
		{"pipeline.playbook_prompts_allow_all", cfg.Pipeline.PlaybookPromptsAllowAll, false},
		{"log.level", cfg.Log.Level, "warn"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, 期望环境变量覆盖为 %v", c.name, c.got, c.want)
		}
	}
}

func TestLoadPlaybookPromptsExplicitAllowAll(t *testing.T) {
	t.Run("yaml", func(t *testing.T) {
		clearVaneEnv(t)
		path := writeTempConfig(t, `
db:
  url: "postgres://yaml"
pipeline:
  playbook_prompts_enabled: true
  playbook_prompt_canary_schedule_id: ""
  playbook_prompts_allow_all: true
`)
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("YAML 显式全量配置应能启动: %v", err)
		}
		if !cfg.Pipeline.PlaybookPromptsEnabled || !cfg.Pipeline.PlaybookPromptsAllowAll ||
			cfg.Pipeline.PlaybookPromptCanaryScheduleID != "" {
			t.Fatalf("YAML 全量配置未接通: %+v", cfg.Pipeline)
		}
	})

	t.Run("environment", func(t *testing.T) {
		clearVaneEnv(t)
		skipIfSystemConfigExists(t)
		t.Chdir(t.TempDir())
		t.Setenv("VANE_DB_URL", "postgres://env")
		t.Setenv("VANE_PIPELINE_PLAYBOOK_PROMPTS_ENABLED", "true")
		t.Setenv("VANE_PIPELINE_PLAYBOOK_PROMPTS_ALLOW_ALL", "true")

		cfg, err := Load("")
		if err != nil {
			t.Fatalf("环境变量显式全量配置应能启动: %v", err)
		}
		if !cfg.Pipeline.PlaybookPromptsEnabled || !cfg.Pipeline.PlaybookPromptsAllowAll ||
			cfg.Pipeline.PlaybookPromptCanaryScheduleID != "" {
			t.Fatalf("环境变量全量配置未接通: %+v", cfg.Pipeline)
		}
	})
}

// TestEnvOnlyNoFile 验证没有任何配置文件时，纯环境变量也能跑通。
func TestEnvOnlyNoFile(t *testing.T) {
	clearVaneEnv(t)
	skipIfSystemConfigExists(t)
	t.Chdir(t.TempDir()) // 隔离 cwd，避免探测到真实 ./config.yaml
	t.Setenv("VANE_DB_URL", "postgres://env-only")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("纯环境变量运行 Load 失败: %v", err)
	}
	if cfg.DB.URL != "postgres://env-only" {
		t.Errorf("db.url = %q, 期望 %q", cfg.DB.URL, "postgres://env-only")
	}
}

// TestEnvOnlyObservationCanaries proves that the exact-task observation
// switches are registered with Viper before AutomaticEnv. Without defaults (or
// an explicit BindEnv), VANE_PIPELINE_OBSERVATION_* is silently omitted by
// Unmarshal in an environment-only deployment.
func TestEnvOnlyObservationCanaries(t *testing.T) {
	clearVaneEnv(t)
	skipIfSystemConfigExists(t)
	t.Chdir(t.TempDir())
	t.Setenv("VANE_DB_URL", "postgres://env")
	t.Setenv("VANE_PIPELINE_COMPILED_RUNTIME_ENABLED", "true")
	t.Setenv("VANE_PIPELINE_COMPILED_RUNTIME_CANARY_SCHEDULE_ID", "push-observe")
	t.Setenv("VANE_PIPELINE_OBSERVATION_SHADOW_CANARY_SCHEDULE_ID", " push-observe ")
	t.Setenv("VANE_PIPELINE_OBSERVATION_AUTHORITY_CANARY_SCHEDULE_ID", "push-observe")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("环境变量 observation canary 配置应能启动: %v", err)
	}
	if cfg.Pipeline.ObservationShadowCanaryScheduleID != "push-observe" ||
		cfg.Pipeline.ObservationAuthorityCanaryScheduleID != "push-observe" {
		t.Fatalf("VANE_PIPELINE_OBSERVATION_* 未进入配置或未规范化: %+v", cfg.Pipeline)
	}
}

// TestEnvOnlyExecutiveBriefCanary proves that every P2-D rollout key is
// registered before AutomaticEnv. Production is environment-only; a missing
// default silently leaves synthesis disabled even while systemd exposes the
// requested variables to the process.
func TestEnvOnlyExecutiveBriefCanary(t *testing.T) {
	clearVaneEnv(t)
	skipIfSystemConfigExists(t)
	t.Chdir(t.TempDir())
	const taskID = "task-executive-env"
	for key, value := range map[string]string{
		"VANE_DB_URL":                                                  "postgres://env",
		"VANE_PIPELINE_COMPILED_RUNTIME_ENABLED":                       "true",
		"VANE_PIPELINE_COMPILED_RUNTIME_CANARY_SCHEDULE_ID":            taskID,
		"VANE_PIPELINE_RUN_OUTCOME_ENABLED":                            "true",
		"VANE_PIPELINE_RUN_OUTCOME_CANARY_SCHEDULE_ID":                 taskID,
		"VANE_PIPELINE_CANONICAL_BRIEF_ENABLED":                        "true",
		"VANE_PIPELINE_CANONICAL_BRIEF_CANARY_SCHEDULE_ID":             taskID,
		"VANE_PIPELINE_STRUCTURED_INSIGHT_ENABLED":                     "true",
		"VANE_PIPELINE_STRUCTURED_INSIGHT_CANARY_SCHEDULE_ID":          taskID,
		"VANE_PIPELINE_CANONICAL_BRIEF_RENDERER_CANARY_SCHEDULE_ID":    taskID,
		"VANE_PIPELINE_STRUCTURED_INSIGHT_RENDERER_ENABLED":            "true",
		"VANE_PIPELINE_STRUCTURED_INSIGHT_RENDERER_CANARY_SCHEDULE_ID": taskID,
		"VANE_PIPELINE_OBSERVATION_SHADOW_CANARY_SCHEDULE_ID":          taskID,
		"VANE_PIPELINE_OBSERVATION_AUTHORITY_CANARY_SCHEDULE_ID":       taskID,
		"VANE_PIPELINE_PUSH_EFFECT_CANARY_SCHEDULE_ID":                 taskID,
		"VANE_PIPELINE_PUSH_EFFECT_RECOVERY_CANARY_SCHEDULE_ID":        taskID,
		"VANE_PIPELINE_STRUCTURED_EVENT_EVIDENCE_ENABLED":              "true",
		"VANE_PIPELINE_STRUCTURED_EVENT_EVIDENCE_CANARY_SCHEDULE_ID":   taskID,
		"VANE_PIPELINE_EXECUTIVE_BRIEF_ENABLED":                        "true",
		"VANE_PIPELINE_EXECUTIVE_BRIEF_CANARY_SCHEDULE_ID":             taskID,
		"VANE_PIPELINE_EXECUTIVE_BRIEF_WEB_CANARY_SCHEDULE_ID":         taskID,
	} {
		t.Setenv(key, value)
	}

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("环境变量 executive Brief canary 配置应能启动: %v", err)
	}
	if !cfg.Pipeline.ExecutiveBriefEnabled ||
		cfg.Pipeline.ExecutiveBriefCanaryScheduleID != taskID ||
		cfg.Pipeline.ExecutiveBriefAllowAll ||
		cfg.Pipeline.ExecutiveBriefWebCanaryScheduleID != taskID ||
		cfg.Pipeline.ExecutiveBriefRendererCanaryScheduleID != "" {
		t.Fatalf(
			"VANE_PIPELINE_EXECUTIVE_BRIEF_* 未进入配置或被改写: %+v",
			cfg.Pipeline,
		)
	}
}

// TestDefaults 验证内置默认值与 config.example.yaml 一致。
func TestDefaults(t *testing.T) {
	clearVaneEnv(t)
	skipIfSystemConfigExists(t)
	t.Chdir(t.TempDir())
	t.Setenv("VANE_DB_URL", "postgres://env") // 满足必填校验

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"server.addr", cfg.Server.Addr, "127.0.0.1:8080"},
		{"temporal.host", cfg.Temporal.Host, "127.0.0.1:7233"},
		{"temporal.namespace", cfg.Temporal.Namespace, "default"},
		{"temporal.task_queue", cfg.Temporal.TaskQueue, "vane-push"},
		{"llm.provider", cfg.LLM.Provider, "deepseek"},
		{"llm.base_url", cfg.LLM.BaseURL, "https://api.deepseek.com"},
		{"llm.model", cfg.LLM.Model, "deepseek-v4-flash"},
		{"llm.agent_provider", cfg.LLM.AgentProvider, ""},
		{"llm.agent_base_url", cfg.LLM.AgentBaseURL, ""},
		{"llm.agent_model", cfg.LLM.AgentModel, "deepseek-v4-pro"},
		{"llm.max_concurrent", cfg.LLM.MaxConcurrent, 32},
		{"llm.compiled_endpoint_generation", cfg.LLM.CompiledEndpointGeneration, int64(1)},
		{"llm.compiled_credential_generation", cfg.LLM.CompiledCredentialGeneration, int64(1)},
		{"fetch.compiled_exa_credential_generation", cfg.Fetch.CompiledExaCredentialGeneration, int64(1)},
		{"fetch.compiled_tikhub_credential_generation", cfg.Fetch.CompiledTikHubCredentialGeneration, int64(1)},
		{"fetch.timeout_seconds", cfg.Fetch.TimeoutSeconds, 20},
		{"fetch.max_response_mb", cfg.Fetch.MaxResponseMB, 5},
		{"pipeline.playbook_prompts_enabled", cfg.Pipeline.PlaybookPromptsEnabled, false},
		{"pipeline.playbook_prompt_canary_schedule_id", cfg.Pipeline.PlaybookPromptCanaryScheduleID, ""},
		{"pipeline.playbook_prompts_allow_all", cfg.Pipeline.PlaybookPromptsAllowAll, false},
		{"pipeline.observation_shadow_canary_schedule_id", cfg.Pipeline.ObservationShadowCanaryScheduleID, ""},
		{"pipeline.observation_authority_canary_schedule_id", cfg.Pipeline.ObservationAuthorityCanaryScheduleID, ""},
		{"pipeline.push_effect_recovery_canary_schedule_id", cfg.Pipeline.PushEffectRecoveryCanaryScheduleID, ""},
		{"pipeline.canonical_brief_renderer_canary_schedule_id", cfg.Pipeline.CanonicalBriefRendererCanaryScheduleID, ""},
		{"pipeline.structured_insight_enabled", cfg.Pipeline.StructuredInsightEnabled, false},
		{"pipeline.structured_insight_canary_schedule_id", cfg.Pipeline.StructuredInsightCanaryScheduleID, ""},
		{"pipeline.structured_insight_allow_all", cfg.Pipeline.StructuredInsightAllowAll, false},
		{"pipeline.structured_insight_renderer_enabled", cfg.Pipeline.StructuredInsightRendererEnabled, false},
		{"pipeline.structured_insight_renderer_canary_schedule_id", cfg.Pipeline.StructuredInsightRendererCanaryScheduleID, ""},
		{"pipeline.structured_insight_renderer_allow_all", cfg.Pipeline.StructuredInsightRendererAllowAll, false},
		{"pipeline.structured_event_evidence_enabled", cfg.Pipeline.StructuredEventEvidenceEnabled, false},
		{"pipeline.structured_event_evidence_canary_schedule_id", cfg.Pipeline.StructuredEventEvidenceCanaryScheduleID, ""},
		{"pipeline.structured_event_evidence_allow_all", cfg.Pipeline.StructuredEventEvidenceAllowAll, false},
		{"pipeline.executive_brief_enabled", cfg.Pipeline.ExecutiveBriefEnabled, false},
		{"pipeline.executive_brief_canary_schedule_id", cfg.Pipeline.ExecutiveBriefCanaryScheduleID, ""},
		{"pipeline.executive_brief_allow_all", cfg.Pipeline.ExecutiveBriefAllowAll, false},
		{"pipeline.executive_brief_web_canary_schedule_id", cfg.Pipeline.ExecutiveBriefWebCanaryScheduleID, ""},
		{"pipeline.executive_brief_renderer_canary_schedule_id", cfg.Pipeline.ExecutiveBriefRendererCanaryScheduleID, ""},
		{"agent.max_turns", cfg.Agent.MaxTurns, 20},
		{"agent.token_budget_daily", cfg.Agent.TokenBudgetDaily, 100000},
		{"agent.session_ttl_minutes", cfg.Agent.SessionTTLMinutes, 30},
		{"log.level", cfg.Log.Level, "info"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, 期望默认值 %v", c.name, c.got, c.want)
		}
	}
}

func TestLLMConfigAgentClientConfig(t *testing.T) {
	primary := LLMConfig{
		Provider:      "deepseek",
		BaseURL:       "https://api.deepseek.com",
		APIKey:        "deepseek-key",
		Model:         "deepseek-v4-flash",
		AgentModel:    "deepseek-v4-pro",
		MaxConcurrent: 32,
	}

	t.Run("legacy route inherits primary endpoint and key", func(t *testing.T) {
		got, err := primary.AgentClientConfig()
		if err != nil {
			t.Fatalf("AgentClientConfig() error = %v", err)
		}
		if got.Provider != "deepseek" || got.BaseURL != "https://api.deepseek.com" ||
			got.APIKey != "deepseek-key" || got.Model != "deepseek-v4-pro" {
			t.Fatalf("legacy agent route = %+v", got)
		}
	})

	t.Run("dedicated route does not reuse primary credential", func(t *testing.T) {
		cfg := primary
		cfg.AgentProvider = "kimi"
		cfg.AgentBaseURL = "https://api.moonshot.cn/v1"
		cfg.AgentAPIKey = "kimi-key"
		cfg.AgentModel = "kimi-k2.6"

		got, err := cfg.AgentClientConfig()
		if err != nil {
			t.Fatalf("AgentClientConfig() error = %v", err)
		}
		if got.Provider != "kimi" || got.BaseURL != "https://api.moonshot.cn/v1" ||
			got.APIKey != "kimi-key" || got.Model != "kimi-k2.6" {
			t.Fatalf("dedicated agent route = %+v", got)
		}
	})

	t.Run("partial dedicated route fails closed", func(t *testing.T) {
		cfg := primary
		cfg.AgentProvider = "kimi"
		if _, err := cfg.AgentClientConfig(); err == nil {
			t.Fatal("partial agent route should be rejected")
		}
	})
}

func TestValidate_NormalizesPlaybookPromptCanaryScheduleID(t *testing.T) {
	tests := []struct {
		name     string
		enabled  bool
		allowAll bool
		input    string
		want     string
		wantErr  bool
	}{
		{name: "empty without explicit all is rejected", enabled: true, wantErr: true},
		{name: "explicit all allows empty canary", enabled: true, allowAll: true},
		{name: "surrounding whitespace is trimmed", enabled: true, input: "  task-canary  ", want: "task-canary"},
		{name: "enabled whitespace only is rejected", enabled: true, input: " \t\n ", wantErr: true},
		{name: "canary and all are ambiguous", enabled: true, allowAll: true, input: "task-canary", wantErr: true},
		{name: "disabled remains a rollback even with whitespace", allowAll: true, input: " \t\n "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				DB: DBConfig{URL: "postgres://test"},
				Pipeline: PipelineConfig{
					PlaybookPromptsEnabled:         tt.enabled,
					PlaybookPromptCanaryScheduleID: tt.input,
					PlaybookPromptsAllowAll:        tt.allowAll,
				},
			}
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v，wantErr=%v", err, tt.wantErr)
			}
			if !tt.wantErr && cfg.Pipeline.PlaybookPromptCanaryScheduleID != tt.want {
				t.Fatalf("canary schedule id = %q，期望 %q", cfg.Pipeline.PlaybookPromptCanaryScheduleID, tt.want)
			}
		})
	}
}

func TestValidateCompiledRouteGenerations(t *testing.T) {
	t.Run("zero defaults to primary generation", func(t *testing.T) {
		cfg := Config{DB: DBConfig{URL: "postgres://test"}}
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		if cfg.Fetch.CompiledExaCredentialGeneration != 1 ||
			cfg.Fetch.CompiledTikHubCredentialGeneration != 1 {
			t.Fatalf("fetch generations = (%d, %d), want (1, 1)",
				cfg.Fetch.CompiledExaCredentialGeneration,
				cfg.Fetch.CompiledTikHubCredentialGeneration)
		}
		if cfg.LLM.CompiledEndpointGeneration != 1 ||
			cfg.LLM.CompiledCredentialGeneration != 1 {
			t.Fatalf("LLM generations = (%d, %d), want (1, 1)",
				cfg.LLM.CompiledEndpointGeneration,
				cfg.LLM.CompiledCredentialGeneration)
		}
	})

	for _, test := range []struct {
		name  string
		fetch FetchConfig
		llm   LLMConfig
	}{
		{name: "negative LLM endpoint", llm: LLMConfig{CompiledEndpointGeneration: -1}},
		{name: "negative LLM credential", llm: LLMConfig{CompiledCredentialGeneration: -1}},
		{name: "negative Exa", fetch: FetchConfig{CompiledExaCredentialGeneration: -1}},
		{name: "negative TikHub", fetch: FetchConfig{CompiledTikHubCredentialGeneration: -1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := Config{
				DB: DBConfig{URL: "postgres://test"}, LLM: test.llm, Fetch: test.fetch,
			}
			if err := cfg.Validate(); err == nil {
				t.Fatal("negative compiled route generation must be rejected")
			}
		})
	}
}

// TestMissingDBURL 验证缺少 db.url 时 Load 报错。
func TestMissingDBURL(t *testing.T) {
	clearVaneEnv(t)
	skipIfSystemConfigExists(t)
	t.Chdir(t.TempDir())

	if _, err := Load(""); err == nil {
		t.Fatal("缺少 db.url 时 Load 应报错，实际返回 nil")
	} else if !strings.Contains(err.Error(), "db.url") {
		t.Errorf("错误信息应提及 db.url，实际: %v", err)
	}
}

// TestServerAddrDefaultAfterEmpty 验证配置文件显式置空 server.addr 时回退到默认 loopback 地址。
func TestServerAddrDefaultAfterEmpty(t *testing.T) {
	clearVaneEnv(t)
	path := writeTempConfig(t, `
server:
  addr: ""
db:
  url: "postgres://x"
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	if cfg.Server.Addr != "127.0.0.1:8080" {
		t.Errorf("server.addr = %q, 期望回退默认值 %q", cfg.Server.Addr, "127.0.0.1:8080")
	}
}

// TestServerAddrDefaultBindsLoopback 安全回归（纵深加固）：默认监听地址必须只绑本机
// loopback，不得回退成 ":8080" / "0.0.0.0:8080"——绑全网卡会让 8080 一旦公网可达即被
// 直连绕过 Caddy 反代与 TLS（并使 clientIP 的 XFF 可信链路失效）。
// 直接断言 setDefaults 注册的默认值、绕过配置文件探测：本安全护栏必须永远执行，不能
// 因宿主机存在 /opt/vane/config/config.yaml 而被 skipIfSystemConfigExists 静默跳过。
func TestServerAddrDefaultBindsLoopback(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	if got := v.GetString("server.addr"); got != "127.0.0.1:8080" {
		t.Fatalf("默认监听地址必须只绑 loopback，实际 %q——绑 0.0.0.0 会让 8080 公网直连绕过 Caddy", got)
	}
}

// TestServerAddrEnvOverride 验证逃生阀：确需对外监听时可用 VANE_SERVER_ADDR 覆盖默认
// loopback 绑定（ServerConfig.Addr 注释与 deploy/README 都指向这条路径，必须真的生效）。
func TestServerAddrEnvOverride(t *testing.T) {
	clearVaneEnv(t)
	skipIfSystemConfigExists(t)
	t.Chdir(t.TempDir())
	t.Setenv("VANE_DB_URL", "postgres://env")
	t.Setenv("VANE_SERVER_ADDR", "0.0.0.0:8080")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	if cfg.Server.Addr != "0.0.0.0:8080" {
		t.Fatalf("VANE_SERVER_ADDR 应覆盖默认 loopback 绑定，实际 %q", cfg.Server.Addr)
	}
}

// TestExplicitPathMissing 验证显式指定的配置文件不存在时报错（与自动探测不同）。
func TestExplicitPathMissing(t *testing.T) {
	clearVaneEnv(t)
	t.Setenv("VANE_DB_URL", "postgres://x")

	missing := filepath.Join(t.TempDir(), "no-such-config.yaml")
	if _, err := Load(missing); err == nil {
		t.Fatal("显式路径不存在时 Load 应报错，实际返回 nil")
	}
}

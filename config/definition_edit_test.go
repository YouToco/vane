package config

import "testing"

func TestDefinitionEditFeatureFlagDefaultsOffAndReadsEnvironment(t *testing.T) {
	t.Run("default off", func(t *testing.T) {
		clearVaneEnv(t)
		skipIfSystemConfigExists(t)
		t.Chdir(t.TempDir())
		t.Setenv("VANE_DB_URL", "postgres://env")

		cfg, err := Load("")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Agent.DefinitionEditEnabled {
			t.Fatal("definition editing must be disabled by default")
		}
	})

	t.Run("explicit environment enable", func(t *testing.T) {
		clearVaneEnv(t)
		skipIfSystemConfigExists(t)
		t.Chdir(t.TempDir())
		t.Setenv("VANE_DB_URL", "postgres://env")
		t.Setenv("VANE_AGENT_DEFINITION_EDIT_ENABLED", "true")

		cfg, err := Load("")
		if err != nil {
			t.Fatal(err)
		}
		if !cfg.Agent.DefinitionEditEnabled {
			t.Fatal("VANE_AGENT_DEFINITION_EDIT_ENABLED was not applied")
		}
	})
}

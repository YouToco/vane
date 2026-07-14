package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
  max_concurrent: 9
feishu:
  app_id: "cli_yaml"
  app_secret: "yaml-secret"
  rate_interval_ms: 500
fetch:
  rss_concurrency: 7
  tikhub_concurrency: 2
  tikhub_api_key: "yaml-tikhub"
  timeout_seconds: 30
  max_response_mb: 8
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
		{"llm.max_concurrent", cfg.LLM.MaxConcurrent, 9},
		{"feishu.app_id", cfg.Feishu.AppID, "cli_yaml"},
		{"feishu.app_secret", cfg.Feishu.AppSecret, "yaml-secret"},
		{"feishu.rate_interval_ms", cfg.Feishu.RateIntervalMS, 500},
		{"fetch.rss_concurrency", cfg.Fetch.RSSConcurrency, 7},
		{"fetch.tikhub_concurrency", cfg.Fetch.TikhubConcurrency, 2},
		{"fetch.tikhub_api_key", cfg.Fetch.TikhubAPIKey, "yaml-tikhub"},
		{"fetch.timeout_seconds", cfg.Fetch.TimeoutSeconds, 30},
		{"fetch.max_response_mb", cfg.Fetch.MaxResponseMB, 8},
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
feishu:
  app_id: "yaml-app-id"
  app_secret: "yaml-app-secret"
fetch:
  tikhub_api_key: "yaml-tikhub-key"
log:
  level: "info"
`)

	t.Setenv("VANE_DB_URL", "postgres://from-env")
	t.Setenv("VANE_LLM_API_KEY", "env-key")
	t.Setenv("VANE_FEISHU_APP_ID", "env-app-id")
	t.Setenv("VANE_FEISHU_APP_SECRET", "env-app-secret")
	t.Setenv("VANE_FETCH_TIKHUB_API_KEY", "env-tikhub-key")
	t.Setenv("VANE_LOG_LEVEL", "warn") // 非敏感键走 AutomaticEnv + 默认值注册

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}

	checks := []struct {
		name string
		got  string
		want string
	}{
		{"db.url", cfg.DB.URL, "postgres://from-env"},
		{"llm.api_key", cfg.LLM.APIKey, "env-key"},
		{"feishu.app_id", cfg.Feishu.AppID, "env-app-id"},
		{"feishu.app_secret", cfg.Feishu.AppSecret, "env-app-secret"},
		{"fetch.tikhub_api_key", cfg.Fetch.TikhubAPIKey, "env-tikhub-key"},
		{"log.level", cfg.Log.Level, "warn"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %q, 期望环境变量覆盖为 %q", c.name, c.got, c.want)
		}
	}
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
		{"server.addr", cfg.Server.Addr, ":8080"},
		{"temporal.host", cfg.Temporal.Host, "127.0.0.1:7233"},
		{"temporal.namespace", cfg.Temporal.Namespace, "default"},
		{"temporal.task_queue", cfg.Temporal.TaskQueue, "vane-push"},
		{"llm.provider", cfg.LLM.Provider, "deepseek"},
		{"llm.base_url", cfg.LLM.BaseURL, "https://api.deepseek.com"},
		{"llm.model", cfg.LLM.Model, "deepseek-v4-flash"},
		{"llm.agent_model", cfg.LLM.AgentModel, "deepseek-v4-pro"},
		{"llm.max_concurrent", cfg.LLM.MaxConcurrent, 5},
		{"feishu.rate_interval_ms", cfg.Feishu.RateIntervalMS, 750},
		{"fetch.rss_concurrency", cfg.Fetch.RSSConcurrency, 10},
		{"fetch.tikhub_concurrency", cfg.Fetch.TikhubConcurrency, 3},
		{"fetch.timeout_seconds", cfg.Fetch.TimeoutSeconds, 20},
		{"fetch.max_response_mb", cfg.Fetch.MaxResponseMB, 5},
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

// TestServerAddrDefaultAfterEmpty 验证配置文件显式置空 server.addr 时回退到 ":8080"。
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
	if cfg.Server.Addr != ":8080" {
		t.Errorf("server.addr = %q, 期望回退默认值 %q", cfg.Server.Addr, ":8080")
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

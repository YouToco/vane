// Package config 提供 Vane 的配置加载：YAML 文件 + VANE_ 前缀环境变量覆盖（Viper）。
//
// 优先级：环境变量 > 配置文件 > 内置默认值。
// 敏感值（数据库连接串、各类 API key）不应写入配置文件，一律走环境变量。
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// defaultConfigPaths 是 path 参数为空时按顺序探测的配置文件位置。
var defaultConfigPaths = []string{
	"./config.yaml",
	"/opt/vane/config/config.yaml",
}

// Config 是 Vane 的全量配置，结构与 config.example.yaml 一一对应。
type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	DB       DBConfig       `mapstructure:"db"`
	Temporal TemporalConfig `mapstructure:"temporal"`
	LLM      LLMConfig      `mapstructure:"llm"`
	Feishu   FeishuConfig   `mapstructure:"feishu"`
	Fetch    FetchConfig    `mapstructure:"fetch"`
	Agent    AgentConfig    `mapstructure:"agent"`
	Log      LogConfig      `mapstructure:"log"`

	Dashboard DashboardConfig `mapstructure:"dashboard"`
	A2A       A2AConfig       `mapstructure:"a2a"`
}

// ServerConfig 是 HTTP 服务配置。
type ServerConfig struct {
	// Addr 是 HTTP 监听地址，默认 ":8080"。
	Addr string `mapstructure:"addr"`
}

// DBConfig 是 Postgres 连接配置。
type DBConfig struct {
	// URL 是 Postgres 连接串（必填），环境变量 VANE_DB_URL。
	URL string `mapstructure:"url"`
}

// TemporalConfig 是 Temporal 集群与任务队列配置。
type TemporalConfig struct {
	Host      string `mapstructure:"host"`
	Namespace string `mapstructure:"namespace"`
	TaskQueue string `mapstructure:"task_queue"`
}

// LLMConfig 是大模型服务配置。
type LLMConfig struct {
	Provider string `mapstructure:"provider"`
	BaseURL  string `mapstructure:"base_url"`
	// APIKey 环境变量 VANE_LLM_API_KEY。
	APIKey string `mapstructure:"api_key"`
	Model  string `mapstructure:"model"`
	// AgentModel 是 agent loop（function calling）使用的模型，与 Model
	//（摘要/评分等高频便宜档）分离：FC 多轮决策的质量依赖高档模型
	//（v4-pro 实测 60/60 全过，M4 事实基准），成本按调用面分账。
	AgentModel    string `mapstructure:"agent_model"`
	MaxConcurrent int    `mapstructure:"max_concurrent"`
}

// FeishuConfig 是飞书推送渠道配置。
type FeishuConfig struct {
	// AppID 环境变量 VANE_FEISHU_APP_ID。
	AppID string `mapstructure:"app_id"`
	// AppSecret 环境变量 VANE_FEISHU_APP_SECRET。
	AppSecret string `mapstructure:"app_secret"`
	// RateIntervalMS 是消息发送的最小间隔（毫秒），用于限流。
	RateIntervalMS int `mapstructure:"rate_interval_ms"`
}

// FetchConfig 是内容抓取配置。
type FetchConfig struct {
	RSSConcurrency    int `mapstructure:"rss_concurrency"`
	TikhubConcurrency int `mapstructure:"tikhub_concurrency"`
	// TikhubAPIKey 环境变量 VANE_FETCH_TIKHUB_API_KEY。
	TikhubAPIKey string `mapstructure:"tikhub_api_key"`
	// ExaAPIKey 环境变量 VANE_FETCH_EXA_API_KEY。
	ExaAPIKey      string `mapstructure:"exa_api_key"`
	TimeoutSeconds int    `mapstructure:"timeout_seconds"`
	MaxResponseMB  int    `mapstructure:"max_response_mb"`
}

// AgentConfig 是 agent loop 运行约束配置。
type AgentConfig struct {
	MaxTurns         int `mapstructure:"max_turns"`
	TokenBudgetDaily int `mapstructure:"token_budget_daily"`
	// SessionTTLMinutes 是会话闲置过期窗口（分钟）：同一 owner 在窗口内的
	// 消息共享一个多轮会话（上下文连续），超时后新开会话（契约 §0）。
	SessionTTLMinutes int `mapstructure:"session_ttl_minutes"`
}

// LogConfig 是日志配置。
type LogConfig struct {
	Level string `mapstructure:"level"`
}

// DashboardConfig 是 Web Dashboard 配置。
type DashboardConfig struct {
	// Password 是 Dashboard 登录密码，环境变量 VANE_DASHBOARD_PASSWORD。
	// 为空时不 fail（允许无 Dashboard 场景），api 层对登录一律 401。
	Password string `mapstructure:"password"`
}

// A2AConfig 是 A2A server 配置（a2a-contract §6）。
type A2AConfig struct {
	// Enabled 默认 false：未显式开启时 main.go 不 Mount，零新增暴露面。
	Enabled bool `mapstructure:"enabled"`
	// Token 环境变量 VANE_A2A_TOKEN；本体存 YouToco/my-credentials，绝不入库。
	// 为空时照常挂载、auth 恒 401、Mount 时 slog.Warn 一次（Dashboard Password 先例）。
	Token string `mapstructure:"token"`
	// BaseURL 是对外 A2A endpoint，进 AgentCard supportedInterfaces。
	BaseURL string `mapstructure:"base_url"`
}

// sensitiveKeys 需要显式 BindEnv：Viper 的 AutomaticEnv 只对"已知键"
// （有默认值或出现在配置文件中）生效，纯环境变量运行时嵌套敏感键会漏读。
var sensitiveKeys = []string{
	"db.url",
	"llm.api_key",
	"feishu.app_id",
	"feishu.app_secret",
	"fetch.tikhub_api_key",
	"fetch.exa_api_key",
	"dashboard.password",
	"a2a.token",
}

// Load 加载配置并校验。
//
// path 非空时必须指向存在且合法的 YAML 文件，否则报错；
// path 为空时按 ./config.yaml、/opt/vane/config/config.yaml 顺序探测，
// 都不存在也不报错——允许纯环境变量运行。
// 环境变量前缀 VANE_，嵌套键的 "." 与 "-" 折算为 "_"，
// 如 VANE_DB_URL 覆盖 db.url，VANE_LLM_API_KEY 覆盖 llm.api_key。
func Load(path string) (*Config, error) {
	v := viper.New()

	// 非敏感键注册默认值：既提供缺省行为，也让 AutomaticEnv 能识别这些键。
	setDefaults(v)

	v.SetEnvPrefix("VANE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	// 敏感键显式绑定（见 sensitiveKeys 注释）。
	for _, key := range sensitiveKeys {
		if err := v.BindEnv(key); err != nil {
			return nil, fmt.Errorf("config: 绑定环境变量 %s 失败: %w", key, err)
		}
	}

	if err := readConfigFile(v, path); err != nil {
		return nil, err
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("config: 解析配置失败: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// setDefaults 注册与 config.example.yaml 一致的非敏感默认值。
func setDefaults(v *viper.Viper) {
	v.SetDefault("server.addr", ":8080")

	v.SetDefault("temporal.host", "127.0.0.1:7233")
	v.SetDefault("temporal.namespace", "default")
	v.SetDefault("temporal.task_queue", "vane-push")

	v.SetDefault("llm.provider", "deepseek")
	v.SetDefault("llm.base_url", "https://api.deepseek.com")
	// deepseek-chat/deepseek-reasoner 为旧别名，2026-07-24 起废弃；默认用便宜档 v4-flash。
	v.SetDefault("llm.model", "deepseek-v4-flash")
	v.SetDefault("llm.agent_model", "deepseek-v4-pro")
	v.SetDefault("llm.max_concurrent", 5)

	v.SetDefault("feishu.rate_interval_ms", 750)

	v.SetDefault("fetch.rss_concurrency", 10)
	v.SetDefault("fetch.tikhub_concurrency", 3)
	v.SetDefault("fetch.timeout_seconds", 20)
	v.SetDefault("fetch.max_response_mb", 5)

	v.SetDefault("agent.max_turns", 20)
	v.SetDefault("agent.token_budget_daily", 100000)
	v.SetDefault("agent.session_ttl_minutes", 30)

	v.SetDefault("log.level", "info")

	// a2a：enabled/base_url 有默认值使 AutomaticEnv 认识对应环境变量；
	// token 无默认值，走 sensitiveKeys 显式 BindEnv（契约 §6"三处缺一不可"）。
	v.SetDefault("a2a.enabled", false)
	v.SetDefault("a2a.base_url", "https://api.vane.zhuoqidev.com/a2a")
}

// readConfigFile 按 Load 的规则定位并读取配置文件。
func readConfigFile(v *viper.Viper, path string) error {
	if path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return fmt.Errorf("config: 读取配置文件 %s 失败: %w", path, err)
		}
		return nil
	}
	for _, candidate := range defaultConfigPaths {
		if _, err := os.Stat(candidate); err != nil {
			continue // 不存在或不可访问则尝试下一个候选。
		}
		v.SetConfigFile(candidate)
		if err := v.ReadInConfig(); err != nil {
			return fmt.Errorf("config: 读取配置文件 %s 失败: %w", candidate, err)
		}
		return nil
	}
	// 所有候选都不存在：允许纯环境变量运行。
	return nil
}

// Validate 校验必填项并补齐零值默认值。
// 允许在 Unmarshal 后单独调用（如配置热更新场景）。
func (c *Config) Validate() error {
	if c.Server.Addr == "" {
		c.Server.Addr = ":8080"
	}
	if c.DB.URL == "" {
		return errors.New("config: db.url 必填（可通过环境变量 VANE_DB_URL 设置）")
	}
	// 密码为空只告警不拒启动：允许纯后端（无 Dashboard）场景；
	// api 层会在密码为空时对登录一律 401，不会形成裸奔入口。
	if c.Dashboard.Password == "" {
		slog.Warn("config: dashboard.password 未配置，Dashboard 登录将不可用（可通过环境变量 VANE_DASHBOARD_PASSWORD 设置）")
	}
	return nil
}

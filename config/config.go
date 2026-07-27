// Package config 提供 Vane 的配置加载：YAML 文件 + VANE_ 前缀环境变量覆盖（Viper）。
//
// 优先级：环境变量 > 配置文件 > 内置默认值。
// 敏感值（数据库连接串、各类 API key）不应写入配置文件，一律走环境变量。
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"

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
	Fetch    FetchConfig    `mapstructure:"fetch"`
	Pipeline PipelineConfig `mapstructure:"pipeline"`
	Agent    AgentConfig    `mapstructure:"agent"`
	Log      LogConfig      `mapstructure:"log"`

	Dashboard DashboardConfig `mapstructure:"dashboard"`
	A2A       A2AConfig       `mapstructure:"a2a"`
}

// ServerConfig 是 HTTP 服务配置。
type ServerConfig struct {
	// Addr 是 HTTP 监听地址，默认 "127.0.0.1:8080"——只绑本机 loopback、不对公网暴露。
	// 生产由 Caddy（host 网络）经 127.0.0.1 反代；若绑 0.0.0.0，8080 一旦公网可达即可被
	// 直连绕过 Caddy（绕 TLS、绕反代加固，且 Caddy 追加真实 peer 的 XFF 链路失效——见
	// api/ratelimit.go clientIP）。确需对外监听时用 VANE_SERVER_ADDR 或配置文件显式覆盖
	// （如 "0.0.0.0:8080"，并自行收好防火墙）。
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
	// AgentProvider/AgentBaseURL/AgentAPIKey 是可选的 Agent 专用路由。三者均为空时，
	// Agent 继承主路由；只要设置任意一项就必须三项齐全，避免把主路由凭证误发到
	// 另一家供应商。AgentAPIKey 环境变量 VANE_LLM_AGENT_API_KEY。
	AgentProvider string `mapstructure:"agent_provider"`
	AgentBaseURL  string `mapstructure:"agent_base_url"`
	AgentAPIKey   string `mapstructure:"agent_api_key"`
	// AgentModel 是 agent loop（function calling）使用的模型，与 Model
	//（摘要/评分等高频便宜档）分离：FC 多轮决策的质量依赖高档模型
	//（v4-pro 实测 60/60 全过，M4 事实基准），成本按调用面分账。
	AgentModel    string `mapstructure:"agent_model"`
	MaxConcurrent int    `mapstructure:"max_concurrent"`
	// CompiledEndpointGeneration and CompiledCredentialGeneration are opaque
	// route generations for immutable task-run snapshots. Any endpoint or key
	// rotation must bump the corresponding generation; an old snapshot then
	// resolves an explicitly retained route or fails closed, never the current
	// client by accident.
	CompiledEndpointGeneration   int64 `mapstructure:"compiled_endpoint_generation"`
	CompiledCredentialGeneration int64 `mapstructure:"compiled_credential_generation"`
}

// FetchConfig 是内容抓取配置。
//
// 注：飞书凭证不在此配置——AppID/AppSecret 由 Dashboard 向导写入 settings 表、
// feishu/manager.go 每次连接前从 store 重读（见其注释），config 侧无 FeishuConfig。
type FetchConfig struct {
	// TikhubAPIKey 环境变量 VANE_FETCH_TIKHUB_API_KEY。
	TikhubAPIKey string `mapstructure:"tikhub_api_key"`
	// ExaAPIKey 环境变量 VANE_FETCH_EXA_API_KEY。
	ExaAPIKey string `mapstructure:"exa_api_key"`
	// Compiled*CredentialGeneration purpose-bind the concrete retained
	// provider clients used by immutable task-run snapshots. Rotating a key
	// or provider endpoint requires a new generation and a separately retained
	// client; a missing old generation fails closed.
	CompiledExaCredentialGeneration    int64 `mapstructure:"compiled_exa_credential_generation"`
	CompiledTikHubCredentialGeneration int64 `mapstructure:"compiled_tikhub_credential_generation"`
	TimeoutSeconds                     int   `mapstructure:"timeout_seconds"`
	MaxResponseMB                      int   `mapstructure:"max_response_mb"`
}

// PipelineConfig 是固定推送流水线的运行策略配置。
type PipelineConfig struct {
	// PlaybookPromptsEnabled 控制任务手册是否注入 scorer/cardgen prompt。
	// 默认关闭；开启后可用 PlaybookPromptCanaryScheduleID 限定单任务灰度。
	PlaybookPromptsEnabled bool `mapstructure:"playbook_prompts_enabled"`
	// PlaybookPromptCanaryScheduleID 非空时只允许该任务注入；为空时还必须显式
	// PlaybookPromptsAllowAll 才能全量开启。
	PlaybookPromptCanaryScheduleID string `mapstructure:"playbook_prompt_canary_schedule_id"`
	// PlaybookPromptsAllowAll 是全量放量的第二把钥匙。只有 Enabled=true、
	// canary ID 为空且本值为 true 才允许全量，防止只漏配 canary 就误放量。
	PlaybookPromptsAllowAll bool `mapstructure:"playbook_prompts_allow_all"`
	// CompiledRuntimeEnabled controls the C1b immutable PrepareRun path. It is
	// dark by default and must be narrowed to one task or explicitly allowed
	// for all tasks; changing process config only rewrites durable Schedule
	// Actions, so Workflow determinism never depends on mutable config.
	CompiledRuntimeEnabled bool `mapstructure:"compiled_runtime_enabled"`
	// CompiledRuntimeCanaryScheduleID selects the sole stable task allowed onto
	// C1b while canarying. Empty requires CompiledRuntimeAllowAll=true.
	CompiledRuntimeCanaryScheduleID string `mapstructure:"compiled_runtime_canary_schedule_id"`
	// CompiledRuntimeAllowAll is the deliberate second key for broad rollout.
	CompiledRuntimeAllowAll bool `mapstructure:"compiled_runtime_allow_all"`
	// RunOutcome* is the independent P1-B lifecycle rollout. It may select only
	// Actions already selected by the compiled runtime rollout.
	RunOutcomeEnabled          bool   `mapstructure:"run_outcome_enabled"`
	RunOutcomeCanaryScheduleID string `mapstructure:"run_outcome_canary_schedule_id"`
	RunOutcomeAllowAll         bool   `mapstructure:"run_outcome_allow_all"`
	// SnapshotV2ShadowCanaryScheduleID enables C2c-2 shadow persistence for
	// exactly one task. Empty is the complete rollback/off state.
	SnapshotV2ShadowCanaryScheduleID string `mapstructure:"snapshot_v2_shadow_canary_schedule_id"`
	// SnapshotV2ReadAuditCanaryScheduleID enables C2c-3a observation-only
	// typed materialization for exactly the same task as the shadow writer.
	// Empty is the complete rollback/off state; v1 remains authoritative.
	SnapshotV2ReadAuditCanaryScheduleID string `mapstructure:"snapshot_v2_read_audit_canary_schedule_id"`
	// ObservationShadowCanaryScheduleID computes the new policy for exactly one
	// task without changing candidates. AuthorityCanary may only name that same
	// task and is the exact rollback switch for deterministic admission.
	ObservationShadowCanaryScheduleID    string `mapstructure:"observation_shadow_canary_schedule_id"`
	ObservationAuthorityCanaryScheduleID string `mapstructure:"observation_authority_canary_schedule_id"`
	// PushEffectCanaryScheduleID enables durable external push effects for
	// exactly one compiled task. Empty is fully dark; broad rollout has no key.
	PushEffectCanaryScheduleID string `mapstructure:"push_effect_canary_schedule_id"`
	// PushEffectRecoveryCanaryScheduleID enables recovery for one compiled
	// task. It is independent of the fresh-send canary for separate rollback.
	PushEffectRecoveryCanaryScheduleID string `mapstructure:"push_effect_recovery_canary_schedule_id"`
}

// AgentConfig 是 agent loop 运行约束配置。
type AgentConfig struct {
	MaxTurns int `mapstructure:"max_turns"`
	// DefinitionEditEnabled exposes the single C2b3-2d Agent/controller path.
	// It is false by default until the real-card Gate and receipt dispatcher
	// are completed; false omits the tool and controller from Agent entirely.
	DefinitionEditEnabled bool `mapstructure:"definition_edit_enabled"`
	// TokenBudgetDaily 预留、**当前未接线**：无任何代码按它拦截、也不递增 profiles 表的
	// tokens_used_today（那三列恒为建表默认值）。agent 现有的按次护栏是 MaxTurns 与
	// EndpointMsgCap/EndpointDailyCap（后两者是活的、从 tool_calls 表 COUNT 强制）。
	// 别把这个键当作可调旋钮——设了不生效。
	TokenBudgetDaily int `mapstructure:"token_budget_daily"`
	// SessionTTLMinutes 是会话闲置过期窗口（分钟）：同一 owner 在窗口内的
	// 消息共享一个多轮会话（上下文连续），超时后新开会话（契约 §0）。
	SessionTTLMinutes int `mapstructure:"session_ttl_minutes"`
	// TikHub 端点调用护栏（端点注册表契约 §7，Boss 拍板 2026-07-17：免确认 + 双重限额）。
	// EndpointMsgCap 单条用户消息内的端点调用上限（一条消息最多 20 模型轮，不设限
	// 一轮循环就能烧几十次按次计费调用）；EndpointDailyCap 滚动 24h 窗口的调用总量
	// 上限（从 tool_calls 表 COUNT，含失败调用——失败同样计费）。
	EndpointMsgCap   int `mapstructure:"endpoint_msg_cap"`
	EndpointDailyCap int `mapstructure:"endpoint_daily_cap"`
	// Exa ad-hoc 工具护栏（web_search/read_page，2026-07-20 对抗审查 HIGH 补齐）：
	// 与端点工具同模板的双重限额——按次计费 + 免确认就必须有频率护栏，否则模型
	// 一轮并行 N 个调用或注入诱导就能把 Exa 账单烧穿。ExaMsgCap 单条消息上限
	// （默认 5）；ExaDailyCap 滚动 24h 上限（默认 100，从 tool_calls 表 COUNT）。
	ExaMsgCap   int `mapstructure:"exa_msg_cap"`
	ExaDailyCap int `mapstructure:"exa_daily_cap"`
}

// LogConfig 是日志配置。
type LogConfig struct {
	Level string `mapstructure:"level"`
}

// DashboardConfig 是 Web Dashboard 配置。
type DashboardConfig struct {
	// Password 是**已废弃**的 Dashboard 共享密码（D2′ 换成邮箱+密码账号体系）。
	//
	// 保留字段而不直接删：生产环境仍设着 VANE_DASHBOARD_PASSWORD，
	// viper 遇到未知键不报错、但留着字段能让「这个环境变量已无作用」这件事
	// 在代码里可见。部署侧移除该变量后本字段即可删。
	// Deprecated: 不再被任何代码读取，见 api/auth.go。
	// 为空时不 fail（允许无 Dashboard 场景），api 层对登录一律 401。
	Password string `mapstructure:"password"`
	// Origin 是 Dashboard 前端的部署源（scheme+host），环境变量 VANE_DASHBOARD_ORIGIN。
	// 前端迁 OSS+CDN 后与 API 不再同源（vane.* 静态托管、api.* 后端），浏览器跨源
	// fetch 需要后端应答 CORS 头；本值是唯一放行的 Origin。为空时不放行任何跨源请求
	// （同源部署场景 CORS 头本就多余）。凭证跨源要求 Allow-Origin 不能是通配符，故必须显式。
	Origin string `mapstructure:"origin"`
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
	"llm.agent_api_key",
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
	// 默认只绑 loopback：生产走 Caddy(host 网络)反代 127.0.0.1:8080，8080 不该对公网可达。
	// 需对外监听时用 VANE_SERVER_ADDR / 配置文件显式覆盖（见 ServerConfig.Addr）。
	v.SetDefault("server.addr", "127.0.0.1:8080")
	// Dashboard 前端生产源。设默认值兼有两个作用：生产零配置即放行正确源；
	// 让 dashboard.origin 成为 Viper 的"已知键"，VANE_DASHBOARD_ORIGIN 可覆盖（见 sensitiveKeys 注释）。
	v.SetDefault("dashboard.origin", "https://vane.zhuoqidev.com")

	v.SetDefault("temporal.host", "127.0.0.1:7233")
	v.SetDefault("temporal.namespace", "default")
	v.SetDefault("temporal.task_queue", "vane-push")

	v.SetDefault("llm.provider", "deepseek")
	v.SetDefault("llm.base_url", "https://api.deepseek.com")
	// deepseek-chat/deepseek-reasoner 为旧别名，2026-07-24 起废弃；默认用便宜档 v4-flash。
	v.SetDefault("llm.model", "deepseek-v4-flash")
	v.SetDefault("llm.agent_provider", "")
	v.SetDefault("llm.agent_base_url", "")
	v.SetDefault("llm.agent_model", "deepseek-v4-pro")
	// max_concurrent 32：真实 API 受控实验定的值（2026-07-18），不是拍脑袋。
	// 45 条批次（生产实测批次规模）在并发 5 下 5.7 秒、并发 32 下 1.25 秒，零错误零 429。
	// 上限侧余量极大——打分/出卡走 deepseek-v4-flash，官方并发限额 2500；
	// 32 只是取"单批 45 条两波跑完"的性价比点，再往上收益递减。
	// 同一实验还推翻了一条曾以为成立的优化：给 http.Client 调
	// MaxIdleConnsPerHost 毫无作用（api.deepseek.com 走 HTTP/2，一条连接多路复用几十个
	// 并发请求，连接数根本不是瓶颈；公平对照下两批次都是新建连接 0 次、握手 0 次）。
	// 故此处**只调并发、不动 Transport**。
	//
	// 改这个值时 workflow 的 parBatchFanout 要同步跟上（两者刻意齐平，理由见那里）。
	v.SetDefault("llm.max_concurrent", 32)
	v.SetDefault("llm.compiled_endpoint_generation", 1)
	v.SetDefault("llm.compiled_credential_generation", 1)

	v.SetDefault("fetch.timeout_seconds", 20)
	v.SetDefault("fetch.max_response_mb", 5)
	v.SetDefault("fetch.compiled_exa_credential_generation", 1)
	v.SetDefault("fetch.compiled_tikhub_credential_generation", 1)

	// P1c 先以暗发布落地：false 精确回退旧 prompt；true + schedule ID
	// 单任务 canary；true + 空 ID + allow_all 才能在 canary 通过后全量开启。
	v.SetDefault("pipeline.playbook_prompts_enabled", false)
	v.SetDefault("pipeline.playbook_prompt_canary_schedule_id", "")
	v.SetDefault("pipeline.playbook_prompts_allow_all", false)
	// C1b follows the same two-key rollout discipline as P1c: disabled by
	// default, one explicit task for canary, explicit allow_all for broad use.
	v.SetDefault("pipeline.compiled_runtime_enabled", false)
	v.SetDefault("pipeline.compiled_runtime_canary_schedule_id", "")
	v.SetDefault("pipeline.compiled_runtime_allow_all", false)
	v.SetDefault("pipeline.run_outcome_enabled", false)
	v.SetDefault("pipeline.run_outcome_canary_schedule_id", "")
	v.SetDefault("pipeline.run_outcome_allow_all", false)
	v.SetDefault("pipeline.snapshot_v2_shadow_canary_schedule_id", "")
	v.SetDefault("pipeline.snapshot_v2_read_audit_canary_schedule_id", "")
	// Observation rollout remains exact-task only. Register both defaults so
	// Viper AutomaticEnv recognizes VANE_PIPELINE_OBSERVATION_* in an
	// environment-only deployment; neither is sensitive configuration.
	v.SetDefault("pipeline.observation_shadow_canary_schedule_id", "")
	v.SetDefault("pipeline.observation_authority_canary_schedule_id", "")
	v.SetDefault("pipeline.push_effect_canary_schedule_id", "")
	v.SetDefault("pipeline.push_effect_recovery_canary_schedule_id", "")

	v.SetDefault("agent.max_turns", 20)
	v.SetDefault("agent.definition_edit_enabled", false)
	v.SetDefault("agent.token_budget_daily", 100000)
	v.SetDefault("agent.session_ttl_minutes", 30)
	v.SetDefault("agent.endpoint_msg_cap", 10)
	v.SetDefault("agent.endpoint_daily_cap", 200)
	v.SetDefault("agent.exa_msg_cap", 5)
	v.SetDefault("agent.exa_daily_cap", 100)

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
	rawCanaryID := c.Pipeline.PlaybookPromptCanaryScheduleID
	canaryID := strings.TrimSpace(rawCanaryID)
	if c.Pipeline.PlaybookPromptsEnabled && rawCanaryID != "" && canaryID == "" {
		return errors.New("config: pipeline.playbook_prompt_canary_schedule_id 不能仅含空白")
	}
	c.Pipeline.PlaybookPromptCanaryScheduleID = canaryID
	if c.Pipeline.PlaybookPromptsEnabled {
		if canaryID == "" && !c.Pipeline.PlaybookPromptsAllowAll {
			return errors.New("config: 全量启用任务手册 prompt 必须显式设置 pipeline.playbook_prompts_allow_all=true")
		}
		if canaryID != "" && c.Pipeline.PlaybookPromptsAllowAll {
			return errors.New("config: 单任务 canary 与 pipeline.playbook_prompts_allow_all 不能同时启用")
		}
	}
	rawCompiledCanaryID := c.Pipeline.CompiledRuntimeCanaryScheduleID
	compiledCanaryID := strings.TrimSpace(rawCompiledCanaryID)
	if c.Pipeline.CompiledRuntimeEnabled && rawCompiledCanaryID != "" && compiledCanaryID == "" {
		return errors.New("config: pipeline.compiled_runtime_canary_schedule_id 不能仅含空白")
	}
	c.Pipeline.CompiledRuntimeCanaryScheduleID = compiledCanaryID
	if c.Pipeline.CompiledRuntimeEnabled {
		if compiledCanaryID == "" && !c.Pipeline.CompiledRuntimeAllowAll {
			return errors.New("config: 全量启用 compiled runtime 必须显式设置 pipeline.compiled_runtime_allow_all=true")
		}
		if compiledCanaryID != "" && c.Pipeline.CompiledRuntimeAllowAll {
			return errors.New("config: compiled runtime 单任务 canary 与 allow_all 不能同时启用")
		}
	}
	rawRunOutcomeCanaryID := c.Pipeline.RunOutcomeCanaryScheduleID
	runOutcomeCanaryID := strings.TrimSpace(rawRunOutcomeCanaryID)
	if c.Pipeline.RunOutcomeEnabled &&
		rawRunOutcomeCanaryID != "" && runOutcomeCanaryID == "" {
		return errors.New(
			"config: pipeline.run_outcome_canary_schedule_id 不能仅含空白")
	}
	c.Pipeline.RunOutcomeCanaryScheduleID = runOutcomeCanaryID
	if c.Pipeline.RunOutcomeEnabled {
		if !c.Pipeline.CompiledRuntimeEnabled {
			return errors.New(
				"config: run outcome lifecycle 要求 compiled runtime 已启用")
		}
		if runOutcomeCanaryID == "" && !c.Pipeline.RunOutcomeAllowAll {
			return errors.New(
				"config: 全量启用 run outcome 必须显式设置 pipeline.run_outcome_allow_all=true")
		}
		if runOutcomeCanaryID != "" && c.Pipeline.RunOutcomeAllowAll {
			return errors.New(
				"config: run outcome 单任务 canary 与 allow_all 不能同时启用")
		}
		if runOutcomeCanaryID != "" &&
			!c.Pipeline.CompiledRuntimeAllowAll &&
			compiledCanaryID != runOutcomeCanaryID {
			return errors.New(
				"config: run outcome canary 必须位于 compiled runtime rollout")
		}
	}
	rawShadowCanaryID := c.Pipeline.SnapshotV2ShadowCanaryScheduleID
	shadowCanaryID := strings.TrimSpace(rawShadowCanaryID)
	if rawShadowCanaryID != "" && shadowCanaryID == "" {
		return errors.New("config: pipeline.snapshot_v2_shadow_canary_schedule_id 不能仅含空白")
	}
	if shadowCanaryID != "" && !validSnapshotShadowCanaryID(shadowCanaryID) {
		return errors.New("config: pipeline.snapshot_v2_shadow_canary_schedule_id 无效")
	}
	c.Pipeline.SnapshotV2ShadowCanaryScheduleID = shadowCanaryID
	rawReadAuditCanaryID := c.Pipeline.SnapshotV2ReadAuditCanaryScheduleID
	readAuditCanaryID := strings.TrimSpace(rawReadAuditCanaryID)
	if rawReadAuditCanaryID != "" && readAuditCanaryID == "" {
		return errors.New("config: pipeline.snapshot_v2_read_audit_canary_schedule_id 不能仅含空白")
	}
	if readAuditCanaryID != "" && !validSnapshotShadowCanaryID(readAuditCanaryID) {
		return errors.New("config: pipeline.snapshot_v2_read_audit_canary_schedule_id 无效")
	}
	if readAuditCanaryID != "" && !c.Pipeline.CompiledRuntimeEnabled {
		return errors.New("config: snapshot v2 read audit canary 要求 compiled runtime 已启用")
	}
	if readAuditCanaryID != "" && readAuditCanaryID != shadowCanaryID {
		return errors.New("config: snapshot v2 read audit canary 必须精确等于 shadow canary")
	}
	if readAuditCanaryID != "" && !c.Pipeline.CompiledRuntimeAllowAll &&
		compiledCanaryID != readAuditCanaryID {
		return errors.New("config: snapshot v2 read audit canary 必须位于 compiled runtime rollout")
	}
	c.Pipeline.SnapshotV2ReadAuditCanaryScheduleID = readAuditCanaryID
	rawPushEffectCanaryID := c.Pipeline.PushEffectCanaryScheduleID
	pushEffectCanaryID := strings.TrimSpace(rawPushEffectCanaryID)
	if rawPushEffectCanaryID != "" && pushEffectCanaryID == "" {
		return errors.New("config: pipeline.push_effect_canary_schedule_id 不能仅含空白")
	}
	if pushEffectCanaryID != "" &&
		!validSnapshotShadowCanaryID(pushEffectCanaryID) {
		return errors.New("config: pipeline.push_effect_canary_schedule_id 无效")
	}
	if pushEffectCanaryID != "" && !c.Pipeline.CompiledRuntimeEnabled {
		return errors.New("config: push effect canary 要求 compiled runtime 已启用")
	}
	if pushEffectCanaryID != "" && !c.Pipeline.CompiledRuntimeAllowAll &&
		compiledCanaryID != pushEffectCanaryID {
		return errors.New("config: push effect canary 必须位于 compiled runtime rollout")
	}
	c.Pipeline.PushEffectCanaryScheduleID = pushEffectCanaryID
	rawPushRecoveryCanaryID :=
		c.Pipeline.PushEffectRecoveryCanaryScheduleID
	pushRecoveryCanaryID := strings.TrimSpace(rawPushRecoveryCanaryID)
	if rawPushRecoveryCanaryID != "" && pushRecoveryCanaryID == "" {
		return errors.New(
			"config: pipeline.push_effect_recovery_canary_schedule_id 不能仅含空白")
	}
	if pushRecoveryCanaryID != "" &&
		!validSnapshotShadowCanaryID(pushRecoveryCanaryID) {
		return errors.New(
			"config: pipeline.push_effect_recovery_canary_schedule_id 无效")
	}
	if pushRecoveryCanaryID != "" && !c.Pipeline.CompiledRuntimeEnabled {
		return errors.New(
			"config: push effect recovery canary 要求 compiled runtime 已启用")
	}
	if pushRecoveryCanaryID != "" &&
		!c.Pipeline.CompiledRuntimeAllowAll &&
		compiledCanaryID != pushRecoveryCanaryID {
		return errors.New(
			"config: push effect recovery canary 必须位于 compiled runtime rollout")
	}
	c.Pipeline.PushEffectRecoveryCanaryScheduleID = pushRecoveryCanaryID

	rawObservationShadow := c.Pipeline.ObservationShadowCanaryScheduleID
	observationShadow := strings.TrimSpace(rawObservationShadow)
	if rawObservationShadow != "" && observationShadow == "" {
		return errors.New("config: pipeline.observation_shadow_canary_schedule_id 不能仅含空白")
	}
	if observationShadow != "" && !validSnapshotShadowCanaryID(observationShadow) {
		return errors.New("config: pipeline.observation_shadow_canary_schedule_id 无效")
	}
	rawObservationAuthority := c.Pipeline.ObservationAuthorityCanaryScheduleID
	observationAuthority := strings.TrimSpace(rawObservationAuthority)
	if rawObservationAuthority != "" && observationAuthority == "" {
		return errors.New("config: pipeline.observation_authority_canary_schedule_id 不能仅含空白")
	}
	if observationAuthority != "" && observationAuthority != observationShadow {
		return errors.New("config: observation authority canary 必须精确等于 shadow canary")
	}
	if observationShadow != "" && !c.Pipeline.CompiledRuntimeEnabled {
		return errors.New("config: observation canary 要求 compiled runtime 已启用")
	}
	if observationShadow != "" && !c.Pipeline.CompiledRuntimeAllowAll &&
		compiledCanaryID != observationShadow {
		return errors.New("config: observation canary 必须位于 compiled runtime rollout")
	}
	c.Pipeline.ObservationShadowCanaryScheduleID = observationShadow
	c.Pipeline.ObservationAuthorityCanaryScheduleID = observationAuthority

	if c.Server.Addr == "" {
		c.Server.Addr = "127.0.0.1:8080" // 与 setDefaults 一致：空值回退也只绑 loopback
	}
	// 绑定非 loopback（含 ":8080" 这种全网卡形态，host 为空）时高声告警：8080 可能对公网
	// 暴露、被直连绕过 Caddy。捕捉两类事故——逃生阀 VANE_SERVER_ADDR 误设，或 VPS 残留
	// 旧 config.yaml(addr: ":8080") 静默覆盖新默认值致本纵深加固失效。
	if host, _, err := net.SplitHostPort(c.Server.Addr); err == nil {
		if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
			slog.Warn("config: server.addr 绑定非 loopback，8080 可能对公网可达——请确认仅经 Caddy 反代并已配防火墙", "addr", c.Server.Addr)
		}
	}
	if c.DB.URL == "" {
		return errors.New("config: db.url 必填（可通过环境变量 VANE_DB_URL 设置）")
	}
	if _, err := c.LLM.AgentClientConfig(); err != nil {
		return err
	}
	if c.LLM.CompiledEndpointGeneration == 0 {
		c.LLM.CompiledEndpointGeneration = 1
	}
	if c.LLM.CompiledCredentialGeneration == 0 {
		c.LLM.CompiledCredentialGeneration = 1
	}
	if c.LLM.CompiledEndpointGeneration < 0 || c.LLM.CompiledCredentialGeneration < 0 {
		return errors.New("config: llm compiled route generation 必须为正数")
	}
	if c.Fetch.CompiledExaCredentialGeneration == 0 {
		c.Fetch.CompiledExaCredentialGeneration = 1
	}
	if c.Fetch.CompiledTikHubCredentialGeneration == 0 {
		c.Fetch.CompiledTikHubCredentialGeneration = 1
	}
	if c.Fetch.CompiledExaCredentialGeneration < 0 ||
		c.Fetch.CompiledTikHubCredentialGeneration < 0 {
		return errors.New("config: fetch compiled credential generation 必须为正数")
	}
	return nil
}

func validSnapshotShadowCanaryID(value string) bool {
	if value == "" || len(value) > 255 || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
			return false
		}
	}
	return true
}

// AgentClientConfig 返回 Agent/DeepDive/A2A 质量档使用的独立客户端配置。
// 旧配置零迁移：没有显式 Agent 路由时沿用主 endpoint/key，只覆盖模型。
func (c LLMConfig) AgentClientConfig() (LLMConfig, error) {
	provider := strings.TrimSpace(c.AgentProvider)
	baseURL := strings.TrimSpace(c.AgentBaseURL)
	dedicated := provider != "" || baseURL != "" || c.AgentAPIKey != ""
	if dedicated && (provider == "" || baseURL == "" || c.AgentAPIKey == "") {
		return LLMConfig{}, errors.New(
			"config: llm.agent_provider、llm.agent_base_url、llm.agent_api_key 必须同时设置",
		)
	}

	agent := c
	agent.Model = c.AgentModel
	if dedicated {
		agent.Provider = provider
		agent.BaseURL = baseURL
		agent.APIKey = c.AgentAPIKey
	}
	return agent, nil
}

// Package config 提供 Vane 的配置加载：YAML 文件 + VANE_ 前缀环境变量覆盖（Viper）。
//
// 优先级：环境变量 > 配置文件 > 内置默认值。
// 敏感值（数据库连接串、各类 API key）不应写入配置文件，一律走环境变量。
package config

import (
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
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
	Server          ServerConfig          `mapstructure:"server"`
	DB              DBConfig              `mapstructure:"db"`
	Temporal        TemporalConfig        `mapstructure:"temporal"`
	LLM             LLMConfig             `mapstructure:"llm"`
	Fetch           FetchConfig           `mapstructure:"fetch"`
	Pipeline        PipelineConfig        `mapstructure:"pipeline"`
	Agent           AgentConfig           `mapstructure:"agent"`
	Log             LogConfig             `mapstructure:"log"`
	ResearchGateway ResearchGatewayConfig `mapstructure:"research_gateway"`

	Dashboard DashboardConfig `mapstructure:"dashboard"`
	SMTP      SMTPConfig      `mapstructure:"smtp"`
	A2A       A2AConfig       `mapstructure:"a2a"`
}

// SMTPConfig is opt-in. When enabled all fields are required and transport is
// restricted to implicit TLS or STARTTLS; plaintext SMTP has no valid value.
type SMTPConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	Host       string `mapstructure:"host"`
	Port       int    `mapstructure:"port"`
	Username   string `mapstructure:"username"`
	Password   string `mapstructure:"password"`
	From       string `mapstructure:"from"`
	TLSMode    string `mapstructure:"tls_mode"`
	ServerName string `mapstructure:"server_name"`
}

type ResearchGatewayConfig struct {
	SocketPath string `mapstructure:"socket_path"`
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
	// ResearchRuntimeURL 是 V3 情报运行专用的非 owner LOGIN 连接串。
	// 留空时旧运行路径保持可用，但所有 V3 Store 入口 fail-closed。
	ResearchRuntimeURL string `mapstructure:"research_runtime_url"`
	// NativeV3EditRecoveryRuntimeURL is the independently authenticated,
	// non-user-facing login used only for atomic stale edit claims.
	NativeV3EditRecoveryRuntimeURL string `mapstructure:"native_v3_edit_recovery_runtime_url"`
	// ResearchControlURL is the independently authenticated vane_server_runtime
	// connection used by the V3 control plane. It must never be the schema-owner
	// URL or the paid executor URL.
	ResearchControlURL string `mapstructure:"research_control_url"`
	// ResearchCapabilityKeyID identifies the active HMAC key. Retired keys must
	// be retained for at least the longest V3 workflow/retry window.
	ResearchCapabilityKeyID string `mapstructure:"research_capability_key_id"`
	// ResearchCapabilityKeyHex is exactly 32 random bytes encoded as lowercase
	// hex. It is control-plane authority and must only be supplied by secret env.
	ResearchCapabilityKeyHex string `mapstructure:"research_capability_key_hex"`
	// ResearchCapabilityRetiredKeys retains derivation-only keys as
	// "kid=64hex,kid2=64hex" for workflows issued before rotation.
	ResearchCapabilityRetiredKeys string `mapstructure:"research_capability_retired_keys"`
	// ResearchCapabilityTTLDays must cover the maximum workflow, retry and
	// Temporal retention window. Default 90 days.
	ResearchCapabilityTTLDays int `mapstructure:"research_capability_ttl_days"`
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
	//（摘要/评分等固定流水线）保持独立配置和调用面分账。2026-08-14
	// production-shaped thinking/high 评测中 v4-flash 60/60，通过率不低于
	// v4-pro，但延迟和成本显著更低，因此默认选择 flash。
	AgentModel string `mapstructure:"agent_model"`
	// ResearchModel is the dedicated strong model for V3 planner/synthesis.
	// It deliberately does not inherit AgentModel: the paid research ledger
	// must have a retained price rule for this exact model generation.
	ResearchModel string `mapstructure:"research_model"`
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
	// ToolRuntimeCanaryScheduleID replaces the retained Source runtime with the
	// Source-free compiled Tool runtime for exactly one durable task. Rollback
	// is pause-task first, then clear this ID; V2 deliberately has no allow-all
	// switch and cannot be relabeled as the incompatible retained V1 runtime.
	ToolRuntimeCanaryScheduleID string `mapstructure:"tool_runtime_canary_schedule_id"`
	// ResearchV3ShadowCanaryScheduleID permits one exact no-delivery shadow run.
	ResearchV3ShadowCanaryScheduleID string `mapstructure:"research_v3_shadow_canary_schedule_id"`
	// ResearchV3AuthorityCanaryScheduleID permits the receipt-backed cutover
	// control plane for exactly the already-shadowed task. There is no allow-all.
	ResearchV3AuthorityCanaryScheduleID string `mapstructure:"research_v3_authority_canary_schedule_id"`
	// ResearchV3RuntimeEnabled keeps the V3 worker/runtime capability available
	// after individual tasks have been cut over. It is not task authority: every
	// formal run must still present an enabled per-task database authority token.
	ResearchV3RuntimeEnabled bool `mapstructure:"research_v3_runtime_enabled"`
	// RunOutcome* is the independent P1-B lifecycle rollout. It may select only
	// Actions already selected by the compiled runtime rollout.
	RunOutcomeEnabled          bool   `mapstructure:"run_outcome_enabled"`
	RunOutcomeCanaryScheduleID string `mapstructure:"run_outcome_canary_schedule_id"`
	RunOutcomeAllowAll         bool   `mapstructure:"run_outcome_allow_all"`
	// CanonicalBrief* is P1-C's independent rollout. It may select only
	// Actions already selected for RunOutcome.
	CanonicalBriefEnabled          bool   `mapstructure:"canonical_brief_enabled"`
	CanonicalBriefCanaryScheduleID string `mapstructure:"canonical_brief_canary_schedule_id"`
	CanonicalBriefAllowAll         bool   `mapstructure:"canonical_brief_allow_all"`
	// StructuredInsight* is Phase 2-A's independent CardGen/Brief rollout.
	StructuredInsightEnabled          bool   `mapstructure:"structured_insight_enabled"`
	StructuredInsightCanaryScheduleID string `mapstructure:"structured_insight_canary_schedule_id"`
	StructuredInsightAllowAll         bool   `mapstructure:"structured_insight_allow_all"`
	// StructuredInsightRenderer* is a separate authority key. Generation may
	// only select tasks inside this scope; already-frozen Briefs remain readable
	// after either rollout is disabled.
	StructuredInsightRendererEnabled          bool   `mapstructure:"structured_insight_renderer_enabled"`
	StructuredInsightRendererCanaryScheduleID string `mapstructure:"structured_insight_renderer_canary_schedule_id"`
	StructuredInsightRendererAllowAll         bool   `mapstructure:"structured_insight_renderer_allow_all"`
	// StructuredEventEvidence* is P2-B1's independently default-off runtime.
	// It may select only tasks already inside structured generation+renderer.
	StructuredEventEvidenceEnabled          bool   `mapstructure:"structured_event_evidence_enabled"`
	StructuredEventEvidenceCanaryScheduleID string `mapstructure:"structured_event_evidence_canary_schedule_id"`
	StructuredEventEvidenceAllowAll         bool   `mapstructure:"structured_event_evidence_allow_all"`
	// ExecutiveBrief* is P2-D's independently default-off synthesis runtime.
	// It may select only tasks already on structured event evidence.
	ExecutiveBriefEnabled          bool   `mapstructure:"executive_brief_enabled"`
	ExecutiveBriefCanaryScheduleID string `mapstructure:"executive_brief_canary_schedule_id"`
	ExecutiveBriefAllowAll         bool   `mapstructure:"executive_brief_allow_all"`
	// Synthesis, Web exposure and Feishu rendering are deliberately separate:
	// an exact task may generate dark artifacts before either channel is on.
	ExecutiveBriefWebCanaryScheduleID      string `mapstructure:"executive_brief_web_canary_schedule_id"`
	ExecutiveBriefWebProjectionAllowAll    bool   `mapstructure:"executive_brief_web_projection_allow_all"`
	ExecutiveBriefRendererCanaryScheduleID string `mapstructure:"executive_brief_renderer_canary_schedule_id"`
	// CanonicalBriefRendererCanaryScheduleID is P1-E's independent Feishu
	// content-authority switch. Empty is the complete rollback state.
	CanonicalBriefRendererCanaryScheduleID string `mapstructure:"canonical_brief_renderer_canary_schedule_id"`
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
	// PushEffectRecoveryCanaryScheduleID enables recovery for one exact task.
	// Legacy compiled tasks retain their rollout dependency; Research V3
	// authority must select this same task before delivery can be enabled.
	PushEffectRecoveryCanaryScheduleID string `mapstructure:"push_effect_recovery_canary_schedule_id"`
}

// AgentConfig 是 agent loop 运行约束配置。
type AgentConfig struct {
	MaxTurns int `mapstructure:"max_turns"`
	// SessionTTLMinutes 是会话闲置过期窗口（分钟）：同一 owner 在窗口内的
	// 消息共享一个多轮会话（上下文连续），超时后新开会话（契约 §0）。
	SessionTTLMinutes int `mapstructure:"session_ttl_minutes"`
	// 社媒端点调用护栏。单消息最多执行 8 次工具，另有 20 轮总循环上限；
	// EndpointDailyCap 是滚动 24h 总量上限（从 tool_calls 表 COUNT，含失败调用）。
	EndpointDailyCap int `mapstructure:"endpoint_daily_cap"`
	// 网页研究工具护栏。单消息边界同样由统一熔断器负责；
	// ExaDailyCap 是滚动 24h 上限（从 tool_calls 表 COUNT）。
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
	"db.research_runtime_url",
	"db.native_v3_edit_recovery_runtime_url",
	"db.research_control_url",
	"db.research_capability_key_id",
	"db.research_capability_key_hex",
	"db.research_capability_retired_keys",
	"llm.api_key",
	"llm.agent_api_key",
	"fetch.tikhub_api_key",
	"fetch.exa_api_key",
	"dashboard.password",
	"smtp.password",
	"a2a.token",
}

const nativeV3EditRecoveryDBCredential = "native_v3_edit_recovery_db_url"

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
	if strings.TrimSpace(cfg.DB.NativeV3EditRecoveryRuntimeURL) == "" {
		credential, err := loadOptionalSystemdCredential(nativeV3EditRecoveryDBCredential)
		if err != nil {
			return nil, err
		}
		cfg.DB.NativeV3EditRecoveryRuntimeURL = credential
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func loadOptionalSystemdCredential(name string) (string, error) {
	directory := strings.TrimSpace(os.Getenv("CREDENTIALS_DIRECTORY"))
	if directory == "" {
		return "", nil
	}
	payload, err := os.ReadFile(filepath.Join(directory, name))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("config: read %s systemd credential", name)
	}
	value := strings.TrimSpace(string(payload))
	if value == "" {
		return "", fmt.Errorf("config: %s systemd credential is empty", name)
	}
	return value, nil
}

// setDefaults 注册与 config.example.yaml 一致的非敏感默认值。
func setDefaults(v *viper.Viper) {
	// 默认只绑 loopback：生产走 Caddy(host 网络)反代 127.0.0.1:8080，8080 不该对公网可达。
	// 需对外监听时用 VANE_SERVER_ADDR / 配置文件显式覆盖（见 ServerConfig.Addr）。
	v.SetDefault("server.addr", "127.0.0.1:8080")
	v.SetDefault("research_gateway.socket_path", "/run/vane-research-gateway/gateway.sock")
	// Dashboard 前端生产源。设默认值兼有两个作用：生产零配置即放行正确源；
	// 让 dashboard.origin 成为 Viper 的"已知键"，VANE_DASHBOARD_ORIGIN 可覆盖（见 sensitiveKeys 注释）。
	v.SetDefault("dashboard.origin", "https://vane.zhuoqidev.com")
	v.SetDefault("smtp.enabled", false)
	v.SetDefault("smtp.host", "")
	v.SetDefault("smtp.port", 465)
	v.SetDefault("smtp.username", "")
	v.SetDefault("smtp.from", "")
	v.SetDefault("smtp.tls_mode", "implicit_tls")
	v.SetDefault("smtp.server_name", "")

	v.SetDefault("temporal.host", "127.0.0.1:7233")
	v.SetDefault("temporal.namespace", "default")
	v.SetDefault("temporal.task_queue", "vane-push")
	v.SetDefault("db.research_capability_ttl_days", 90)

	v.SetDefault("llm.provider", "deepseek")
	v.SetDefault("llm.base_url", "https://api.deepseek.com")
	// deepseek-chat/deepseek-reasoner 为旧别名，2026-07-24 起废弃；默认用便宜档 v4-flash。
	v.SetDefault("llm.model", "deepseek-v4-flash")
	v.SetDefault("llm.agent_provider", "")
	v.SetDefault("llm.agent_base_url", "")
	v.SetDefault("llm.agent_model", "deepseek-v4-flash")
	// Research V3 is intentionally pinned to the same economical DeepSeek
	// flash tier as the fixed pipeline. Agent conversations also default to flash;
	// content-addressed Research snapshots keep their already-frozen model.
	v.SetDefault("llm.research_model", "deepseek-v4-flash")
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
	v.SetDefault("pipeline.tool_runtime_canary_schedule_id", "")
	v.SetDefault("pipeline.research_v3_shadow_canary_schedule_id", "")
	v.SetDefault("pipeline.research_v3_authority_canary_schedule_id", "")
	v.SetDefault("pipeline.research_v3_runtime_enabled", false)
	v.SetDefault("pipeline.run_outcome_enabled", false)
	v.SetDefault("pipeline.run_outcome_canary_schedule_id", "")
	v.SetDefault("pipeline.run_outcome_allow_all", false)
	v.SetDefault("pipeline.canonical_brief_enabled", false)
	v.SetDefault("pipeline.canonical_brief_canary_schedule_id", "")
	v.SetDefault("pipeline.canonical_brief_allow_all", false)
	v.SetDefault("pipeline.structured_insight_enabled", false)
	v.SetDefault("pipeline.structured_insight_canary_schedule_id", "")
	v.SetDefault("pipeline.structured_insight_allow_all", false)
	v.SetDefault("pipeline.structured_insight_renderer_enabled", false)
	v.SetDefault("pipeline.structured_insight_renderer_canary_schedule_id", "")
	v.SetDefault("pipeline.structured_insight_renderer_allow_all", false)
	v.SetDefault("pipeline.structured_event_evidence_enabled", false)
	v.SetDefault("pipeline.structured_event_evidence_canary_schedule_id", "")
	v.SetDefault("pipeline.structured_event_evidence_allow_all", false)
	v.SetDefault("pipeline.executive_brief_enabled", false)
	v.SetDefault("pipeline.executive_brief_canary_schedule_id", "")
	v.SetDefault("pipeline.executive_brief_allow_all", false)
	v.SetDefault("pipeline.executive_brief_web_canary_schedule_id", "")
	v.SetDefault("pipeline.executive_brief_web_projection_allow_all", false)
	v.SetDefault("pipeline.executive_brief_renderer_canary_schedule_id", "")
	v.SetDefault("pipeline.canonical_brief_renderer_canary_schedule_id", "")
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
	v.SetDefault("agent.session_ttl_minutes", 30)
	v.SetDefault("agent.endpoint_daily_cap", 200)
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
	if c.SMTP.Enabled {
		if strings.TrimSpace(c.SMTP.Host) == "" || c.SMTP.Port < 1 || c.SMTP.Port > 65535 ||
			strings.TrimSpace(c.SMTP.Username) == "" || c.SMTP.Password == "" ||
			strings.TrimSpace(c.SMTP.From) == "" {
			return errors.New("config: smtp.enabled 要求 host/port/username/password/from 完整")
		}
		if c.SMTP.TLSMode != "implicit_tls" && c.SMTP.TLSMode != "starttls" {
			return errors.New("config: smtp.tls_mode 只允许 implicit_tls 或 starttls")
		}
	}
	c.LLM.ResearchModel = strings.TrimSpace(c.LLM.ResearchModel)
	if c.LLM.ResearchModel == "" {
		c.LLM.ResearchModel = "deepseek-v4-flash"
	}
	c.DB.ResearchCapabilityKeyID = strings.TrimSpace(c.DB.ResearchCapabilityKeyID)
	c.DB.ResearchCapabilityKeyHex = strings.TrimSpace(c.DB.ResearchCapabilityKeyHex)
	c.DB.ResearchCapabilityRetiredKeys = strings.TrimSpace(
		c.DB.ResearchCapabilityRetiredKeys)
	if c.DB.ResearchCapabilityTTLDays == 0 {
		c.DB.ResearchCapabilityTTLDays = 90
	}
	if c.DB.ResearchCapabilityTTLDays < 7 || c.DB.ResearchCapabilityTTLDays > 400 {
		return errors.New("config: db.research_capability_ttl_days 必须在 7–400 天")
	}
	if strings.TrimSpace(c.DB.ResearchRuntimeURL) != "" {
		if c.DB.ResearchCapabilityKeyID == "" || c.DB.ResearchCapabilityKeyHex == "" {
			return errors.New("config: V3 research runtime 要求 research capability key")
		}
		if len(c.DB.ResearchCapabilityKeyHex) != 64 ||
			strings.ToLower(c.DB.ResearchCapabilityKeyHex) != c.DB.ResearchCapabilityKeyHex {
			return errors.New("config: db.research_capability_key_hex 必须是 32-byte lowercase hex")
		}
	}
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
	rawToolRuntimeCanaryID := c.Pipeline.ToolRuntimeCanaryScheduleID
	toolRuntimeCanaryID := strings.TrimSpace(rawToolRuntimeCanaryID)
	if rawToolRuntimeCanaryID != "" && toolRuntimeCanaryID == "" {
		return errors.New(
			"config: pipeline.tool_runtime_canary_schedule_id 不能仅含空白")
	}
	c.Pipeline.ToolRuntimeCanaryScheduleID = toolRuntimeCanaryID
	if toolRuntimeCanaryID != "" {
		if !c.Pipeline.CompiledRuntimeEnabled {
			return errors.New(
				"config: Tool runtime canary 要求 compiled runtime 已启用")
		}
		if !c.Pipeline.CompiledRuntimeAllowAll &&
			compiledCanaryID != toolRuntimeCanaryID {
			return errors.New(
				"config: Tool runtime canary 必须位于 compiled runtime rollout")
		}
	}
	rawResearchV3ShadowID := c.Pipeline.ResearchV3ShadowCanaryScheduleID
	researchV3ShadowID := strings.TrimSpace(rawResearchV3ShadowID)
	if rawResearchV3ShadowID != "" && researchV3ShadowID == "" {
		return errors.New(
			"config: pipeline.research_v3_shadow_canary_schedule_id 不能仅含空白")
	}
	if researchV3ShadowID != "" && !validSnapshotShadowCanaryID(researchV3ShadowID) {
		return errors.New(
			"config: pipeline.research_v3_shadow_canary_schedule_id 必须是 1-255 字节的有效任务 ID，且不能包含控制字符")
	}
	c.Pipeline.ResearchV3ShadowCanaryScheduleID = researchV3ShadowID
	rawResearchV3AuthorityID := c.Pipeline.ResearchV3AuthorityCanaryScheduleID
	researchV3AuthorityID := strings.TrimSpace(rawResearchV3AuthorityID)
	if rawResearchV3AuthorityID != "" && researchV3AuthorityID == "" {
		return errors.New(
			"config: pipeline.research_v3_authority_canary_schedule_id 不能仅含空白")
	}
	if researchV3AuthorityID != "" && !validSnapshotShadowCanaryID(researchV3AuthorityID) {
		return errors.New(
			"config: pipeline.research_v3_authority_canary_schedule_id 必须是 1-255 字节的有效任务 ID，且不能包含控制字符")
	}
	c.Pipeline.ResearchV3AuthorityCanaryScheduleID = researchV3AuthorityID
	if researchV3AuthorityID != "" && researchV3AuthorityID != researchV3ShadowID {
		return errors.New(
			"config: Research V3 authority canary 必须与已配置的 shadow canary 是同一任务")
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
	rawCanonicalBriefCanaryID := c.Pipeline.CanonicalBriefCanaryScheduleID
	canonicalBriefCanaryID := strings.TrimSpace(rawCanonicalBriefCanaryID)
	if c.Pipeline.CanonicalBriefEnabled &&
		rawCanonicalBriefCanaryID != "" && canonicalBriefCanaryID == "" {
		return errors.New(
			"config: pipeline.canonical_brief_canary_schedule_id 不能仅含空白")
	}
	c.Pipeline.CanonicalBriefCanaryScheduleID = canonicalBriefCanaryID
	if c.Pipeline.CanonicalBriefEnabled {
		if !c.Pipeline.RunOutcomeEnabled {
			return errors.New(
				"config: canonical brief writer 要求 run outcome lifecycle 已启用")
		}
		if canonicalBriefCanaryID == "" && !c.Pipeline.CanonicalBriefAllowAll {
			return errors.New(
				"config: 全量启用 canonical brief 必须显式设置 pipeline.canonical_brief_allow_all=true")
		}
		if canonicalBriefCanaryID != "" && c.Pipeline.CanonicalBriefAllowAll {
			return errors.New(
				"config: canonical brief 单任务 canary 与 allow_all 不能同时启用")
		}
		if canonicalBriefCanaryID != "" &&
			!c.Pipeline.RunOutcomeAllowAll &&
			runOutcomeCanaryID != canonicalBriefCanaryID {
			return errors.New(
				"config: canonical brief canary 必须位于 run outcome rollout")
		}
	}
	rawStructuredInsightCanaryID :=
		c.Pipeline.StructuredInsightCanaryScheduleID
	structuredInsightCanaryID :=
		strings.TrimSpace(rawStructuredInsightCanaryID)
	if c.Pipeline.StructuredInsightEnabled &&
		rawStructuredInsightCanaryID != "" &&
		structuredInsightCanaryID == "" {
		return errors.New(
			"config: pipeline.structured_insight_canary_schedule_id 不能仅含空白")
	}
	c.Pipeline.StructuredInsightCanaryScheduleID = structuredInsightCanaryID
	rawStructuredRendererCanaryID :=
		c.Pipeline.StructuredInsightRendererCanaryScheduleID
	structuredRendererCanaryID :=
		strings.TrimSpace(rawStructuredRendererCanaryID)
	if c.Pipeline.StructuredInsightRendererEnabled &&
		rawStructuredRendererCanaryID != "" &&
		structuredRendererCanaryID == "" {
		return errors.New(
			"config: pipeline.structured_insight_renderer_canary_schedule_id 不能仅含空白")
	}
	c.Pipeline.StructuredInsightRendererCanaryScheduleID =
		structuredRendererCanaryID
	if c.Pipeline.StructuredInsightRendererEnabled {
		if structuredRendererCanaryID == "" &&
			!c.Pipeline.StructuredInsightRendererAllowAll {
			return errors.New(
				"config: 全量启用 structured insight renderer 必须显式设置 allow_all=true")
		}
		if structuredRendererCanaryID != "" &&
			c.Pipeline.StructuredInsightRendererAllowAll {
			return errors.New(
				"config: structured insight renderer canary 与 allow_all 不能同时启用")
		}
		canonicalRendererID := strings.TrimSpace(
			c.Pipeline.CanonicalBriefRendererCanaryScheduleID)
		if c.Pipeline.StructuredInsightRendererAllowAll ||
			structuredRendererCanaryID != canonicalRendererID {
			return errors.New(
				"config: structured insight renderer 必须位于 exact canonical Brief renderer canary")
		}
	}
	if c.Pipeline.StructuredInsightEnabled {
		if !c.Pipeline.CanonicalBriefEnabled {
			return errors.New(
				"config: structured insight 要求 canonical brief 已启用")
		}
		if structuredInsightCanaryID == "" &&
			!c.Pipeline.StructuredInsightAllowAll {
			return errors.New(
				"config: 全量启用 structured insight 必须显式设置 pipeline.structured_insight_allow_all=true")
		}
		if structuredInsightCanaryID != "" &&
			c.Pipeline.StructuredInsightAllowAll {
			return errors.New(
				"config: structured insight 单任务 canary 与 allow_all 不能同时启用")
		}
		if structuredInsightCanaryID != "" &&
			!c.Pipeline.CanonicalBriefAllowAll &&
			canonicalBriefCanaryID != structuredInsightCanaryID {
			return errors.New(
				"config: structured insight canary 必须位于 canonical brief rollout")
		}
		if !c.Pipeline.StructuredInsightRendererEnabled ||
			c.Pipeline.StructuredInsightAllowAll &&
				!c.Pipeline.StructuredInsightRendererAllowAll ||
			structuredInsightCanaryID != "" &&
				!c.Pipeline.StructuredInsightRendererAllowAll &&
				structuredInsightCanaryID != structuredRendererCanaryID {
			return errors.New(
				"config: structured insight generation 必须位于独立 renderer rollout")
		}
	}
	rawStructuredEventEvidenceCanaryID :=
		c.Pipeline.StructuredEventEvidenceCanaryScheduleID
	structuredEventEvidenceCanaryID :=
		strings.TrimSpace(rawStructuredEventEvidenceCanaryID)
	if c.Pipeline.StructuredEventEvidenceEnabled &&
		rawStructuredEventEvidenceCanaryID != "" &&
		structuredEventEvidenceCanaryID == "" {
		return errors.New(
			"config: pipeline.structured_event_evidence_canary_schedule_id 不能仅含空白")
	}
	c.Pipeline.StructuredEventEvidenceCanaryScheduleID =
		structuredEventEvidenceCanaryID
	if c.Pipeline.StructuredEventEvidenceEnabled {
		if !c.Pipeline.StructuredInsightEnabled {
			return errors.New(
				"config: structured event evidence 要求 structured insight 已启用")
		}
		if structuredEventEvidenceCanaryID == "" {
			return errors.New(
				"config: structured event evidence 当前只允许精确 canary")
		}
		if c.Pipeline.StructuredEventEvidenceAllowAll {
			return errors.New(
				"config: structured event evidence 尚不允许 allow_all")
		}
		if !c.Pipeline.StructuredInsightAllowAll &&
			structuredEventEvidenceCanaryID !=
				structuredInsightCanaryID {
			return errors.New(
				"config: structured event evidence 必须位于 structured insight rollout")
		}
		if observationAuthority :=
			strings.TrimSpace(
				c.Pipeline.ObservationAuthorityCanaryScheduleID,
			); observationAuthority == "" ||
			observationAuthority != structuredEventEvidenceCanaryID {
			return errors.New(
				"config: structured event evidence 必须精确等于 observation authority canary")
		}
	}
	rawExecutiveBriefCanaryID :=
		c.Pipeline.ExecutiveBriefCanaryScheduleID
	executiveBriefCanaryID :=
		strings.TrimSpace(rawExecutiveBriefCanaryID)
	if c.Pipeline.ExecutiveBriefEnabled &&
		rawExecutiveBriefCanaryID != "" &&
		executiveBriefCanaryID == "" {
		return errors.New(
			"config: pipeline.executive_brief_canary_schedule_id 不能仅含空白")
	}
	c.Pipeline.ExecutiveBriefCanaryScheduleID = executiveBriefCanaryID
	if c.Pipeline.ExecutiveBriefEnabled {
		if !c.Pipeline.StructuredEventEvidenceEnabled {
			return errors.New(
				"config: executive brief 要求 structured event evidence 已启用")
		}
		if executiveBriefCanaryID == "" &&
			!c.Pipeline.ExecutiveBriefAllowAll {
			return errors.New(
				"config: 全量启用 executive brief 必须显式设置 allow_all=true")
		}
		if executiveBriefCanaryID != "" &&
			c.Pipeline.ExecutiveBriefAllowAll {
			return errors.New(
				"config: executive brief canary 与 allow_all 不能同时启用")
		}
		if c.Pipeline.ExecutiveBriefAllowAll &&
			!c.Pipeline.StructuredEventEvidenceAllowAll {
			return errors.New(
				"config: executive brief allow_all 要求 structured event evidence allow_all")
		}
		if !c.Pipeline.ExecutiveBriefAllowAll &&
			executiveBriefCanaryID !=
				structuredEventEvidenceCanaryID {
			return errors.New(
				"config: executive brief 必须位于 structured event evidence rollout")
		}
	}
	rawExecutiveWebCanary :=
		c.Pipeline.ExecutiveBriefWebCanaryScheduleID
	executiveWebCanary := strings.TrimSpace(rawExecutiveWebCanary)
	if rawExecutiveWebCanary != "" && executiveWebCanary == "" {
		return errors.New(
			"config: pipeline.executive_brief_web_canary_schedule_id 不能仅含空白")
	}
	c.Pipeline.ExecutiveBriefWebCanaryScheduleID = executiveWebCanary
	if c.Pipeline.ExecutiveBriefWebProjectionAllowAll &&
		!c.Pipeline.ExecutiveBriefEnabled {
		return errors.New(
			"config: executive brief Web projection allow_all 要求 synthesis 已启用")
	}
	rawExecutiveRendererCanary :=
		c.Pipeline.ExecutiveBriefRendererCanaryScheduleID
	executiveRendererCanary :=
		strings.TrimSpace(rawExecutiveRendererCanary)
	if rawExecutiveRendererCanary != "" &&
		executiveRendererCanary == "" {
		return errors.New(
			"config: pipeline.executive_brief_renderer_canary_schedule_id 不能仅含空白")
	}
	c.Pipeline.ExecutiveBriefRendererCanaryScheduleID =
		executiveRendererCanary
	for name, taskID := range map[string]string{
		"Web":      executiveWebCanary,
		"renderer": executiveRendererCanary,
	} {
		if taskID == "" {
			continue
		}
		if !c.Pipeline.ExecutiveBriefEnabled ||
			(!c.Pipeline.ExecutiveBriefAllowAll &&
				taskID != executiveBriefCanaryID) {
			return fmt.Errorf(
				"config: executive brief %s canary 必须位于 synthesis rollout",
				name)
		}
	}
	if executiveRendererCanary != "" &&
		executiveRendererCanary != strings.TrimSpace(
			c.Pipeline.CanonicalBriefRendererCanaryScheduleID) {
		return errors.New(
			"config: executive brief renderer 必须位于 canonical Brief renderer canary")
	}
	if executiveRendererCanary != "" &&
		!c.Pipeline.ExecutiveBriefWebProjectionAllowAll &&
		executiveRendererCanary != executiveWebCanary {
		return errors.New(
			"config: executive brief renderer 必须位于 Web canary")
	}
	rawCanonicalRendererCanaryID :=
		c.Pipeline.CanonicalBriefRendererCanaryScheduleID
	canonicalRendererCanaryID :=
		strings.TrimSpace(rawCanonicalRendererCanaryID)
	if rawCanonicalRendererCanaryID != "" &&
		canonicalRendererCanaryID == "" {
		return errors.New(
			"config: pipeline.canonical_brief_renderer_canary_schedule_id 不能仅含空白")
	}
	if canonicalRendererCanaryID != "" &&
		!validSnapshotShadowCanaryID(canonicalRendererCanaryID) {
		return errors.New(
			"config: pipeline.canonical_brief_renderer_canary_schedule_id 无效")
	}
	c.Pipeline.CanonicalBriefRendererCanaryScheduleID =
		canonicalRendererCanaryID
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
	if pushRecoveryCanaryID != "" && researchV3AuthorityID == "" &&
		!c.Pipeline.CompiledRuntimeEnabled {
		return errors.New(
			"config: push effect recovery canary 要求 compiled runtime 已启用")
	}
	if pushRecoveryCanaryID != "" && researchV3AuthorityID == "" &&
		!c.Pipeline.CompiledRuntimeAllowAll &&
		compiledCanaryID != pushRecoveryCanaryID {
		return errors.New(
			"config: push effect recovery canary 必须位于 compiled runtime rollout")
	}
	c.Pipeline.PushEffectRecoveryCanaryScheduleID = pushRecoveryCanaryID
	if researchV3AuthorityID != "" && pushRecoveryCanaryID != researchV3AuthorityID {
		return errors.New(
			"config: Research V3 authority canary 必须启用同一任务的 push effect recovery")
	}
	if c.Pipeline.CanonicalBriefEnabled {
		if c.Pipeline.CanonicalBriefAllowAll ||
			canonicalBriefCanaryID == "" {
			return errors.New(
				"config: canonical brief 当前仅允许 exact-task canary")
		}
		if pushEffectCanaryID != canonicalBriefCanaryID ||
			pushRecoveryCanaryID != canonicalBriefCanaryID {
			return errors.New(
				"config: canonical brief canary 必须同时位于 push effect fresh/recovery canary")
		}
	}
	if canonicalRendererCanaryID != "" {
		if !c.Pipeline.CanonicalBriefEnabled ||
			canonicalBriefCanaryID != canonicalRendererCanaryID {
			return errors.New(
				"config: canonical Brief renderer canary 必须精确位于 canonical Brief writer canary")
		}
		if pushEffectCanaryID != canonicalRendererCanaryID ||
			pushRecoveryCanaryID != canonicalRendererCanaryID {
			return errors.New(
				"config: canonical Brief renderer canary 必须同时位于 push effect fresh/recovery canary")
		}
		origin, err := url.Parse(c.Dashboard.Origin)
		if err != nil || origin == nil ||
			(origin.Scheme != "https" && origin.Scheme != "http") ||
			origin.Host == "" || origin.User != nil ||
			(origin.Path != "" && origin.Path != "/") ||
			origin.RawQuery != "" || origin.Fragment != "" {
			return errors.New(
				"config: canonical Brief renderer 要求 dashboard.origin 为 HTTP(S) 源")
		}
	}

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
	rawGatewaySocket := c.ResearchGateway.SocketPath
	gatewaySocket := strings.TrimSpace(rawGatewaySocket)
	// Validate is also called on directly constructed Config values. Preserve
	// the same isolated default as Load, while still rejecting an explicitly
	// supplied whitespace-only path.
	if rawGatewaySocket == "" {
		gatewaySocket = "/run/vane-research-gateway/gateway.sock"
	}
	if gatewaySocket == "" || len(gatewaySocket) > 255 ||
		!strings.HasPrefix(gatewaySocket, "/") || path.Clean(gatewaySocket) != gatewaySocket ||
		strings.ContainsRune(gatewaySocket, 0) {
		return errors.New("config: research_gateway.socket_path 必须是规范的绝对 Unix socket 路径")
	}
	c.ResearchGateway.SocketPath = gatewaySocket
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
	if c.Pipeline.ResearchV3RuntimeEnabled ||
		c.Pipeline.ResearchV3ShadowCanaryScheduleID != "" ||
		c.Pipeline.ResearchV3AuthorityCanaryScheduleID != "" {
		if strings.TrimSpace(c.DB.ResearchRuntimeURL) == "" {
			return errors.New("config: Research V3 runtime 要求 db.research_runtime_url")
		}
		if strings.TrimSpace(c.DB.NativeV3EditRecoveryRuntimeURL) == "" {
			return errors.New("config: Research V3 runtime 要求 db.native_v3_edit_recovery_runtime_url")
		}
		if strings.TrimSpace(c.DB.ResearchControlURL) == "" {
			return errors.New("config: Research V3 runtime 要求 db.research_control_url")
		}
		if c.DB.ResearchCapabilityKeyID == "" ||
			len(c.DB.ResearchCapabilityKeyHex) != 64 {
			return errors.New("config: Research V3 runtime 要求 active research capability key")
		}
		if _, err := hex.DecodeString(c.DB.ResearchCapabilityKeyHex); err != nil {
			return errors.New("config: Research V3 runtime capability key hex 无效")
		}
		if c.LLM.CompiledEndpointGeneration <= 0 ||
			c.LLM.CompiledCredentialGeneration <= 0 ||
			c.Fetch.CompiledExaCredentialGeneration <= 0 {
			return errors.New("config: Research V3 runtime 要求可用的 LLM/Exa retained generations")
		}
		if strings.TrimSpace(c.Fetch.ExaAPIKey) == "" {
			return errors.New("config: Research V3 runtime 要求 fetch.exa_api_key")
		}
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

package types

import (
	"encoding/json"
	"time"
)

// 本文件定义 9 个领域实体，字段与 store/migrations/001_init.sql
// 的 9 张表一一对应。约定：
//   - 主键 / 外键 bigserial|BIGINT → int64；
//   - 可空时间列 → *time.Time，可空数值列 → 指针；
//   - 带 DEFAULT '' 或语义上恒有值的 TEXT 列 → string（store 层写入空串而非 NULL）；
//   - JSONB 列 → json.RawMessage（延迟解析，各消费方自行定义结构）。

// User 用户（users 表）。MVP 阶段用户即飞书用户。
type User struct {
	ID           int64     `json:"id"`
	FeishuOpenID string    `json:"feishu_open_id"` // 飞书 open_id，UNIQUE NOT NULL
	Name         string    `json:"name"`
	CreatedAt    time.Time `json:"created_at"`
}

// Source 信源（sources 表）。
// 关键设计：next_fetch_at 为预计算的下次抓取时间（替代表达式索引），
// 抓取完成时由 store 层同时更新 last_fetched_at 与 next_fetch_at；
// 调度查询 WHERE status='active' AND next_fetch_at <= now() 命中 (status, next_fetch_at) 索引。
type Source struct {
	ID                   int64           `json:"id"`
	Type                 SourceType      `json:"type"`
	URL                  string          `json:"url"`
	Title                string          `json:"title"`
	Config               json.RawMessage `json:"config"` // JSONB，按 Type 各自定义结构
	Status               SourceStatus    `json:"status"`
	FetchIntervalSeconds int             `json:"fetch_interval_seconds"`
	NextFetchAt          time.Time       `json:"next_fetch_at"`             // NOT NULL DEFAULT now()
	LastFetchedAt        *time.Time      `json:"last_fetched_at,omitempty"` // 从未抓取过时为 NULL
	FailCount            int             `json:"fail_count"`                // 连续失败计数，达阈值自动 disabled
	CreatedAt            time.Time       `json:"created_at"`
	UpdatedAt            time.Time       `json:"updated_at"`
}

// Subscription 用户订阅关系（subscriptions 表），UNIQUE(user_id, source_id)。
type Subscription struct {
	ID        int64              `json:"id"`
	UserID    int64              `json:"user_id"`
	SourceID  int64              `json:"source_id"`
	Status    SubscriptionStatus `json:"status"`
	CreatedAt time.Time          `json:"created_at"`
}

// ContentItem 抓取到的内容条目（content_items 表），UNIQUE(source_id, external_id)。
// content_hash 精确去重，simhash 近似去重（72h 窗口）。
type ContentItem struct {
	ID          int64      `json:"id"`
	SourceID    int64      `json:"source_id"`
	ExternalID  string     `json:"external_id"` // 源内唯一 ID（RSS guid / 平台条目 ID）
	URL         string     `json:"url"`
	Title       string     `json:"title"`
	Content     string     `json:"content"`
	Author      string     `json:"author"`
	PublishedAt *time.Time `json:"published_at,omitempty"` // 源未提供发布时间时为 NULL
	ContentHash string     `json:"content_hash"`           // NOT NULL
	Simhash     *int64     `json:"simhash,omitempty"`      // BIGINT，可空（未计算时为 NULL）
	FetchedAt   time.Time  `json:"fetched_at"`
	CreatedAt   time.Time  `json:"created_at"`
}

// PushBatch 一次推送决策周期（push_batches 表）。
type PushBatch struct {
	ID          int64       `json:"id"`
	UserID      int64       `json:"user_id"`
	ScheduledAt *time.Time  `json:"scheduled_at,omitempty"` // 计划推送时间，可空
	Status      BatchStatus `json:"status"`
	CreatedAt   time.Time   `json:"created_at"`
}

// Delivery 单条内容投递记录（deliveries 表）。
// 关键设计：content_item_id ON DELETE SET NULL —— 内容按 TTL 清理后
// 投递记录仍保留为推送历史，只是不再能追溯原始内容。
type Delivery struct {
	ID              int64           `json:"id"`
	BatchID         int64           `json:"batch_id"`
	UserID          int64           `json:"user_id"`
	ContentItemID   *int64          `json:"content_item_id,omitempty"` // 可空：原内容已被清理
	Score           float64         `json:"score"`                     // NUMERIC NOT NULL：Delivery 在打分之后才创建，恒有值
	CardJSON        json.RawMessage `json:"card_json"`                 // JSONB，飞书交互卡片
	FeishuMessageID string          `json:"feishu_message_id"`         // 发送成功后回填
	Status          DeliveryStatus  `json:"status"`
	SentAt          *time.Time      `json:"sent_at,omitempty"` // 未发送时为 NULL
	CreatedAt       time.Time       `json:"created_at"`
}

// Feedback 用户反馈（feedbacks 表）。
type Feedback struct {
	ID         int64          `json:"id"`
	UserID     int64          `json:"user_id"`
	DeliveryID int64          `json:"delivery_id"`
	Action     FeedbackAction `json:"action"` // NOT NULL
	Detail     string         `json:"detail"` // 文字反馈原文，按钮反馈为空串
	CreatedAt  time.Time      `json:"created_at"`
}

// Profile 用户画像与 token 预算（profiles 表），user_id UNIQUE（1:1）。
type Profile struct {
	ID               int64     `json:"id"`
	UserID           int64     `json:"user_id"`
	Industry         string    `json:"industry"`
	Occupation       string    `json:"occupation"`
	Tags             []string  `json:"tags"` // TEXT[] DEFAULT '{}'
	Summary          string    `json:"summary"`
	TokenBudgetDaily int       `json:"token_budget_daily"` // NOT NULL DEFAULT 100000
	TokensUsedToday  int       `json:"tokens_used_today"`  // NOT NULL DEFAULT 0
	TokenResetAt     time.Time `json:"token_reset_at"`     // NOT NULL
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// LLMCall 单次 LLM 调用记录（llm_calls 表，Step 6 可观测性核心）。
// trace_id 关联同一 pipeline 的多次调用；ref_type + ref_id 多态关联业务对象。
type LLMCall struct {
	ID               int64     `json:"id"`
	TraceID          string    `json:"trace_id"`
	SpanName         string    `json:"span_name"`         // 调用环节：score/cardgen/summarize/feedback_interpret/quality_check
	UserID           *int64    `json:"user_id,omitempty"` // 可空：系统级调用无归属用户；刻意不建 FK
	RefType          RefType   `json:"ref_type"`
	RefID            *int64    `json:"ref_id,omitempty"` // 可空（索引 WHERE ref_id IS NOT NULL）
	Provider         string    `json:"provider"`
	Model            string    `json:"model"`
	SystemPrompt     string    `json:"system_prompt"`
	UserPrompt       string    `json:"user_prompt"`
	Completion       string    `json:"completion"`
	PromptTokens     int       `json:"prompt_tokens"`
	CompletionTokens int       `json:"completion_tokens"`
	LatencyMs        int       `json:"latency_ms"`
	CostUSD          float64   `json:"cost_usd"`                   // NUMERIC(10,6)，MVP 精度用 float64 足够
	PrefixCacheHit   *bool     `json:"prefix_cache_hit,omitempty"` // 可空：provider 未报告缓存信息
	Temperature      *float32  `json:"temperature,omitempty"`      // REAL，可空：未显式设置
	MaxTokens        *int      `json:"max_tokens,omitempty"`       // 可空：未显式设置
	Error            string    `json:"error"`                      // 调用失败原因，成功为空串
	CreatedAt        time.Time `json:"created_at"`
}

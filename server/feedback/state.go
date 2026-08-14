// Package feedback 处理推送卡反馈按钮点击与追问上下文包装（M5 契约 §10/§11）。
// 依赖边界：只依赖 llm/types 与调用方注入的窄接口，不得 import
// feishu/agent/store/workflow——workflow 与 feishu 反向引用本包（CardState、
// FeedbackRunner），import 环由这条边界封死。
package feedback

import (
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/YouToco/vane/types"
)

// AggregateCardMaxBytesV1 is the provider-safe hard limit shared by initial
// Feishu projection and every callback rebuild. Keeping one leaf-package
// constant prevents a feedback form from growing a previously valid card past
// the limit.
const AggregateCardMaxBytesV1 = 28 << 10

// CanonicalBriefWebURLV1 builds the only trusted task deep-link shape used by
// both initial render and callback verification.
func CanonicalBriefWebURLV1(origin, taskID string) (string, error) {
	origin = strings.TrimRight(strings.TrimSpace(origin), "/")
	parsed, err := url.Parse(origin)
	if err != nil || parsed == nil ||
		(parsed.Scheme != "https" && parsed.Scheme != "http") ||
		parsed.Host == "" || parsed.User != nil ||
		(parsed.Path != "" && parsed.Path != "/") ||
		parsed.RawQuery != "" || parsed.Fragment != "" ||
		taskID == "" {
		return "", errors.New("canonical Brief dashboard origin is invalid")
	}
	escapedTaskID := url.PathEscape(taskID)
	escapedTaskID = strings.NewReplacer(
		"(", "%28", ")", "%29",
	).Replace(escapedTaskID)
	return origin + "/#/tasks/" + escapedTaskID, nil
}

// CardState 推送卡状态行的渲染输入（契约 §10.2），由 feishu.BuildDeliveryCard 消费。
// 三字段均以库内查询为准、最终一致：同卡并发点击时两版卡片以飞书到达序为准，
// 短暂缺项在下次点击时自愈。
type CardState struct {
	// Preference 最新态度：零值 ""（未表态）/ interested / not_interested。
	// 查询方恒传 {interested, not_interested} 双值集合取最新一条（审查 F5：
	// 传单值会命中旧行、复刻被否决的唯一索引 bug）。
	Preference types.FeedbackAction
	// Misjudged 是否已标记误判（独立于态度、可并存，MVP 不可撤销）。
	Misjudged bool
	// BadFeedbackOpen only controls the transient reason panel rendered in the
	// callback response. Opening it writes no feedback row and therefore cannot
	// accidentally become a "not interested" profile signal.
	BadFeedbackOpen bool
	// DeepDiveRequested 是否已请求深度解读（此行定格后不再变，生成失败也不回退）。
	DeepDiveRequested bool
}

// CardInput 构卡函数的全量输入（卡片改版扩展签名，替代原 (bodyMD, deliveryID, state)
// 三参数）。反馈重建路径按 best-effort 查库填充：内容/源查不到时字段为零值，
// 构卡函数据此降级渲染（标题空则 header 回退默认、subtitle 缺字段则省略对应段）。
type CardInput struct {
	BodyMD      string
	DeliveryID  int64
	State       CardState
	Title       string         // content_items.title → header title
	Score       int            // round(deliveries.score) → ⚡ tag
	URL         string         // content_items.url → 阅读原文按钮
	SourceTitle string         // fetch_targets.title → subtitle 栏目（证据展示）
	Platform    types.Platform // fetch_targets.platform → subtitle emoji
	PublishedAt *time.Time     // content_items.published_at → subtitle 相对时间
	// DiscoveredAt is the immutable Brief observation time. Legacy cards leave
	// it zero; the canonical renderer uses it only when PublishedAt is absent.
	DiscoveredAt time.Time
	// EvidenceSources is the ordered public subset of an immutable P2-C event
	// evidence extension. It never contains database IDs, digests or bodies.
	EvidenceSources []CanonicalEvidenceSourceV1
}

type CanonicalEvidenceSourceV1 = types.EvidenceSourceProjectionV1

// CanonicalBriefCardV1 is transport metadata for a Feishu prefix projection.
// Content stays in BriefV1; this only preserves the exact batch identity,
// visible prefix length, and Web deep link across feedback card rebuilds.
type CanonicalBriefCardV1 struct {
	BatchID      int64
	TotalItems   int
	VisibleItems int
	WebURL       string
}

func (b CanonicalBriefCardV1) Validate(renderedItems int) error {
	if b.BatchID <= 0 || b.TotalItems <= 0 || b.VisibleItems <= 0 ||
		b.VisibleItems != renderedItems ||
		b.VisibleItems > b.TotalItems ||
		b.WebURL == "" || strings.TrimSpace(b.WebURL) != b.WebURL {
		return errors.New("canonical Brief card identity is invalid")
	}
	parsed, err := url.Parse(b.WebURL)
	if err != nil || parsed == nil ||
		(parsed.Scheme != "https" && parsed.Scheme != "http") ||
		parsed.Host == "" || parsed.User != nil ||
		(parsed.Path != "" && parsed.Path != "/") ||
		parsed.RawQuery != "" ||
		!strings.HasPrefix(parsed.Fragment, "/tasks/") {
		return errors.New("canonical Brief card URL is invalid")
	}
	if _, err := b.TaskID(); err != nil {
		return err
	}
	return nil
}

func (b CanonicalBriefCardV1) TaskID() (string, error) {
	parsed, err := url.Parse(b.WebURL)
	if err != nil || parsed == nil {
		return "", errors.New("canonical Brief card URL is invalid")
	}
	escapedFragment := parsed.EscapedFragment()
	encoded := strings.TrimPrefix(escapedFragment, "/tasks/")
	if encoded == "" || encoded == escapedFragment {
		return "", errors.New("canonical Brief card task is invalid")
	}
	taskID, err := url.PathUnescape(encoded)
	if err != nil || taskID == "" {
		return "", errors.New("canonical Brief card task is invalid")
	}
	return taskID, nil
}

// AggregateCardInput 聚合卡（一个任务一张卡，卡内 N 条情报）的构卡全量输入
// （card-redesign-spec.md 附录 A，2026-07-18 定稿）。
//
// HeaderTitle/HeaderTemplate 由两条路径填充，语义刻意不同：
//   - 首发（Push 活动）：由任务名派生（"{emoji} {任务名} · 今日 N 条" + 哈希取色）；
//   - 点击重建（feedback.rebuilt）：从库存 card_json 的 header **原样解析回填**——
//     重建时拿不到任务名（deliveries 不存它），而 header 在卡的生命周期内不该变，
//     解析存量比再传一遍任务名少一条数据通路，也不会因任务改名而让老卡 header 漂移。
type AggregateCardInput struct {
	HeaderTitle    string      // 完整标题串；空则构卡函数用兜底"📮 今日推送 · N 条"
	HeaderTemplate string      // 飞书 header template 色名；空则 "blue"
	EffectID       string      // durable push effect marker；空则保持历史卡字节形态
	Items          []CardInput // 按分数降序；每项的 DeliveryID/State 各自独立
	// CanonicalBrief is nil for every legacy/ad-hoc card. When present, Items
	// must be the exact ordered prefix of the immutable Brief.
	CanonicalBrief *CanonicalBriefCardV1
	// Executive is the channel-neutral synthesis frozen for this exact Brief.
	// It is nil for legacy cards and never carries profile text or provenance.
	Executive         *types.ExecutiveBriefContentV1
	ExecutiveFallback bool
	ExecutivePartial  bool
}

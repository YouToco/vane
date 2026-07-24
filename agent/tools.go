// agent 工具集（M4 契约 §8）：七个工具 + BuildTools 装配。
// Tool 接口本身在 loop.go（契约 §7）定义，本文件只实现具体工具类型。
//
// 错误分层约定（贯穿全部 Execute）：
//   - 模型可自纠的确定性失败（参数不是合法 JSON、字段校验不过）→ 返回中文文案 + nil error，
//     loop 会把文案作为 role=tool 结果回给模型继续多轮（或经确认卡展示给用户）；
//   - 基础设施失败（DB / Temporal）→ 返回 error 向上抛，由 feishu 层 humanize。
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/YouToco/vane/fetcher"
	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/sourcecatalog"
	"github.com/YouToco/vane/sourcespec"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/task"
	"github.com/YouToco/vane/types"
)

// PushTrigger 是 push_now 工具依赖的窄接口（契约 §8）：只暴露 userID 一个入参，
// 不把 workflow.PushScope 泄进工具面。*scheduler.Scheduler 已实现（TriggerPushNow）。
// agent 禁止 import api，即时触发经此接口而非 HTTP handler 复用。
type PushTrigger interface {
	TriggerPushNow(ctx context.Context, userID int64) (runID string, err error)
}

// scheduleUpdater / scheduleDeleter 收窄 update/remove_schedule 依赖的 scheduler 能力
// （*scheduler.Scheduler 都已实现）。
//
// 收窄的理由不只是"能起假实现"：Execute 把工具入参翻译成 scheduler.ScheduleSpec 的
// 那几行是**纯接线**，漏传一个字段不报错、只让该能力静默失效——本 PR 的对抗审查实测，
// 删掉 `AnchorAt: a.Spec.AnchorAt` 后全仓测试照样绿。有了接口，替身才能捕获真正传下去
// 的 spec，把"工具面广告的字段真的到达了 scheduler"钉进单测，而不是只断言 schema 有这个 key。
type scheduleUpdater interface {
	UpdatePush(ctx context.Context, schedID string, userID int64, spec scheduler.ScheduleSpec, nlDesc *string) error
}

type scheduleDeleter interface {
	DeletePush(ctx context.Context, schedID string, userID int64) error
}

// profileStore 是画像两工具依赖的窄接口（M5 契约 §12.3，*store.Store 已实现），
// 收窄后 Execute 分支可用内存假实现覆盖，不依赖数据库。
type profileStore interface {
	GetProfile(ctx context.Context, userID int64) (*types.Profile, error)
	UpsertProfileFields(ctx context.Context, userID int64, industry, occupation *string, tags []string) (*types.Profile, error)
}

// playbookStore 是任务手册三条路径（view/edit_task_playbook + create_schedule 初始化）
// 依赖的窄接口（*store.Store 已实现）。收窄理由同 profileStore：Execute 分支（rune 截断、
// 归属未命中处理）可用内存假实现覆盖；归属校验的 SQL WHERE 本身只由 store 集成测试覆盖
// （enable_source 先例）。Upsert 返回 ok bool（同 EnableSource 的 enabled bool）：
// false=任务不存在/非本人，未写任何行。
type playbookStore interface {
	GetSchedulePlaybook(ctx context.Context, userID int64, scheduleID string) (*types.SchedulePlaybook, error)
	UpsertSchedulePlaybook(ctx context.Context, userID int64, scheduleID, content string) (ok bool, err error)
	// SetFetchPlan 只改 fetch_plan（P1 编译层），归属同 Upsert 进 SQL；ok=false=任务不存在/非本人/无手册行。
	SetFetchPlan(ctx context.Context, userID int64, scheduleID string, plan json.RawMessage) (ok bool, err error)
	// GetOrCreateSource 材料化 plan 源（P1b b2）：不存在就建、已存在原样返回不覆写（不改用户既有源配置）。
	GetOrCreateSource(ctx context.Context, src *types.Source) (id int64, created bool, err error)
	// ReplaceScheduleSources 把本任务的「任务↔源」软范围链接整体替换为 sourceIDs（P1b b2），归属进 SQL。
	ReplaceScheduleSources(ctx context.Context, userID int64, scheduleID string, sourceIDs []int64) error
}

// BuildTools 装配 agent 全部可用工具。返回的切片即工具白名单的静态部分（契约 §10）：
// loop 只认这里注册的名字 + 会话已激活的 TikHub 端点（端点注册表契约 §4），
// 模型编造的其余工具名一律拒绝。
// endpoints 为 nil（TikHub key 未配置）时不装配 search_endpoints，工具面与
// 该特性上线前完全一致。
// tasks 仅保留给历史测试夹具。生产传 nil：遗留 v0 create_schedule 卡由 Loop
// 安全消费并要求用户重发，不再冒险走 active-first 补偿路径。
// prober 是试跑=准入入口（*fetcher.Multi，生产直接传 multi；1.5 起统一分派绑定能力
// 与 URL 类 web 能力的试跑）；nil 合法（测试/未装配）——退回不试跑直接落库。
// exa 是 Exa ad-hoc 工具对（web_search/read_page）；nil（Exa key 未配置）时不装配，
// 工具面与该特性上线前完全一致（同 endpoints 的 nil 语义）。
func BuildTools(st *store.Store, sched *scheduler.Scheduler, tasks taskCreator, pusher PushTrigger, endpoints *EndpointTools, prober sourceProber, exa *ExaTools, definitionEdits ...DefinitionEditController) []ToolSpec {
	tools := []ToolSpec{
		newToolSpec(&listSourcesTool{st: st}, a2aReadPolicy(Effects(EffectInternalRead, EffectTrustTaint))),
		newToolSpec(&addSourceTool{st: st, prober: prober}, ownerPolicy(
			Effects(EffectNetworkRead, EffectBillable, EffectStateWrite, EffectTrustTaint),
			ConfirmationRequired, BudgetDownstreamManaged)),
		newToolSpec(&removeSourceTool{st: st}, ownerPolicy(
			Effects(EffectStateWrite), ConfirmationRequired, BudgetNone)),
		newToolSpec(&enableSourceTool{st: st}, ownerPolicy(
			Effects(EffectStateWrite), ConfirmationRequired, BudgetNone)),
		newToolSpec(&listSchedulesTool{st: st}, a2aReadPolicy(Effects(EffectInternalRead))),
		newToolSpec(&createScheduleTool{tasks: tasks}, ownerPolicy(
			Effects(EffectDurableProposal, EffectStateWrite), ConfirmationRequired, BudgetNone)),
		newToolSpec(&removeScheduleTool{sched: sched}, ownerPolicy(
			Effects(EffectStateWrite), ConfirmationRequired, BudgetNone)),
		newToolSpec(&pushNowTool{pusher: pusher}, ownerPolicy(
			Effects(EffectDelivery), ConfirmationNone, BudgetDownstreamManaged)),
		newToolSpec(&viewProfileTool{st: st}, ownerPolicy(
			Effects(EffectInternalRead), ConfirmationNone, BudgetNone)),
		newToolSpec(&updateProfileTool{st: st}, ownerPolicy(
			Effects(EffectStateWrite), ConfirmationRequired, BudgetNone)),
		newToolSpec(&viewTaskPlaybookTool{st: st}, ownerPolicy(
			Effects(EffectInternalRead), ConfirmationNone, BudgetNone)),
	}
	if len(definitionEdits) == 1 && definitionEdits[0] != nil {
		tools = append(tools, newToolSpec(&editTaskDefinitionTool{}, ownerPolicy(
			Effects(EffectDurableProposal, EffectStateWrite),
			ConfirmationRequired, BudgetNone)))
	}
	if endpoints != nil {
		tools = append(tools, endpoints.SearchTool(), endpoints.ReadResultTool())
	}
	if exa != nil {
		tools = append(tools, exa.SearchTool(), exa.ReadPageTool())
	}
	return tools
}

// edit_task_definition is the only current definition writer. Its proposal,
// confirmation and cancellation are intercepted by Loop and delegated to the
// durable definition-edit controller; Execute is deliberately non-operational
// so a generic pending-action replay can never reach an old writer.
const editTaskDefinitionSchema = `{
  "type": "object",
  "properties": {
    "task_id": {
      "type": "string",
      "description": "要编辑的定时任务 id（先用 list_schedules 查询）"
    },
    "spec": {
      "type": "object",
      "description": "可选：替换触发频率；cron 与 every_seconds 必须且只能提供一个",
      "properties": {
        "cron": {"type": "string", "description": "5 段 cron，如 \"30 8 * * *\""},
        "every_seconds": {"type": "integer", "description": "固定间隔秒数，不低于 3600"},
        "anchor_at": {"type": "string", "description": "可选 RFC3339 锚点，只与 every_seconds 搭配"},
        "tz": {"type": "string", "description": "可选 IANA 时区，缺省 Asia/Shanghai"}
      },
      "additionalProperties": false
    },
    "intent": {
      "type": "string",
      "minLength": 1,
      "description": "可选：完整替换持续监控意图与任务手册；必须自包含，不能只写增量片段"
    },
    "nl_description": {
      "type": "string",
      "description": "可选：替换任务列表中的自然语言描述；空串表示清空"
    },
    "strictness": {
      "type": "string",
      "enum": ["loose", "normal", "strict"],
      "description": "可选：替换推送门槛"
    }
  },
  "required": ["task_id"],
  "additionalProperties": false
}`

type editTaskDefinitionTool struct{}

func (*editTaskDefinitionTool) Name() string { return "edit_task_definition" }
func (*editTaskDefinitionTool) Description() string {
	return "编辑已有定时任务的完整已批准定义。可一次修改触发频率、完整监控意图/手册、列表描述和推送门槛；未提供的字段保持不变。系统会冻结当前 definition head 与目标定义，先发原确认卡，确认后由唯一可恢复协调器执行。"
}
func (*editTaskDefinitionTool) Parameters() json.RawMessage {
	return json.RawMessage(editTaskDefinitionSchema)
}
func (*editTaskDefinitionTool) Execute(
	context.Context,
	int64,
	json.RawMessage,
) (string, error) {
	return "这张任务编辑确认不属于当前安全协议，请重新发起编辑。", nil
}
func (*editTaskDefinitionTool) Summarize(args json.RawMessage) string {
	return summarizeFallback("编辑定时推送任务", args)
}

// emptyParamsSchema 是无参工具的 JSON schema：仍须是合法 object schema，
// DeepSeek FC 对空 properties 已实测可用（M4 spike）。
const emptyParamsSchema = `{"type":"object","properties":{}}`

// summarizeFallback 在确认卡摘要解析 args 失败时兜底：展示截断后的原始参数，
// 保证卡片恒有内容可读（Summarize 无 error 通道，不能失败）。
func summarizeFallback(action string, args json.RawMessage) string {
	raw := []rune(string(args))
	if len(raw) > 200 {
		raw = append(raw[:200], '…')
	}
	return action + "（参数未能解析）：" + string(raw)
}

// ============================================================
// list_sources：读工具，列出当前用户订阅的全部信源。
// ============================================================

type listSourcesTool struct {
	st *store.Store
}

func (t *listSourcesTool) Name() string { return "list_sources" }

// 信源标题可能来自 RSS/网页等外部元数据；即使列表本身来自本地数据库，返回给
// 模型的文本仍不能被当作可信指令。
func (t *listSourcesTool) untrustedResult() bool { return true }
func (t *listSourcesTool) Description() string {
	return "列出用户当前订阅的全部信源（含 id、类型、标题、状态）。"
}
func (t *listSourcesTool) Parameters() json.RawMessage { return json.RawMessage(emptyParamsSchema) }

// Execute 用 ListSubscribedSourcesByUser（不过滤 source.status）而非 active-only：
// 与 GET /api/subscriptions 同语义——被自动 disabled 的源也要让用户看见并知道原因。
func (t *listSourcesTool) Execute(ctx context.Context, userID int64, _ json.RawMessage) (string, error) {
	sources, err := t.st.ListSubscribedSourcesByUser(ctx, userID)
	if err != nil {
		return "", err
	}
	if len(sources) == 0 {
		return "当前没有任何订阅信源。", nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "共 %d 个订阅信源：\n", len(sources))
	for _, src := range sources {
		title := src.Title
		if title == "" {
			title = src.URL
		}
		fmt.Fprintf(&b, "- id=%d [%s/%s] %s（状态: %s）\n", src.ID, src.Platform, src.Capability, title, src.Status)
	}
	return strings.TrimSuffix(b.String(), "\n"), nil
}

func (t *listSourcesTool) Summarize(json.RawMessage) string { return "" }

// ============================================================
// add_source：写工具，校验/构造复用 sourcespec（与 API 加订阅同一份规则）。
// ============================================================

// addSourceArgs 与工具 schema 对应。只认新 schema（platform + capability）——
// M6 契约 §13.1【硬约束】：legacy type 垫片只服务 HTTP api（vane-web 兼容窗口），
// 绝不进 agent 面，否则等于给模型两条重叠表达路（曾让 Boss 听见「已添加 tikhub_xhs 源」）。
type addSourceArgs struct {
	Platform       string   `json:"platform"`
	Capability     string   `json:"capability"`
	URL            string   `json:"url"`
	Query          string   `json:"query"`
	Keyword        string   `json:"keyword"`
	ScreenName     string   `json:"screen_name"`
	UserID         string   `json:"user_id"`     // xhs/user_posts、xhs/faved_notes：小红书用户 ID
	ProfileURL     string   `json:"profile_url"` // xhs 用户主页链接（可替代 user_id）；weibo 主页链接（可替代 uid）
	PageID         string   `json:"page_id"`     // xhs/topic_feed：话题页面 ID（24 位十六进制）
	TopicURL       string   `json:"topic_url"`   // xhs/topic_feed：话题链接/深链（可替代 page_id，自动抽 ID）
	UID            string   `json:"uid"`         // weibo/user_posts：微博用户数字 ID
	Username       string   `json:"username"`    // wechat_mp/user_posts：公众号原始 ID（gh_ 开头）
	Title          string   `json:"title"`
	Category       string   `json:"category"`
	Categories     []string `json:"categories"`
	IncludeDomains []string `json:"include_domains"` // 仅 web/search：Exa 域名白名单（追新解药）
}

// addSourceSchema 对齐 M6 信源插件化契约：只有新 schema（platform + capability + params）。
// M6 §13.1【硬约束】：legacy type 不进 agent 面（那是 HTTP api 给 vane-web 的兼容窗口）。
// url/query/keyword 的条件必填（随 platform/capability 而定）写进 description 由模型遵循，
// 权威校验在 sourcespec.Build（与 POST /api/subscriptions 完全一致）。
const addSourceSchema = `{
  "type": "object",
  "properties": {
    "platform": {
      "type": "string",
      "enum": ["web", "x", "xhs", "weibo", "wechat_mp"],
      "description": "内容平台：web=开放网页；x=X(Twitter)；xhs=小红书；weibo=微博；wechat_mp=微信公众号"
    },
    "capability": {
      "type": "string",
      "enum": ["feed", "search", "user_posts", "contents", "hot_list", "topic_feed", "faved_notes"],
      "description": "能力：feed=RSS/Atom 订阅（仅 web）；search=关键词/语义搜索（web=Exa 网页搜索，xhs=小红书关键词）；user_posts=订阅某账号的新发布（x=Twitter 账号，xhs=小红书博主，weibo=微博账号需 uid 或 profile_url，wechat_mp=公众号需 username）；contents=监控指定网页内容变化（仅 web，如产品定价页——内容变了才推送）；hot_list=平台热榜追新（xhs=小红书热榜，weibo=微博热搜，均无参数）；topic_feed=某话题下的新笔记（仅 xhs，需 page_id 或 topic_url）；faved_notes=某账号公开收藏的新笔记（仅 xhs，需 user_id 或 profile_url，对方收藏须公开）。当前不支持的能力及原因见本工具说明（Description）。"
    },
    "url": {"type": "string", "description": "网页地址（http/https）：platform=web capability=feed 时是 RSS 源地址，capability=contents 时是要监控变化的页面地址，均必填"},
    "query": {"type": "string", "description": "Exa 搜索词，platform=web capability=search 时必填"},
    "keyword": {"type": "string", "description": "小红书搜索关键词，platform=xhs capability=search 时必填"},
    "screen_name": {"type": "string", "description": "X 用户名（如 OpenAI），platform=x capability=user_posts 时必填"},
    "user_id": {"type": "string", "description": "小红书用户 ID（24 位十六进制），platform=xhs 且 capability=user_posts/faved_notes 时必填（或改用 profile_url）"},
    "profile_url": {"type": "string", "description": "用户主页链接：platform=xhs（如 https://www.xiaohongshu.com/user/profile/<id>）时可替代 user_id；platform=weibo（如 https://weibo.com/u/<数字>）时可替代 uid"},
    "uid": {"type": "string", "description": "微博用户数字 ID（如 2803301701），platform=weibo capability=user_posts 时必填（或改用 profile_url）。不知道 uid 时可用微博搜索端点按昵称检索获取"},
    "username": {"type": "string", "description": "公众号原始 ID（gh_ 开头，如 gh_363b924965e9），platform=wechat_mp capability=user_posts 时必填。可用微信搜索端点按公众号名称检索后从结果的 userName 字段获取"},
    "page_id": {"type": "string", "description": "小红书话题页面 ID（24 位十六进制），platform=xhs capability=topic_feed 时必填（或改用 topic_url）。可从笔记正文话题标签的深链（xhsdiscover://…topic/normal?id=…）或话题页链接中获得"},
    "topic_url": {"type": "string", "description": "小红书话题链接或深链，platform=xhs capability=topic_feed 时可替代 page_id（自动从中抽取 24 位十六进制 ID）"},
    "title": {"type": "string", "description": "可选：展示名，缺省按类型自动生成"},
    "category": {"type": "string", "description": "可选：Exa 结果类别（如 news），仅 web/search 生效"},
    "categories": {"type": "array", "items": {"type": "string"}, "description": "可选：RSS 分类过滤（如 [\"Product\",\"Research\"]），仅 web/feed 生效；不传=不限"},
    "include_domains": {"type": "array", "items": {"type": "string"}, "description": "可选：限定 Exa 搜索只返回这些域名的结果（如 [\"anthropic.com\",\"claude.com\"]），仅 web/search 生效；追新优先用它、避免日期过滤把无发布日期的官方页删光；不传=不限"}
  }
}`

// sourceProber 收窄试跑能力（*fetcher.Multi 已实现），可 fake 单测。
// 试跑=准入（endpoint-binding-contract.md §2.2，1.5 起推广到 URL 类 web 能力）：
// 真调一次上游，全过才落库。返回 (nil, nil) 表示该能力无试跑门（如 web/search），
// 直接落库——「哪些能力要试跑」是 fetcher 层知识，agent 不感知清单。
type sourceProber interface {
	Probe(ctx context.Context, src types.Source) (*fetcher.ProbeReport, error)
}

type addSourceTool struct {
	st     *store.Store
	prober sourceProber // nil = 不试跑（测试/未装配路径），绑定能力直接落库
}

// probeBudget 单次试跑的独立超时。卡片回调的执行预算 30s（feishu cardActionExecBudget），
// 超 2.5s 自动转异步补发结果，故 probe 拉长不阻塞用户。
//
// 取 25s 而非 10s（对抗审查 A-F3）：web/feed 试跑不是「一次上游调用」——它跑完整抓取路径
// 含正文补全（probeEnrichCap=5 条 × Exa /contents，每条含一次 GET），加上 feed 本体 GET 与
// 兜底嗅探的二次 GET。10s 会被慢源或 Exa 抖动打穿：部分条目未补全 → 报告条数系统性偏低，
// 或响应 10-20s 的慢源（RSS client 超时 20s，周期抓取能成）在添加期结构性误拒。25s > client
// 20s 超时，让真正的慢源以 CodeFetchTimeout（可重试）落「稍后再试」而非被拒，且 < 30s 执行
// 预算留回写余量。
const probeBudget = 25 * time.Second

func (t *addSourceTool) Name() string { return "add_source" }

// 确认执行期会真实 Probe 外部源，结果可能含上游声明 URL/样例标题。
// ExecuteAction 会把详细结果展示给用户，但用固定回执写入模型历史。
func (t *addSourceTool) untrustedResult() bool { return true }
func (t *addSourceTool) Description() string {
	return "添加一个信源并建立订阅。指定 platform（web/xhs/x/weibo/wechat_mp）和 capability（feed/search/user_posts/contents/hot_list/topic_feed/faved_notes）。" +
		"绑定类能力与 URL 类 web 能力（feed/contents）在确认后会先真实试跑一次，通过才落库。" +
		"用户给了一个网址但不确定是什么来源时：先按 web/feed 尝试（免费、增量语义最好）；" +
		"若试跑被拒，拒绝话术会给出解析结果与替代建议（页面声明的真实 feed 地址、或改用 " +
		"web/contents 监控页面变化、或改用 web/search 关键词订阅）——把建议转述给用户确认后按建议重新添加，" +
		"不要不经用户同意就换成别的能力或地址。" +
		unavailableCapabilitiesNote()
}

// unavailableCapabilitiesNote 从 sourcecatalog 派生「当前不支持的能力及原因」附到工具说明里。
//
// 存在的理由（契约 §2.2 + 本次审计缺陷）：让模型能主动回答"X 关键词搜索为何不支持"，
// 而不是静默改用别的能力。关键是**原因取自注册表单一事实来源**（sourcecatalog.List），
// 不再是手抄进 schema 的副本——审计发现旧写法把 x/search 的 Reason 硬编码复制到 schema
// 里，注册表一改这里就漂移。改成派生后，注册表与 agent 工具面自动同步，
// 「注册表被三处共用」这句话（fetcher 分发 / sourcespec 构造 / agent 描述）才真正成立。
func unavailableCapabilitiesNote() string {
	var lines []string
	for _, e := range sourcecatalog.List() {
		if e.Available() {
			continue
		}
		lines = append(lines, fmt.Sprintf("%s/%s：%s", e.Platform, e.Capability, e.Reason))
	}
	if len(lines) == 0 {
		return ""
	}
	return "\n\n当前不支持的能力（请勿尝试添加，会被拒绝）：\n- " + strings.Join(lines, "\n- ")
}
func (t *addSourceTool) Parameters() json.RawMessage { return json.RawMessage(addSourceSchema) }

// specFromArgs 把模型给的扁平入参映射为 sourcespec.Spec 的 params map。
// 抽成纯函数是为了让「哪个字段进哪个 param 键」这层 **agent 独有**的映射可被单测：
// Execute 持具体 *store.Store（不可 fake），而拼错键名（如 screen_name→screenname）
// 不会被 sourcespec 自己的用例发现——只会在生产里产出错误的确认卡预填（审计 M-3）。
func specFromArgs(a addSourceArgs) sourcespec.Spec {
	params := make(map[string]string)
	set := func(k, v string) {
		if v != "" {
			params[k] = v
		}
	}
	set("url", a.URL)
	set("query", a.Query)
	set("keyword", a.Keyword)
	set("category", a.Category)
	set("screen_name", a.ScreenName)
	set("user_id", a.UserID)
	set("profile_url", a.ProfileURL)
	set("page_id", a.PageID)
	set("topic_url", a.TopicURL)
	set("uid", a.UID)
	set("username", a.Username)
	if len(a.Categories) > 0 {
		catJSON, _ := json.Marshal(a.Categories)
		params["categories"] = string(catJSON)
	}
	// include_domains：[]string → JSON 字符串数组进 params（与 categories 同一序列化路径），
	// sourcespec 反序列化后归一化 + 入 config/幂等键（D-2 修复）。
	if len(a.IncludeDomains) > 0 {
		domJSON, _ := json.Marshal(a.IncludeDomains)
		params["include_domains"] = string(domJSON)
	}
	return sourcespec.Spec{
		Platform:   a.Platform,
		Capability: a.Capability,
		Params:     params,
		Title:      a.Title,
	}
}

func (t *addSourceTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	var a addSourceArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "参数不是合法 JSON，请修正后重试", nil
	}
	if a.Platform == "" {
		return "请指定 platform（web/xhs/x/weibo/wechat_mp）与 capability（feed/search/user_posts）", nil
	}
	src, msg := sourcespec.Build(specFromArgs(a))
	if msg != "" {
		return msg, nil
	}

	// 试跑=准入（契约 §2.2，1.5 起覆盖绑定能力 + web/feed + web/contents）：确认后先
	// 真调一次上游，全过才落库；失败不落任何行——消除「添加了一个永远抓不到内容的源、
	// 用户却被告知已订阅」的假装成功。哪些能力要试跑由 fetcher.Multi.Probe 决定
	// （返回 nil report = 无试跑门，直接落库）。
	// 红线 3（对抗审查 HIGH-3）：只有 ProbeRejection（准入话术，1.5 起自带替代建议）
	// 可原样透出；其余错误（漂移/网络/鉴权——内嵌端点名、上游 body）按错误码映射
	// 固定人话，原文只进 slog 与 tool_calls（记账在引擎内完成）。
	var probeNote string
	if t.prober != nil {
		probeCtx, cancel := context.WithTimeout(ctx, probeBudget)
		// 记账 trace/user = 会话归属（契约 §5）：probe 的计费调用可关联回发起
		// 会话，store 再由 user 推导 tenant；确认卡回调无会话 trace 时仍保留 user。
		var traceID string
		if m, ok := ctx.Value(chatMetaKey{}).(chatMeta); ok {
			traceID = m.traceID
		}
		probeCtx = fetcher.WithBindingAttribution(probeCtx, traceID, userID)
		report, perr := t.prober.Probe(probeCtx, *src)
		cancel()
		if perr != nil {
			slog.Warn("add_source: 试跑未通过",
				"platform", src.Platform, "capability", src.Capability, "err", perr)
			var pr *fetcher.ProbeRejection
			if errors.As(perr, &pr) {
				return "试跑未通过，未添加信源：" + pr.AE.Message, nil
			}
			var ae *types.AppError
			if errors.As(perr, &ae) {
				return "试跑未通过，未添加信源：" + probeUserText(ae, src.Platform), nil
			}
			return "", perr
		}
		if report != nil {
			if report.Extracted == 0 {
				// 只有 web/feed 会以 0 条通过（lookback 把旧条目全滤掉是合法的）；
				// 不解释会让用户以为订了个空源。
				probeNote = "；试跑通过：feed 有效，当前窗口内无新内容（新文章发布后会自动进入推送）"
			} else {
				probeNote = fmt.Sprintf("；试跑通过：提取 %d 条", report.Extracted)
				if report.Newest != nil {
					probeNote += fmt.Sprintf("，最新 %s", report.Newest.In(cstZone()).Format("2006-01-02 15:04"))
				}
				if len(report.SampleTitles) > 0 {
					probeNote += fmt.Sprintf("，如「%s」", report.SampleTitles[0])
				}
			}
		}
	}

	sourceID, updated, err := t.st.UpsertSource(ctx, src)
	if err != nil {
		return "", err
	}
	if err := t.st.AddSubscription(ctx, userID, sourceID); err != nil {
		return "", err
	}
	title := src.Title
	if title == "" {
		title = src.URL
	}
	verb := "已添加并订阅"
	if updated {
		// 措辞是"订阅"而非"更新"：config 现在是先到先得（既有值胜出、只补缺键），
		// 命中既有行时并不会改动它的配置，说"已更新"是假陈述。会改变抓取行为的入参
		// 一律进幂等键（不变量 I-S2），因此命中既有行 ⇒ 这确实就是同一个源。
		verb = "已订阅既有"
	}
	return fmt.Sprintf("%s信源（id=%d）：[%s/%s] %s%s", verb, sourceID, src.Platform, src.Capability, title, probeNote), nil
}

// cstZone 回执时间用东八区（Boss 在国内看卡片，UTC 时间读起来像穿越）。
func cstZone() *time.Location { return time.FixedZone("CST", 8*3600) }

// probeUserText 把非准入类的试跑失败映射成固定人话（红线 3）：这些错误的 Message
// 是为管理员 fail_count 通道写的，内嵌端点名/上游 body/内部 id，不得进用户/模型面。
func probeUserText(ae *types.AppError, platform types.Platform) string {
	switch ae.Code {
	case types.CodeFetchRateLimit:
		return "上游暂时限流，请稍后再试"
	case types.CodeFetchTimeout:
		// 公众号上游明示接口慢且「超时也已扣费」，而 probe 受卡片执行预算约束
		// （probeBudget=25s < 模板 30s）——话术如实告知费用面，避免用户当无事反复重试。
		if platform == types.PlatformWechatMP {
			return "上游响应超时（公众号接口偏慢，本次试跑可能已产生一次调用费用），请稍后再试"
		}
		return "上游暂时不可达或响应异常，请稍后再试"
	default:
		return "参数可能有误或该能力暂时不可用，请检查后重试；若反复失败请联系管理员"
	}
}

func (t *addSourceTool) Summarize(args json.RawMessage) string {
	var a addSourceArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return summarizeFallback("添加信源", args)
	}
	var b strings.Builder
	p, c := a.Platform, a.Capability
	switch p + "/" + c {
	case "web/feed":
		fmt.Fprintf(&b, "添加 RSS 信源：%s", a.URL)
	case "web/search":
		fmt.Fprintf(&b, "添加搜索信源：搜索词「%s」", strings.TrimSpace(a.Query))
		if a.Category != "" {
			fmt.Fprintf(&b, "，类别「%s」", a.Category)
		}
		if len(a.IncludeDomains) > 0 {
			fmt.Fprintf(&b, "，限定域名 %s", strings.Join(a.IncludeDomains, "、"))
		}
	case "xhs/search":
		fmt.Fprintf(&b, "添加小红书关键词信源：「%s」", strings.TrimSpace(a.Keyword))
	case "xhs/user_posts":
		who := strings.TrimSpace(a.UserID)
		if who == "" {
			who = strings.TrimSpace(a.ProfileURL)
		}
		fmt.Fprintf(&b, "添加小红书博主信源：%s", who)
	case "x/user_posts":
		fmt.Fprintf(&b, "添加 X 用户时间线信源：@%s", strings.TrimSpace(a.ScreenName))
	case "xhs/hot_list":
		b.WriteString("添加小红书热榜信源（全站热榜追新，确认后先试跑一次）")
	case "xhs/topic_feed":
		topic := strings.TrimSpace(a.PageID)
		if topic == "" {
			topic = strings.TrimSpace(a.TopicURL)
		}
		fmt.Fprintf(&b, "添加小红书话题信源：%s（确认后先试跑一次）", topic)
	case "xhs/faved_notes":
		who := strings.TrimSpace(a.UserID)
		if who == "" {
			who = strings.TrimSpace(a.ProfileURL)
		}
		fmt.Fprintf(&b, "添加小红书收藏流信源：%s（需对方收藏公开，确认后先试跑一次）", who)
	default:
		fmt.Fprintf(&b, "添加信源（%s/%s，确认后校验）", p, c)
	}
	if title := strings.TrimSpace(a.Title); title != "" {
		fmt.Fprintf(&b, "，展示名「%s」", title)
	}
	return b.String()
}

// ============================================================
// remove_source：写工具，取消订阅（信源本身与历史内容保留，与 DELETE /api/subscriptions 同语义）。
// ============================================================

const removeSourceSchema = `{
  "type": "object",
  "properties": {
    "source_ids": {
      "type": "array",
      "items": {"type": "integer"},
      "minItems": 1,
      "maxItems": 20,
      "description": "要取消订阅的信源 id 列表，一次最多 20 个（可先用 list_sources 查询）。用户一次要求退订多个时必须放进同一次调用，会合成一张确认卡一次确认。"
    }
  },
  "required": ["source_ids"]
}`

// removeSourceBatchMax 与 schema maxItems 保持一致：确认卡摘要要完整列出每个
// id，无上限会让卡片文案和单次事务规模都失控。
const removeSourceBatchMax = 20

type removeSourceTool struct {
	st *store.Store
}

func (t *removeSourceTool) Name() string { return "remove_source" }
func (t *removeSourceTool) Description() string {
	return "取消订阅一个或多个信源（信源本身与历史内容保留）。source_ids 可先用 list_sources 查询；批量退订放同一次调用，一张确认卡一次确认。"
}
func (t *removeSourceTool) Parameters() json.RawMessage { return json.RawMessage(removeSourceSchema) }

// removeSourceIDs 解析并规范化入参：schema 只声明 source_ids，但仍接受旧的
// 单数 source_id——pending_actions 里可能有部署前落库、24h 内被点击的存量行，
// 换 schema 不能让已发出的确认卡变成废卡。去重保序，全量校验后才动手。
// malformed 区分「JSON 本身坏」与「能解析但校验不过」：前者卡面只能走兜底，
// 后者要把 errText 原样上卡，用户在确认前就能看到这张卡不会执行。
func removeSourceIDs(args json.RawMessage) (ids []int64, errText string, malformed bool) {
	var a struct {
		SourceID  int64   `json:"source_id"`
		SourceIDs []int64 `json:"source_ids"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, "source_ids 必须是正整数数组", true
	}
	in := a.SourceIDs
	if len(in) == 0 && a.SourceID > 0 {
		in = []int64{a.SourceID}
	}
	if len(in) == 0 {
		return nil, "source_ids 必须是非空的正整数数组", false
	}
	if len(in) > removeSourceBatchMax {
		return nil, fmt.Sprintf("一次最多退订 %d 个信源", removeSourceBatchMax), false
	}
	seen := make(map[int64]bool, len(in))
	out := make([]int64, 0, len(in))
	for _, id := range in {
		if id <= 0 {
			return nil, "source_ids 必须是正整数数组", false
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out, "", false
}

func joinSourceIDs(ids []int64) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatInt(id, 10)
	}
	return strings.Join(parts, "、")
}

// Execute 复用 store.RemoveSubscription：按 (user_id, source_id) 删行，天然只能
// 删自己的订阅，无需再校验信源归属；删不存在的订阅静默成功（与 API 幂等语义一致）。
// 批量逐个删，中途失败如实报告已删部分——删除幂等，用户重新发起不会产生新副作用。
func (t *removeSourceTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	ids, errText, _ := removeSourceIDs(args)
	if errText != "" {
		return errText, nil
	}
	return removeSourcesSequentially(ctx, ids, func(ctx context.Context, id int64) error {
		return t.st.RemoveSubscription(ctx, userID, id)
	})
}

// removeSourcesSequentially 抽出批量循环便于单测（工具持具体 *store.Store 不可
// fake）。中途失败必须用**最外层** AppError 携带已删部分——feishu 卡片文案与
// 会话回调都只取 errors.As 命中的 AppError.Message，fmt.Errorf 包装文本到不了
// 用户面前，"如实报告已删部分"的承诺会变成只写日志的死话。
func removeSourcesSequentially(
	ctx context.Context, ids []int64, remove func(context.Context, int64) error,
) (string, error) {
	for i, id := range ids {
		if err := remove(ctx, id); err != nil {
			if i > 0 {
				return "", types.NewAppError(types.CodeDatabase, fmt.Sprintf(
					"已取消订阅信源（id=%s），但退订 id=%d 失败，其余未处理",
					joinSourceIDs(ids[:i]), id), err)
			}
			return "", err
		}
	}
	return fmt.Sprintf("已取消订阅信源（id=%s），信源与历史内容保留。", joinSourceIDs(ids)), nil
}

func (t *removeSourceTool) Summarize(args json.RawMessage) string {
	ids, errText, malformed := removeSourceIDs(args)
	if malformed {
		return summarizeFallback("取消订阅信源", args)
	}
	if errText != "" {
		// 能解析但校验不过：卡面必须诚实——这张卡点确认也不会执行。
		return fmt.Sprintf("取消订阅信源（参数无效：%s；确认后也不会执行）", errText)
	}
	if len(ids) == 1 {
		return fmt.Sprintf("取消订阅信源（id=%d）", ids[0])
	}
	return fmt.Sprintf("取消订阅 %d 个信源（id=%s）", len(ids), joinSourceIDs(ids))
}

const enableSourceSchema = `{
  "type": "object",
  "properties": {
    "source_id": {"type": "integer", "description": "要重新启用的信源 id（连续抓取失败被自动暂停的源，可先用 list_sources 查看状态）"}
  },
  "required": ["source_id"]
}`

type enableSourceTool struct {
	st *store.Store
}

func (t *enableSourceTool) Name() string { return "enable_source" }
func (t *enableSourceTool) Description() string {
	return "重新启用一个因连续抓取失败被自动暂停的信源：置回正常、清零失败计数、立即恢复抓取。source_id 可先用 list_sources 查看状态。"
}
func (t *enableSourceTool) Parameters() json.RawMessage { return json.RawMessage(enableSourceSchema) }

// Execute 复用 store.EnableSource：归属校验（本人 active 订阅）进 SQL 的 WHERE，
// 启用未订阅的源 enabled=false，回自纠文案而不上抛（与 remove_source 的越权处理一致）。
func (t *enableSourceTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	var a struct {
		SourceID int64 `json:"source_id"`
	}
	if err := json.Unmarshal(args, &a); err != nil || a.SourceID <= 0 {
		return "source_id 必须是正整数", nil
	}
	enabled, err := t.st.EnableSource(ctx, userID, a.SourceID)
	if err != nil {
		return "", err
	}
	if !enabled {
		return fmt.Sprintf("没找到你订阅的信源（id=%d），可能已取消订阅或 id 有误。用 list_sources 查一下。", a.SourceID), nil
	}
	return fmt.Sprintf("已重新启用信源（id=%d），失败计数已清零，将在下次抓取时恢复。", a.SourceID), nil
}

func (t *enableSourceTool) Summarize(args json.RawMessage) string {
	var a struct {
		SourceID int64 `json:"source_id"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return summarizeFallback("重新启用信源", args)
	}
	return fmt.Sprintf("重新启用信源（id=%d）", a.SourceID)
}

// ============================================================
// list_schedules：读工具，列出当前用户的全部定时推送任务。
// ============================================================

type listSchedulesTool struct {
	st *store.Store
}

func (t *listSchedulesTool) Name() string { return "list_schedules" }
func (t *listSchedulesTool) Description() string {
	return "列出用户当前的全部定时推送任务（含 id、触发频率、状态、描述）。"
}
func (t *listSchedulesTool) Parameters() json.RawMessage { return json.RawMessage(emptyParamsSchema) }

func (t *listSchedulesTool) Execute(ctx context.Context, userID int64, _ json.RawMessage) (string, error) {
	list, err := t.st.ListSchedulesByUser(ctx, userID)
	if err != nil {
		return "", err
	}
	if len(list) == 0 {
		return "当前没有任何定时推送任务。", nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "共 %d 个定时推送任务：\n", len(list))
	for _, sc := range list {
		desc := sc.NLDescription
		if desc == "" {
			desc = "（无描述）"
		}
		fmt.Fprintf(&b, "- id=%s %s（状态: %s，描述: %s）\n", sc.ID, formatSpecJSON(sc.SpecJSON), sc.Status, desc)
	}
	return strings.TrimSuffix(b.String(), "\n"), nil
}

func (t *listSchedulesTool) Summarize(json.RawMessage) string { return "" }

// formatSpecJSON 把镜像表里的 spec JSONB 渲染成中文频率描述；解析失败时
// 原样展示 JSON（列表工具不应因单条脏数据整体失败）。
func formatSpecJSON(raw json.RawMessage) string {
	var spec scheduler.ScheduleSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return "频率: " + string(raw)
	}
	return formatScheduleSpec(spec)
}

// formatScheduleSpec 渲染中立 spec 的中文频率描述。默认时区与 scheduler.defaultTZ 一致。
func formatScheduleSpec(spec scheduler.ScheduleSpec) string {
	tz := spec.TZ
	if tz == "" {
		tz = "Asia/Shanghai"
	}
	if spec.EverySeconds > 0 {
		// 有锚点时必须说出来：同样是"每 259200 秒"，有没有锚点决定了它落在
		// epoch 对齐点还是用户指定的时刻，卡面不提等于让用户无从判断对不对。
		if anchor := strings.TrimSpace(spec.AnchorAt); anchor != "" {
			return fmt.Sprintf("从 %s 起每 %d 秒触发一次（时区 %s）", anchor, spec.EverySeconds, tz)
		}
		return fmt.Sprintf("每 %d 秒触发一次（时区 %s，按 epoch 对齐）", spec.EverySeconds, tz)
	}
	return fmt.Sprintf("按 cron「%s」触发（时区 %s）", spec.Cron, tz)
}

// ============================================================
// create_schedule：写工具，前置校验对齐 api/schedules.go，权威校验在 scheduler。
// ============================================================

// minEverySeconds 与 api/schedules.go / scheduler 的 1 小时硬地板一致；
// 这里前置拦截只为尽早给模型清晰文案，scheduler.validateSpec 仍是权威校验方。
const minEverySeconds = 3600

const createScheduleSchema = `{
  "type": "object",
  "properties": {
    "spec": {
      "type": "object",
      "description": "触发频率：cron 与 every_seconds 必须且只能提供一个",
      "properties": {
        "cron": {"type": "string", "description": "5 段 cron（分 时 日 月 周），分钟字段必须是 0-59 的整数，如 \"0 8 * * *\""},
        "every_seconds": {"type": "integer", "description": "固定间隔秒数，不低于 3600（1 小时）"},
        "anchor_at": {"type": "string", "description": "可选：RFC3339 绝对时刻（如 \"2026-07-19T20:00:00+08:00\"），只能与 every_seconds 搭配。给了它触发点就从该时刻起按间隔推进（每 3 天的晚上 8 点 = every_seconds:259200 + 该时刻）；不给则按 Unix epoch 对齐（21600 会落在 00/06/12/18 点整，通常不是用户想要的）"},
        "tz": {"type": "string", "description": "可选：IANA 时区名，缺省 Asia/Shanghai"}
      }
    },
    "intent": {"type": "string", "minLength": 1, "description": "用户已经确认的持续监控目标与筛选范围。必须完整、自包含；它会成为任务手册，确认后不得由系统自行扩大主题或范围。"},
    "approved_fetch_plan": {
      "type": "object",
      "description": "待用户确认的完整长期抓取计划。可以用 existing_source_ids 引用 list_sources 返回的本人现有信源，也可以用 source_specs 提交新的原始信源规格；两者合计必须为 1-64 个。系统会在出确认卡前把规格确定性物化并冻结，模型绝不能编写 config、selectors 或 vane:// URL。",
      "properties": {
        "existing_source_ids": {
          "type": "array",
          "maxItems": 64,
          "uniqueItems": true,
          "description": "可选：list_sources 返回的本人 active 订阅信源 id。不要把 id 拼成 URL；系统会在提案时按当前授权解析并冻结完整值。",
          "items": {"type": "integer", "minimum": 1}
        },
        "source_specs": {
          "type": "object",
          "description": "可选：新的长期信源原始规格。只能使用下面的 kind 与对应的人类可读参数；系统负责生成内部 URL/config/title。可以与 existing_source_ids 组合，但不能提交旧式 sources。",
          "properties": {
            "version": {
              "type": "string",
              "enum": ["vane.source-specs/v1"],
              "description": "固定协议版本，必须逐字填写 vane.source-specs/v1"
            },
            "items": {
              "type": "array",
              "minItems": 1,
              "maxItems": 64,
              "description": "信源规格。kind 与参数必须匹配，不能夹带其他 kind 的字段。常用精确模板：网页搜索={\"kind\":\"web_search\",\"query\":\"主题\",\"include_domains\":[\"openai.com\"]}；普通页面={\"kind\":\"web_contents\",\"page_url\":\"https://...\"}；已知 RSS={\"kind\":\"web_feed\",\"feed_url\":\"https://...xml\"}。",
              "items": {
                "type": "object",
                "properties": {
                  "kind": {
                    "type": "string",
                    "enum": ["web_search", "web_feed", "web_contents", "x_user_posts", "xhs_search", "xhs_user_posts", "xhs_hot_list", "xhs_topic_feed", "xhs_faved_notes"],
                    "description": "web_search=网页搜索；web_feed=已知 RSS/Atom 地址；web_contents=监控已知页面；x_user_posts=X 账号；其余为对应小红书能力"
                  },
                  "query": {"type": "string", "description": "仅 web_search 必填：搜索词"},
                  "category": {"type": "string", "description": "仅 web_search 可选：Exa 类别，如 news"},
                  "include_domains": {
                    "type": "array",
                    "uniqueItems": true,
                    "items": {"type": "string"},
                    "description": "仅 web_search 可选：裸域名白名单，如 [\"openai.com\",\"anthropic.com\"]；不能含协议、路径、端口、通配符或 IP。用户只点名机构并要求官方来源时，可基于该机构填写对应官方根域名；确认卡会展示并冻结精确域名，绝不能加入用户未点名的机构、媒体或社区。"
                  },
                  "feed_url": {"type": "string", "description": "仅 web_feed 必填：已知 RSS/Atom 的 http/https 地址；不要把普通网页猜成 feed"},
                  "categories": {"type": "array", "items": {"type": "string"}, "description": "仅 web_feed 可选：RSS 分类过滤"},
                  "page_url": {"type": "string", "description": "仅 web_contents 必填：要监控的普通 http/https 页面地址"},
                  "screen_name": {"type": "string", "description": "仅 x_user_posts 必填：X 用户名"},
                  "keyword": {"type": "string", "description": "仅 xhs_search 必填：小红书搜索词"},
	                  "user_id": {"type": "string", "pattern": "^[0-9a-f]{24}$", "description": "仅 xhs_user_posts/xhs_faved_notes：24 位小写十六进制用户 ID，与 profile_url 二选一"},
	                  "profile_url": {"type": "string", "description": "仅 xhs_user_posts/xhs_faved_notes：小红书用户主页 https://www.xiaohongshu.com/user/profile/<24位小写十六进制ID>，与 user_id 二选一"},
	                  "page_id": {"type": "string", "pattern": "^[0-9a-f]{24}$", "description": "仅 xhs_topic_feed：24 位小写十六进制话题 ID，与 topic_url 二选一"},
                  "topic_url": {"type": "string", "description": "仅 xhs_topic_feed：与 page_id 二选一"}
                },
                "required": ["kind"],
                "additionalProperties": false
              }
            }
          },
          "required": ["version", "items"],
          "additionalProperties": false
        }
      },
      "additionalProperties": false
    },
    "nl_description": {"type": "string", "description": "可选：该任务的自然语言描述（如\"每天早上 8 点推送\"），用于列表展示"},
    "strictness": {"type": "string", "enum": ["loose", "normal", "strict"], "description": "可选：推送门槛档位，从用户对相关度的要求推断——「只要非常相关的/重大更新才推」→ strict（仅 ≥60 分高相关才推）；「一般相关就行」→ normal（≥40 分）；「都推来看看/宽松点」→ loose（只过滤与画像无关的内容）。用户没表态就不传（按系统兜底，等价 loose）"}
  },
  "required": ["spec", "intent", "approved_fetch_plan"],
  "additionalProperties": false
}`

// createScheduleArgs 与工具 schema 对应；spec 结构与 api 的 scheduleSpecDTO 一致。
type createScheduleArgs struct {
	Spec struct {
		Cron         string `json:"cron"`
		EverySeconds int    `json:"every_seconds"`
		AnchorAt     string `json:"anchor_at"`
		TZ           string `json:"tz"`
	} `json:"spec"`
	Intent            string          `json:"intent"`
	ApprovedFetchPlan json.RawMessage `json:"approved_fetch_plan"`
	NLDescription     string          `json:"nl_description"`
	Strictness        string          `json:"strictness"`
}

type approvedFetchPlanSummary struct {
	Sources []approvedFetchSourceSummary `json:"sources"`
}

type approvedFetchSourceSummary struct {
	Platform   string          `json:"platform"`
	Capability string          `json:"capability"`
	Title      string          `json:"title"`
	URL        string          `json:"url"`
	Config     json.RawMessage `json:"config"`
}

// taskCreator 是历史 v0 create_schedule 行为刻画的兼容接缝；生产不再装配。
type taskCreator interface {
	Create(ctx context.Context, input task.CreateInput) (task.CreateResult, error)
}

type createScheduleTool struct {
	tasks taskCreator
}

func (t *createScheduleTool) Name() string { return "create_schedule" }
func (t *createScheduleTool) Description() string {
	return "创建定时推送任务。必须同时提交用户批准的监控意图与长期抓取计划；已有信源用 list_sources 返回的 id 放进 existing_source_ids，新信源用版本化 source_specs 提交原始参数。系统会在确认卡前确定性生成并冻结内部信源，模型不得编写 config、selectors 或 vane:// URL。触发频率用 cron 或 every_seconds 二选一，频率不得高于每小时一次。"
}
func (t *createScheduleTool) Parameters() json.RawMessage {
	return json.RawMessage(createScheduleSchema)
}

// Execute 只服务历史 v0 卡片，保留其原有 best-effort 语义。新 v1 动作在 Loop 中
// 进入 durable controller，绝不会调用这里或在确认后重新选择长期信源。
func (t *createScheduleTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	if t.tasks == nil {
		return "这张旧版任务确认已失效，请重新描述需求以生成完整任务。", nil
	}
	var a createScheduleArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "参数不是合法 JSON，请修正后重试", nil
	}
	if msg := validateScheduleArgs(a); msg != "" {
		return msg, nil
	}
	// 档位在建调度**之前**校验：schema enum 只是软约束，模型乱传时要在产生任何
	// 副作用前拒绝——建完调度再发现档位非法，就只剩"带错档位继续"或"回滚调度"两个坏选项。
	if a.Strictness != "" && !types.PushStrictness(a.Strictness).Valid() {
		return "strictness 只能是 loose / normal / strict 之一（或不传）", nil
	}
	spec := scheduler.ScheduleSpec{
		Cron:         a.Spec.Cron,
		EverySeconds: a.Spec.EverySeconds,
		AnchorAt:     a.Spec.AnchorAt,
		TZ:           a.Spec.TZ,
	}
	result, err := t.tasks.Create(ctx, task.CreateInput{
		UserID:          userID,
		Spec:            spec,
		NLDescription:   a.NLDescription,
		PlaybookContent: capPlaybookContent(a.NLDescription),
		Strictness:      types.PushStrictness(a.Strictness),
	})
	if err != nil {
		// CreatePush 的 CodeValidation（cron 分钟字段过细、活跃上限等）是确定性、
		// 用户可修正的失败：转成文案返回而非向上抛，模型/卡片能给出可读提示；
		// 其余（Temporal/DB）仍作为基础设施错误上抛。
		var ae *types.AppError
		if errors.As(err, &ae) && ae.Code == types.CodeValidation {
			return ae.Message, nil
		}
		return "", err
	}
	reply := fmt.Sprintf("已创建定时推送任务（id=%s）：%s", result.ScheduleID, formatScheduleSpec(spec))
	if a.Strictness != "" && result.StrictnessApplied {
		v := types.PushStrictness(a.Strictness)
		reply += fmt.Sprintf("，推送门槛「%s」（%s）", strictnessLabel(v), strictnessDesc(v))
	}
	return reply, nil
}

// strictnessLabel / strictnessDesc 档位的人话名与一句话说明（工具回执/摘要共用）。
func strictnessLabel(v types.PushStrictness) string {
	switch v {
	case types.StrictnessStrict:
		return "严格"
	case types.StrictnessNormal:
		return "标准"
	default:
		return "宽松"
	}
}

func strictnessDesc(v types.PushStrictness) string {
	switch v {
	case types.StrictnessStrict:
		return fmt.Sprintf("仅 ≥%d 分的高相关内容才推送", v.MinKeepScore())
	case types.StrictnessNormal:
		return fmt.Sprintf("≥%d 分才推送，弱相关不打扰", v.MinKeepScore())
	default:
		return "只过滤与你画像无关的内容"
	}
}

func (t *createScheduleTool) Summarize(args json.RawMessage) string {
	var a createScheduleArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return summarizeFallback("创建定时推送任务", args)
	}
	s := "创建定时推送任务：" + formatScheduleSpec(scheduler.ScheduleSpec{
		Cron:         a.Spec.Cron,
		EverySeconds: a.Spec.EverySeconds,
		AnchorAt:     a.Spec.AnchorAt,
		TZ:           a.Spec.TZ,
	})
	if desc := strings.TrimSpace(a.NLDescription); desc != "" {
		s += fmt.Sprintf("，描述「%s」", desc)
	}
	if intent := strings.TrimSpace(a.Intent); intent != "" {
		s += "\n监控意图：" + intent
	}
	strictness := types.PushStrictness(a.Strictness)
	if strictness == "" {
		strictness = types.StrictnessLoose
	}
	s += fmt.Sprintf("\n推送门槛：%s（%s）", strictnessLabel(strictness), strictnessDesc(strictness))
	var plan approvedFetchPlanSummary
	if err := json.Unmarshal(a.ApprovedFetchPlan, &plan); err == nil && len(plan.Sources) > 0 {
		s += fmt.Sprintf("\n批准信源（%d）：", len(plan.Sources))
		for _, source := range plan.Sources {
			label := strings.TrimSpace(source.Title)
			if label == "" {
				label = source.Platform + "/" + source.Capability
			}
			config := strings.TrimSpace(string(source.Config))
			if config == "" {
				config = "{}"
			}
			s += fmt.Sprintf("\n- %s [%s/%s] %s；参数 %s",
				label, source.Platform, source.Capability, source.URL, config)
		}
	}
	return s
}

// validateScheduleSpecFields 是 create_schedule / update_schedule 共用的 spec 前置校验，
// 与 api/schedules.go 的 toScheduleSpec 逐条对齐：cron 与 every_seconds 恰好提供其一；
// every_seconds 不低于 1h 地板。文案同 api。
//
// 抽成共用函数而不是两处各写一遍：两个工具对同一个 spec 结构给出不同的拒绝理由，
// 就等于同一条规则在系统里有两个版本，改地板时必漏一处。cron 频率的权威校验仍在
// scheduler.validateCronMinInterval（这里只做能尽早给出清晰文案的那部分）。
func validateScheduleSpecFields(cron string, everySeconds int) string {
	hasCron := strings.TrimSpace(cron) != ""
	hasEvery := everySeconds > 0
	if hasCron == hasEvery {
		return "spec 必须且只能提供 cron 或 every_seconds 之一"
	}
	if hasEvery && everySeconds < minEverySeconds {
		return "推送间隔不得小于 1 小时"
	}
	return ""
}

// validateScheduleArgs 是 create_schedule 的入口（保留原名，语义不变）。
func validateScheduleArgs(a createScheduleArgs) string {
	return validateScheduleSpecFields(a.Spec.Cron, a.Spec.EverySeconds)
}

// ============================================================
// update_schedule：写工具，原地改已有调度的触发频率（不删重建，schedule_id 不变）。
// ============================================================

const updateScheduleSchema = `{
  "type": "object",
  "properties": {
    "schedule_id": {"type": "string", "description": "要修改的定时任务 id（先用 list_schedules 查）"},
    "spec": {
      "type": "object",
      "description": "新的触发频率：cron 与 every_seconds 必须且只能提供一个",
      "properties": {
        "cron": {"type": "string", "description": "5 段 cron（分 时 日 月 周），按时区的墙上时间触发，分钟字段必须是 0-59 的整数，如 \"30 8 * * *\"=每天 8:30。要「每天某个具体时刻」一律用 cron"},
        "every_seconds": {"type": "integer", "description": "固定间隔秒数，不低于 3600。不配 anchor_at 时触发点按 Unix epoch 对齐（21600=6 小时落在 00/06/12/18 点整），要指定从哪个时刻起请配 anchor_at"},
        "anchor_at": {"type": "string", "description": "可选：RFC3339 绝对时刻（如 \"2026-07-19T20:00:00+08:00\"），只能与 every_seconds 搭配。给了它触发点就从该时刻起按间隔推进（每 3 天的晚上 8 点 = every_seconds:259200 + 该时刻）；不给则按 Unix epoch 对齐（21600 会落在 00/06/12/18 点整，通常不是用户想要的）"},
        "tz": {"type": "string", "description": "可选：IANA 时区名，缺省 Asia/Shanghai"}
      }
    },
    "nl_description": {"type": "string", "description": "可选：同时更新该任务的自然语言描述（如\"每天早上 8 点半推送\"）；省略则保持原描述不变"}
  },
  "required": ["schedule_id", "spec"]
}`

type updateScheduleArgs struct {
	ScheduleID string `json:"schedule_id"`
	Spec       struct {
		Cron         string `json:"cron"`
		EverySeconds int    `json:"every_seconds"`
		AnchorAt     string `json:"anchor_at"`
		TZ           string `json:"tz"`
	} `json:"spec"`
	// 指针区分「省略=不改描述」与「显式置空」，与 store.UpdateScheduleSpec 的 nil 语义对应。
	NLDescription *string `json:"nl_description"`
}

type updateScheduleTool struct {
	sched scheduleUpdater
}

func (t *updateScheduleTool) Name() string { return "update_schedule" }
func (t *updateScheduleTool) Description() string {
	return "修改已有定时推送任务的触发频率（原地改，schedule_id 不变，不会中断调度）。" +
		"要改推送时间用本工具，不要用「删除再新建」——那会换掉 id 且中间有空窗。" +
		"频率用 cron（5 段，按墙上时间）或 every_seconds（固定间隔；配 anchor_at 可指定从哪个时刻起，" +
		"不配则按 epoch 对齐）二选一，不得高于每小时一次。"
}
func (t *updateScheduleTool) Parameters() json.RawMessage {
	return json.RawMessage(updateScheduleSchema)
}

func (t *updateScheduleTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	var a updateScheduleArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "参数不是合法 JSON，请修正后重试", nil
	}
	schedID := strings.TrimSpace(a.ScheduleID)
	if schedID == "" {
		return "schedule_id 必须是非空字符串（可先用 list_schedules 查询）", nil
	}
	if msg := validateScheduleSpecFields(a.Spec.Cron, a.Spec.EverySeconds); msg != "" {
		return msg, nil
	}
	spec := scheduler.ScheduleSpec{
		Cron:         a.Spec.Cron,
		EverySeconds: a.Spec.EverySeconds,
		AnchorAt:     a.Spec.AnchorAt,
		TZ:           a.Spec.TZ,
	}
	if err := t.sched.UpdatePush(ctx, schedID, userID, spec, a.NLDescription); err != nil {
		// 与 create_schedule 同约定：确定性、用户可修正的失败（cron 过细、任务不存在）
		// 转成文案回给模型自纠；基础设施错误上抛。
		var ae *types.AppError
		if errors.As(err, &ae) && (ae.Code == types.CodeValidation || ae.Code == types.CodeNotFound) {
			return ae.Message, nil
		}
		return "", err
	}
	return fmt.Sprintf("已修改定时推送任务（id=%s）：%s", schedID, formatScheduleSpec(spec)), nil
}

func (t *updateScheduleTool) Summarize(args json.RawMessage) string {
	var a updateScheduleArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return summarizeFallback("修改定时推送任务", args)
	}
	s := fmt.Sprintf("修改定时推送任务（id=%s）：触发频率改为 %s",
		strings.TrimSpace(a.ScheduleID),
		formatScheduleSpec(scheduler.ScheduleSpec{
			Cron:         a.Spec.Cron,
			EverySeconds: a.Spec.EverySeconds,
			AnchorAt:     a.Spec.AnchorAt, // 漏了它，卡面会说"按 epoch 对齐"而实际锚定——主动说反话
			TZ:           a.Spec.TZ,
		}))
	// 描述只在显式提供时上卡：省略=不改，卡面不能让人以为描述会被动。
	if a.NLDescription != nil {
		if desc := strings.TrimSpace(*a.NLDescription); desc != "" {
			s += fmt.Sprintf("，描述改为「%s」", desc)
		} else {
			s += "，并清空描述"
		}
	}
	return s
}

// ============================================================
// remove_schedule：写工具，删除 Temporal 调度 + 镜像。
// ============================================================

const removeScheduleSchema = `{
  "type": "object",
  "properties": {
    "schedule_id": {"type": "string", "description": "要删除的定时任务 id（可先用 list_schedules 查询）"}
  },
  "required": ["schedule_id"]
}`

type removeScheduleTool struct {
	sched scheduleDeleter
}

func (t *removeScheduleTool) Name() string { return "remove_schedule" }
func (t *removeScheduleTool) Description() string {
	return "删除指定定时推送任务。schedule_id 可先用 list_schedules 查询。"
}
func (t *removeScheduleTool) Parameters() json.RawMessage {
	return json.RawMessage(removeScheduleSchema)
}

// Execute 的归属校验由 Scheduler.DeletePush 内的 GetSchedule(id, userID) 承担：
// 「不存在」与「不属于你」归一为 NotFound，agent 拿到伪造的 schedule_id 也删不动别人的。
//
// 原注释写着「不逐条校验调度归属：单 owner MVP 所有调度同属一人」——契约 §2.8 曾据此
// 把本处列为已知越权洞。校验后来补上了，注释却留在原地。**这条路径尤其危险**：
// schedule_id 直接来自模型生成的工具入参，而模型的输入里混着不可信的外部内容
// （抓来的正文、用户消息），提示注入完全可能让它去删一个别人的 id。
// 守卫见 store/schedule_ownership_test.go。
func (t *removeScheduleTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	var a struct {
		ScheduleID string `json:"schedule_id"`
	}
	if err := json.Unmarshal(args, &a); err != nil || strings.TrimSpace(a.ScheduleID) == "" {
		return "schedule_id 必须是非空字符串", nil
	}
	schedID := strings.TrimSpace(a.ScheduleID)
	if err := t.sched.DeletePush(ctx, schedID, userID); err != nil {
		return "", err
	}
	return fmt.Sprintf("已删除定时推送任务（id=%s）。", schedID), nil
}

func (t *removeScheduleTool) Summarize(args json.RawMessage) string {
	var a struct {
		ScheduleID string `json:"schedule_id"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return summarizeFallback("删除定时推送任务", args)
	}
	return fmt.Sprintf("删除定时推送任务（id=%s）", strings.TrimSpace(a.ScheduleID))
}

// ============================================================
// push_now：读工具（低危，契约 §8）：立即触发一次推送，不建调度、不改任何配置，
// 故不走确认卡。经 PushTrigger 窄接口触发（与 POST /api/push/now 同款底层）。
// 免确认的扇出护栏在 scheduler.TriggerPushNow：确定性 workflow ID 把同一用户的
// 并发管道钉死为 1（审查 #push_now 扇出），模型多次调用只会收到"已在进行"文案。
// ============================================================

type pushNowTool struct {
	pusher PushTrigger
}

func (t *pushNowTool) Name() string { return "push_now" }
func (t *pushNowTool) Description() string {
	return "立即触发一次推送（推送用户全部订阅的最新内容，不创建定时任务）。"
}
func (t *pushNowTool) Parameters() json.RawMessage { return json.RawMessage(emptyParamsSchema) }

func (t *pushNowTool) Execute(ctx context.Context, userID int64, _ json.RawMessage) (string, error) {
	_, err := t.pusher.TriggerPushNow(ctx, userID)
	if err != nil {
		// 确定性拒绝（并发护栏"已有推送在进行"）走自纠通道：文案回给模型，
		// 由它向用户解释而不是重复触发；基础设施错误仍上抛。
		// 只回 AppError.Message：Error() 会拼上 Cause（Temporal 服务端原文
		// "Workflow execution is already running" 之类），错误链不进模型上下文。
		if errors.Is(err, types.ErrValidation) {
			var ae *types.AppError
			if errors.As(err, &ae) && ae.Message != "" {
				return ae.Message, nil
			}
			return "本次触发被拒绝，请稍后再试。", nil
		}
		return "", err
	}
	return "已触发一次立即推送，系统正在汇总你所有订阅信源的最新内容，推送卡片稍后会送达飞书，请留意。", nil
}

func (t *pushNowTool) Summarize(json.RawMessage) string { return "" }

// ============================================================
// view_profile：读工具，查看当前用户画像（M5 契约 §12.3）。
// ============================================================

type viewProfileTool struct {
	st profileStore
}

func (t *viewProfileTool) Name() string { return "view_profile" }
func (t *viewProfileTool) Description() string {
	return "查看用户当前画像（行业、职业、关注标签、摘要）。修改画像前应先调用本工具取现有标签，合并后再提交。"
}
func (t *viewProfileTool) Parameters() json.RawMessage { return json.RawMessage(emptyParamsSchema) }

// Execute NotFound 回固定引导文案（契约 §12.3 锁死文本）而非报错：画像为空是
// 常态起点，systemPrompt 会驱动模型据此自然引导首采。
func (t *viewProfileTool) Execute(ctx context.Context, userID int64, _ json.RawMessage) (string, error) {
	p, err := t.st.GetProfile(ctx, userID)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return "画像为空：还不了解你。可以告诉我你的行业、职业和关注的主题。", nil
		}
		return "", err
	}
	return "当前画像——" + renderProfile(p), nil
}

func (t *viewProfileTool) Summarize(json.RawMessage) string { return "" }

// ============================================================
// update_profile：写工具，画像首采（2.1）与人工修正（2.3）共用，走 M4 标准确认卡
// （首采不特例：AI 出预填、人点执行的不变式对写画像同样成立）。
// summary 刻意不在工具面：归演化独有；标签删除只在这里发生（演化只增不减）。
// ============================================================

// maxProfileTags 人工标签上限（契约 §2：与库内/演化上限统一为 12，超 12 截前 12——
// 截断而非报错，人工整体替换不得静默丢演化标签，也不得因超限整次作废）。
const maxProfileTags = 12

const updateProfileSchema = `{
  "type": "object",
  "properties": {
    "industry": {"type": "string", "description": "所在行业，省略表示不修改"},
    "occupation": {"type": "string", "description": "职业/岗位，省略表示不修改"},
    "tags": {
      "type": "array",
      "items": {"type": "string"},
      "description": "关注标签列表，整体替换现有标签：先用 view_profile 取现有标签、合并后完整提供（缺了的会被删除），最多 12 个；省略表示不修改"
    }
  }
}`

// updateProfileArgs 以指针/nil 切片区分「省略=不改」与「显式置空」，
// 与 store.UpsertProfileFields 的 nil 语义一一对应。
type updateProfileArgs struct {
	Industry   *string  `json:"industry"`
	Occupation *string  `json:"occupation"`
	Tags       []string `json:"tags"`
}

type updateProfileTool struct {
	st profileStore
}

func (t *updateProfileTool) Name() string { return "update_profile" }
func (t *updateProfileTool) Description() string {
	return "更新用户画像：行业、职业、关注标签。tags 是整体替换，提交前先用 view_profile 查看现有标签并合并；省略的字段保持不变。"
}
func (t *updateProfileTool) Parameters() json.RawMessage {
	return json.RawMessage(updateProfileSchema)
}

// Execute（确认后执行）：全缺省是确定性可自纠失败，回文案不触库；
// UpsertProfileFields 部分更新（nil 不改），不触碰 summary/游标/token 三件套。
func (t *updateProfileTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	var a updateProfileArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "参数不是合法 JSON，请修正后重试", nil
	}
	if a.Industry == nil && a.Occupation == nil && a.Tags == nil {
		return "没有提供任何要修改的字段，请至少提供 industry、occupation、tags 之一", nil
	}
	p, err := t.st.UpsertProfileFields(ctx, userID, a.Industry, a.Occupation, capProfileTags(a.Tags))
	if err != nil {
		return "", err
	}
	return "画像已更新。当前画像——" + renderProfile(p), nil
}

// Summarize 只列提供的字段（契约 §12.3）：确认卡如实展示本次会改什么，
// 未提供的字段绝不出现——「不改」与「清空」在卡面上必须可区分。
func (t *updateProfileTool) Summarize(args json.RawMessage) string {
	var a updateProfileArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return summarizeFallback("更新画像", args)
	}
	var parts []string
	if a.Industry != nil {
		parts = append(parts, profileFieldChange("行业", *a.Industry))
	}
	if a.Occupation != nil {
		parts = append(parts, profileFieldChange("职业", *a.Occupation))
	}
	if a.Tags != nil {
		if tags := capProfileTags(a.Tags); len(tags) == 0 {
			parts = append(parts, "清空关注标签")
		} else {
			// 展示截断后的实际生效值：卡面列 13 个而只落 12 个是对用户撒谎。
			parts = append(parts, "关注标签整体替换为「"+strings.Join(tags, "、")+"」")
		}
	}
	if len(parts) == 0 {
		return "更新画像（未提供任何字段，确认后不会有变更）"
	}
	return "更新画像：" + strings.Join(parts, "；")
}

// capProfileTags 超 12 截前 12。nil 原样透传：nil=不改，截断不得把它变成非 nil 置空。
func capProfileTags(tags []string) []string {
	if len(tags) > maxProfileTags {
		return tags[:maxProfileTags]
	}
	return tags
}

// profileFieldChange 渲染单字段变更描述；空串是显式清空，卡面要说人话。
func profileFieldChange(name, v string) string {
	if strings.TrimSpace(v) == "" {
		return "清空" + name
	}
	return name + "改为「" + v + "」"
}

// renderProfile 把画像渲染成给模型/用户看的中文单段（view_profile 与 update_profile
// 结果共用）。行业/职业/标签为空时显式标注「未设置」：模型据此知道缺什么、该引导
// 采集什么；summary 归演化独有，为空整段省略，不引导模型对它下手。
func renderProfile(p *types.Profile) string {
	var b strings.Builder
	b.WriteString("行业：" + orUnset(p.Industry))
	b.WriteString("；职业：" + orUnset(p.Occupation))
	if len(p.Tags) > 0 {
		b.WriteString("；关注标签：" + strings.Join(p.Tags, "、"))
	} else {
		b.WriteString("；关注标签：（未设置）")
	}
	if s := strings.TrimSpace(p.Summary); s != "" {
		b.WriteString("；摘要：" + s)
	}
	return b.String()
}

func orUnset(s string) string {
	if strings.TrimSpace(s) == "" {
		return "（未设置）"
	}
	return s
}

// ============================================================
// 情报任务手册（Task Playbook P0）：每个定时任务 1:1 绑一份自然语言手册。
// P0 只做存取——view（读）/ edit（写，走确认卡）/ create_schedule 创建即初始化。
// 手册不影响任何抓取/打分/出卡（那是 P1）。
// ============================================================

// maxPlaybookContentRunes 手册内容 rune 上限：超出截断（不报错，同 capProfileTags 策略），
// 防超长手册撑爆确认卡与 DB 行。playbookSummaryPreviewRunes 是确认卡摘要预览的更短上限。
const (
	maxPlaybookContentRunes     = 4000
	playbookSummaryPreviewRunes = 80
)

// capPlaybookContent 超上限截前 N rune（不加省略号——落库内容要干净）。
func capPlaybookContent(s string) string {
	r := []rune(s)
	if len(r) > maxPlaybookContentRunes {
		return string(r[:maxPlaybookContentRunes])
	}
	return s
}

const viewTaskPlaybookSchema = `{
  "type": "object",
  "properties": {
    "schedule_id": {"type": "string", "description": "要查看手册的定时任务 id（先用 list_schedules 查询）"}
  },
  "required": ["schedule_id"]
}`

type viewTaskPlaybookTool struct {
	st playbookStore
}

func (t *viewTaskPlaybookTool) Name() string { return "view_task_playbook" }
func (t *viewTaskPlaybookTool) Description() string {
	return "查看某个定时推送任务的情报手册（描述这个任务要抓什么、关注哪些主题、偏好哪些来源）。schedule_id 可先用 list_schedules 查询。修改手册前应先调用本工具取现有内容。"
}
func (t *viewTaskPlaybookTool) Parameters() json.RawMessage {
	return json.RawMessage(viewTaskPlaybookSchema)
}

// Execute NotFound 回引导文案而非报错：手册不存在（老任务未迁移）或任务非本人，
// 对模型都是"没有可看的手册"，回自纠文案让它引导用户；基础设施错误上抛。
func (t *viewTaskPlaybookTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	var a struct {
		ScheduleID string `json:"schedule_id"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return "参数不是合法 JSON，请修正后重试", nil
	}
	schedID := strings.TrimSpace(a.ScheduleID)
	if schedID == "" {
		return "schedule_id 必须是非空字符串（可先用 list_schedules 查询）", nil
	}
	pb, err := t.st.GetSchedulePlaybook(ctx, userID, schedID)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return fmt.Sprintf("没找到你的定时任务（id=%s），或它还没有手册。用 list_schedules 查一下。", schedID), nil
		}
		return "", err
	}
	return renderPlaybook(pb), nil
}

func (t *viewTaskPlaybookTool) Summarize(json.RawMessage) string { return "" }

// renderPlaybook 把手册渲染成给模型/用户看的中文文本：手册正文 +（P1 编译层）据此编译出的
// 抓取计划摘要。content 为空时给引导文案，让模型知道该采集什么；fetch_plan 无源时不赘述。
func renderPlaybook(pb *types.SchedulePlaybook) string {
	content := strings.TrimSpace(pb.Content)
	if content == "" {
		content = "（手册为空——可以告诉我这个任务要抓什么、关注哪些主题、偏好哪些来源。）"
	}
	out := fmt.Sprintf("任务手册（id=%s）：\n%s", pb.ScheduleID, content)
	if plan := renderFetchPlan(pb.FetchPlan); plan != "" {
		out += "\n\n" + plan
	}
	return out
}

// renderFetchPlan 渲染 fetch_plan 里已编译的源；无源/不合法时返回空串（渲染方不赘述）。
func renderFetchPlan(raw json.RawMessage) string {
	var plan FetchPlan
	if len(raw) == 0 || json.Unmarshal(raw, &plan) != nil || len(plan.Sources) == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "据此编译出的抓取计划（%d 个源）：", len(plan.Sources))
	for _, s := range plan.Sources {
		title := s.Title
		if title == "" {
			title = s.URL
		}
		fmt.Fprintf(&b, "\n- [%s/%s] %s", s.Platform, s.Capability, title)
	}
	return b.String()
}

// compilePlaybookPlan 把手册正文翻译成 fetch_plan 并落库（P1 编译层的工具侧胶水）。
// **best-effort**：翻译器未装配（tr==nil）或任何一步失败都只 slog、绝不影响主效果
// （调度已建 / 手册正文已存）。返回成功落库的计划源数（>=0），供 edit 回执展示。
func compilePlaybookPlan(ctx context.Context, st playbookStore, tr playbookTranslator, userID int64, scheduleID, content string) int {
	if tr == nil {
		return 0 // 未装配 LLM 翻译器（部分测试/降级路径）：手册仍可用，只是不编译计划。
	}
	plan, err := tr.Translate(ctx, userID, content)
	if err != nil {
		// 翻译失败（LLM 调用错误 / 输出取不出 JSON）：保留既有计划不动，只记日志。
		slog.Warn("agent: 手册编译成抓取计划失败（手册已存，本次不更新计划）", "schedule_id", scheduleID, "err", err)
		return 0
	}
	ok, serr := st.SetFetchPlan(ctx, userID, scheduleID, plan)
	if serr != nil {
		slog.Error("agent: 抓取计划落库失败", "schedule_id", scheduleID, "err", serr)
		return 0
	}
	if !ok {
		slog.Warn("agent: 抓取计划落库未命中手册行（任务不存在/非本人/无手册行）", "schedule_id", scheduleID)
		return 0
	}
	// b2：把计划里的源材料化成真源行 + 同步「任务↔源」软范围链接（schedule_sources）。
	// best-effort、不改回执：链接同步失败不影响已落库的 fetch_plan，也不改用户看到的源数。
	// 软范围≠硬闸门：只圈定本任务取材范围，不建订阅、不碰 agent 的配源/搜索工具面。
	syncPlaybookSources(ctx, st, userID, scheduleID, plan)
	return countPlanSources(plan)
}

// syncPlaybookSources 把 fetch_plan 里的每个源材料化成真 sources 行（GetOrCreateSource：
// 已存在不覆写），再把本任务的 schedule_sources 链接整体替换为这些源（P1b b2）。best-effort：
// 单源材料化失败跳过并 warn；链接同步失败只记 error。b2 阶段这些链接尚无人消费（b3 才读）。
func syncPlaybookSources(ctx context.Context, st playbookStore, userID int64, scheduleID string, planJSON json.RawMessage) {
	var plan FetchPlan
	if len(planJSON) == 0 || json.Unmarshal(planJSON, &plan) != nil {
		return
	}
	ids := make([]int64, 0, len(plan.Sources))
	for _, ps := range plan.Sources {
		src := &types.Source{
			Platform:   types.Platform(ps.Platform),
			Capability: types.Capability(ps.Capability),
			URL:        ps.URL,
			Title:      ps.Title,
			Config:     ps.Config,
		}
		id, _, err := st.GetOrCreateSource(ctx, src)
		if err != nil {
			// 有源没材料化成：**不半更新链接**。ReplaceScheduleSources 是整体替换，
			// 若拿"成功子集"去替换，会把 fetch_plan 里仍列出、只是本次瞬时建源失败的那些源的
			// 旧链接误删——fetch_plan 与 schedule_sources 就分叉了。保留既有链接不动，下次
			// 改手册（或重试）自愈，比"因一次 DB 抖动静默丢一条正确链接"安全。
			slog.Warn("agent: 部分计划源材料化失败，跳过本次链接同步（保留既有链接，避免与 fetch_plan 分叉）",
				"schedule_id", scheduleID, "url", ps.URL, "err", err)
			return
		}
		ids = append(ids, id)
	}
	// 全部材料化成功（含"计划本就零源"→ ids 空 → 清空该任务链接，正当）才整体替换链接。
	if err := st.ReplaceScheduleSources(ctx, userID, scheduleID, ids); err != nil {
		slog.Error("agent: 同步任务源链接失败", "schedule_id", scheduleID, "err", err)
	}
}

const editTaskPlaybookSchema = `{
  "type": "object",
  "properties": {
    "schedule_id": {"type": "string", "description": "要修改手册的定时任务 id（先用 list_schedules 查询）"},
    "content": {"type": "string", "description": "手册全文（自然语言，整体替换现有手册）：描述这个情报任务要抓什么、关注哪些主题、偏好哪些来源。提交前先用 view_task_playbook 取现有内容并合并。最多约 4000 字，超出自动截断。"}
  },
  "required": ["schedule_id", "content"]
}`

type editTaskPlaybookArgs struct {
	ScheduleID string `json:"schedule_id"`
	Content    string `json:"content"`
}

type editTaskPlaybookTool struct {
	st playbookStore
	tr playbookTranslator // 改手册后据此重新编译 fetch_plan（P1 编译层；nil=跳过）
}

func (t *editTaskPlaybookTool) Name() string { return "edit_task_playbook" }
func (t *editTaskPlaybookTool) Description() string {
	return "修改某个定时推送任务的情报手册（整体替换现有内容）：用自然语言描述这个任务要抓什么、关注哪些主题、偏好哪些来源。提交前先用 view_task_playbook 查看现有内容并合并；省略会丢掉现有内容。schedule_id 可先用 list_schedules 查询。"
}
func (t *editTaskPlaybookTool) Parameters() json.RawMessage {
	return json.RawMessage(editTaskPlaybookSchema)
}

// Execute（确认后执行）：空 schedule_id / 空 content 是确定性可自纠失败，回文案不触库；
// content 超上限截断后落库；UpsertSchedulePlaybook 归属未命中（ok=false）回自纠文案不上抛
// （与 enable_source 越权处理一致），基础设施错误上抛。
func (t *editTaskPlaybookTool) Execute(ctx context.Context, userID int64, args json.RawMessage) (string, error) {
	var a editTaskPlaybookArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "参数不是合法 JSON，请修正后重试", nil
	}
	schedID := strings.TrimSpace(a.ScheduleID)
	if schedID == "" {
		return "schedule_id 必须是非空字符串（可先用 list_schedules 查询）", nil
	}
	if strings.TrimSpace(a.Content) == "" {
		return "手册内容不能为空。请描述这个任务要抓什么、关注哪些主题、偏好哪些来源。", nil
	}
	content := capPlaybookContent(a.Content)
	ok, err := t.st.UpsertSchedulePlaybook(ctx, userID, schedID, content)
	if err != nil {
		return "", err
	}
	if !ok {
		return fmt.Sprintf("没找到你的定时任务（id=%s），可能 id 有误或已删除。用 list_schedules 查一下。", schedID), nil
	}
	// 手册已更新 → 据此重新编译抓取计划（P1 编译层，best-effort）。回执带上编译出的源数，
	// 让用户/模型看见手册确实转成了可执行的抓取计划；编译失败/零源则回执不提计划，静默降级。
	if n := compilePlaybookPlan(ctx, t.st, t.tr, userID, schedID, content); n > 0 {
		return fmt.Sprintf("已更新定时任务（id=%s）的情报手册，并据此编译出 %d 个抓取源的计划。", schedID, n), nil
	}
	return fmt.Sprintf("已更新定时任务（id=%s）的情报手册。", schedID), nil
}

// Summarize 展示截断后的实际生效内容预览（卡面列超上限文本而只落截断值是对用户撒谎）。
func (t *editTaskPlaybookTool) Summarize(args json.RawMessage) string {
	var a editTaskPlaybookArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return summarizeFallback("修改任务手册", args)
	}
	preview := truncateRunes(strings.TrimSpace(capPlaybookContent(a.Content)), playbookSummaryPreviewRunes)
	return fmt.Sprintf("修改定时任务手册（id=%s）：新内容「%s」", strings.TrimSpace(a.ScheduleID), preview)
}

package fetcher

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/YouToco/vane/server/types"
)

// dropReason 是一条内容**被丢弃的原因**，用于把「本轮全灭」的诊断信息一路带到用户面前。
//
// 为什么要给丢弃分类（2026-07-18 生产实证）：给账号加 Hacker News RSS 后，抓取路径全程
// 「成功」——last_fetched_at 正常刷新、fail_count=0、status=active——但该源入库内容数恒为 0。
// HN 的 description 全是 `<![CDATA[<a href="...">Comments</a>]]>`，30 条被 §12.3 裸 HTML 护栏
// 逐条跳过。真相只存在于 journalctl 的 30 行 WARN 里，用户侧看到的是一个健康的 active 信源，
// 却永远收不到任何东西，也没有任何提示。
//
// **护栏是对的，问题在于「源里本来就没有新东西」与「有 30 条、全被丢了」在用户侧完全不可
// 区分**——而区分它们所需的信息在丢弃的那一刻是有的，只是被扔掉了。这与 vane#77（点了
// 立即推送没有任何回音，实为空批次静默）是同一个缺陷家族：**正常和坏掉长得一模一样**。
//
// 分类的判据（决定哪些丢弃算「源坏了」）：
//   - **本类型只承载 A 类＝源不兼容/装配错误**：由「条目结构不符合提取假设」触发，
//     与时间无关、每轮 100% 复现、用户改 config 也修不好（只能改代码或换源）。
//   - **B 类＝正常过滤**（lookback 过期、categories 不匹配）**刻意不进本类型**：
//     它们由用户声明的选择策略触发，丢弃率随时间自然波动，语义上「本来就不想要」。
//
// 这个区分不是洁癖：生产有 5 个 feed 源走默认 7 天 lookback，任何一个博客只要 8 天不更新，
// 它的 RSS 照样返回 30 条旧条目、全被 lookback 正常滤掉、入库 0 条。若把 B 类也算进「全灭」，
// **每次抓取都会误告警**，10 轮后还会把一个完全健康的源自动停用。
//
// 实现上这个区分是**结构性保证**而非纪律要求：B 类过滤发生在 applyLookback / applyCategories
// 里、在映射之前就把条目摘掉了，映射函数根本看不见它们。全灭判定比较的是「映射函数的入参
// 与产出」，B 类天然不在分母里。
type dropReason string

const (
	dropNone dropReason = "" // 未丢弃。

	// dropNoIdentity：算不出 canonical_key。web 平台身份即 URL，故通常意味着条目没有 link。
	dropNoIdentity dropReason = "身份缺失"
	// dropNoKind：抓取器忘了给 Kind 赋值——装配 bug 级，finalize 刻意只校验不兜底。
	dropNoKind dropReason = "kind 缺失"
	// dropBareHTML：正文含裸 HTML，抽取未在指纹之前完成（契约 §12.3）。HN 事故本尊。
	dropBareHTML dropReason = "正文含裸 HTML"
	// dropEmptyResult：结果既无 URL 又无标题，无从构造内容（Exa 搜索的空结果）。
	dropEmptyResult dropReason = "结果为空"
)

// dropTally 累计一轮映射里各原因丢弃的条数，用于组装用户可读的诊断文案。
// 零值可用。
type dropTally struct {
	counts map[dropReason]int
	total  int
}

func (t *dropTally) add(r dropReason) {
	if r == dropNone {
		return
	}
	if t.counts == nil {
		t.counts = make(map[dropReason]int, 4)
	}
	t.counts[r]++
	t.total++
}

// summary 把丢弃计数排成稳定顺序的中文短语，如「正文含裸 HTML 27、身份缺失 3」。
// 按条数降序、同数按原因字典序——**排序是刻意的**：告警文案不该因 map 遍历顺序
// 每次长得不一样，那会让「同一个故障」看起来像好几个不同的故障。
func (t dropTally) summary() string {
	if t.total == 0 {
		return ""
	}
	type kv struct {
		r dropReason
		n int
	}
	kvs := make([]kv, 0, len(t.counts))
	for r, n := range t.counts {
		kvs = append(kvs, kv{r, n})
	}
	sort.Slice(kvs, func(i, j int) bool {
		if kvs[i].n != kvs[j].n {
			return kvs[i].n > kvs[j].n
		}
		return kvs[i].r < kvs[j].r
	})
	parts := make([]string, len(kvs))
	for i, e := range kvs {
		parts[i] = fmt.Sprintf("%s %d", e.r, e.n)
	}
	return strings.Join(parts, "、")
}

// allDroppedErr 是「反漂移防线」的通用形态：**收到了条目，却一条都没能留下**。
//
// 这不是安静的空轮，是源与解析器不兼容或上游结构漂移的确证——received 已经排除了
// 正常过滤（见 dropReason 的说明），所以 kept==0 只可能是 A 类原因造成的。
//
// 返回 error（而非只记日志）是刻意的，因为**收益链路已经全部接好**：
// 该错误从 Fetch 返回 → workflow 的 Fetch 活动 markFetchResult(false) → fail_count++
// → 连续 3 次触发 alertFetchFailures → 飞书告警卡，卡里「原因：」字段直接渲染
// AppError.Message。也就是说下面这句话会原样出现在用户的飞书卡片里。
// 本包已有同形先例：binding.go 的「反漂移防线 3」。
//
// received==0（源本轮真的没给条目）**绝不在此报错**——那是合法的空轮，
// 「无内容可推必须仍是正常终态」是红线。
//
// 文案粒度对齐 binding 防线 3：可以带条数、原因、source_id，
// **绝不把 feed 原文或上游响应体拼进来**——它会原样进飞书卡片。
// errAllDropped 是全灭防线的判定哨兵：probe 路径（probe.go）据此把「收到条目却全部
// 无法入库」重新措辞成准入拒绝话术（Message 里的 source_id=0 与 drop 摘要是给管理员
// 告警卡看的，不适合原样进入用户回复）。周期抓取路径不感知它。
var errAllDropped = errors.New("条目全部无法入库")

func allDroppedErr(src types.FetchTarget, received int, t dropTally) error {
	return types.NewAppError(types.CodeValidation,
		fmt.Sprintf("条目全部无法入库（%d 条：%s，source_id=%d）——该源的内容格式与解析器不兼容，"+
			"或上游结构已漂移；本源将持续零产出直到格式恢复或改用其它源",
			received, t.summary(), src.ID), errAllDropped)
}

// enrichAllFailedErr 是全灭的**瞬态**变体（对抗审查 A-F1）：条目全被丢弃，但这一轮
// 有正文补全尝试且失败——全灭的成因更可能是补全上游（Exa）临时故障，而非源本身格式
// 不兼容。故用 CodeFetchTimeout + Retryable=true（与 allDroppedErr 的确定性 CodeValidation
// 相对），让 probe 走「稍后再试」（translateFeedProbeErr 不翻译瞬态错误，交 agent 层
// 给固定话术）、让 §86 告警措辞说「补全上游暂时不可用」而非「格式不兼容」。
// **不带 errAllDropped 哨兵**，故 probe 的 errors.Is(errAllDropped) 分支不会误吃它。
func enrichAllFailedErr(src types.FetchTarget, failed int) error {
	ae := types.NewAppError(types.CodeFetchTimeout,
		fmt.Sprintf("条目需抓取全文才能入库，但本轮 %d 条全文补全全部失败（补全上游暂时不可用，source_id=%d）",
			failed, src.ID), nil)
	ae.Retryable = true
	return ae
}

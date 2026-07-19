// 试跑=准入的统一入口（功能清单 1.5「通用来源兜底解析」）。
//
// 此前「确认后先真调一次上游、全过才落库」只覆盖绑定能力（endpoint-binding-contract.md
// §2.2，agent 侧门禁 IsBindingBacked）；web/feed 与 web/contents 不试跑直接落库——
// 用户给一个解析不了的冷门 URL 时，add_source 照样回「已添加并订阅」，源却永远
// 零产出，要等 fail_count 涨满 10 轮才自动停用。「假装成功，用户误以为已订阅」
// 正是 1.5 要消除的痛点。
//
// 本文件把试跑推广到 URL 类 web 能力，并在「不是 feed」的失败上接兜底解析
// （resolve.go 嗅探 autodiscovery），让拒绝话术自带替代建议：
//   - 页面声明了 feed 地址 → 建议改用该地址订阅（web/feed）
//   - 没有 feed 声明     → 建议 web/contents 页面监控或 web/search 关键词订阅
// 只建议、不静默改道（m6 契约 §2.2；确认卡上是什么 URL 就订什么 URL）。
//
// 「不无限重试」在两层各有承载，本文件只负责第一层：
//   - 添加期：试跑单次、probeBudget 10s（agent 侧），失败即拒，不落任何行；
//   - 运行期：既有 fail_count 链（连续 3 次告警卡、10 次自动停用）对 web provider
//     早已接线（#86 全灭防线同链路），零新代码。
package fetcher

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/YouToco/vane/sourcecatalog"
	"github.com/YouToco/vane/types"
)

// Probe 试跑一个待添加的源（真调一次上游，计费调用照常记账 tool_calls）。
//
// 返回语义（调用方是 agent 的 add_source）：
//   - (report, nil)  试跑通过，report 进确认回执（首批统计，用户看得见）；
//   - (nil, nil)     该能力无试跑门（如 web/search——入参是关键词不是 URL，
//     没有「来源解析失败」一说；0 条命中已有运行期全灭防线兜底），直接落库；
//   - (nil, err)     试跑未通过，不落任何行。*ProbeRejection 的 Message 是给用户的
//     人话（含替代建议，红线 3 允许透出）；其余错误由调用方按错误码映射固定话术。
//
// 「哪些能力要试跑」是 fetcher 层知识，集中在这里而不散在 agent 层——
// 新增能力时漏接试跑，这里的 default 分支让它退回「不试跑」而不是把 add_source 打挂。
func (m *Multi) Probe(ctx context.Context, src types.Source) (*ProbeReport, error) {
	// 与 Fetch 同款 sourcecatalog 门禁（Unavailable veto，endpoint-binding 契约 §2.3a）：
	// sourcespec.Build 已挡过一遍，此处防御性复查零成本——Probe 是导出方法，
	// 不该假设所有调用方都先过了 Build。
	entry, ok := sourcecatalog.Lookup(src.Platform, src.Capability)
	if !ok {
		return nil, probeReject(fmt.Sprintf("未知信源能力 %q/%q，无法添加", src.Platform, src.Capability))
	}
	if !entry.Available() {
		return nil, probeReject(fmt.Sprintf("信源能力 %q/%q 当前不可用：%s", src.Platform, src.Capability, entry.Reason))
	}

	switch {
	case IsBindingBacked(src.Platform, src.Capability):
		return m.binding.Probe(ctx, src)
	case src.Platform == types.PlatformWeb && src.Capability == types.CapFeed:
		return m.probeFeed(ctx, src)
	case src.Platform == types.PlatformWeb && src.Capability == types.CapContents:
		return m.probeContents(ctx, src)
	default:
		return nil, nil
	}
}

// probeFeed 试跑 web/feed：跑完整抓取路径（含 lookback/categories/正文补全/全灭防线），
// 不是只验证「能 GET」——试跑通过必须意味着周期运行也能通过（试跑=准入的诚实边界；
// HN 那种「feed 合法但 30 条全被裸 HTML 护栏丢弃」的源必须在这里就被拒掉，
// 而不是 probe 绿、生产死）。正文补全在试跑轮的花费与正常轮同量级（封顶 10 条），
// 是准入判定正确性的必要成本。
//
// 试跑成功且 0 条是合法的（8 天没更新的博客，lookback 把旧条目全滤掉）：
// feed 本身有效，新文章发布后自然进入推送，不拒。
func (m *Multi) probeFeed(ctx context.Context, src types.Source) (*ProbeReport, error) {
	items, err := m.rss.FetchRSS(ctx, src)
	if err != nil {
		return nil, m.translateFeedProbeErr(ctx, src, err)
	}
	rep := &ProbeReport{Extracted: len(items)}
	for i := range items {
		if pt := items[i].PublishedAt; pt != nil && (rep.Newest == nil || pt.After(*rep.Newest)) {
			t := *pt
			rep.Newest = &t
		}
		if len(rep.SampleTitles) < 3 && strings.TrimSpace(items[i].Title) != "" {
			rep.SampleTitles = append(rep.SampleTitles, items[i].Title)
		}
	}
	return rep, nil
}

// translateFeedProbeErr 把 web/feed 的抓取失败翻译成准入话术（ProbeRejection，
// 可原样进用户面）。翻译只覆盖**确定性**失败——瞬态（超时/5xx/429）原样返回，
// 由 agent 层按错误码给「稍后再试」，用户重试即可，不该被拒绝话术误导成 URL 有问题。
func (m *Multi) translateFeedProbeErr(ctx context.Context, src types.Source, err error) error {
	// 核心场景：URL 能访问但不是 feed —— 兜底解析（1.5 本体）。
	// 再抓一次页面做 autodiscovery 嗅探：FetchRSS 失败时响应体已随错误路径丢弃，
	// 这次 GET 只发生在一次性的试跑上（周期抓取路径零嗅探开销），RSS 抓取不计费。
	if errors.Is(err, errNotFeed) {
		var links []string
		if u, perr := url.Parse(src.URL); perr == nil {
			if data, ferr := m.rss.fetchBody(ctx, src.URL); ferr == nil {
				links = sniffFeedLinks(data, u)
			}
		}
		if len(links) > 0 {
			return probeReject(fmt.Sprintf(
				"该地址返回的内容不是 RSS/Atom feed，但页面声明了 feed 地址：%s。建议把 url 换成该地址重新添加（web/feed）。",
				strings.Join(links, " 、 ")))
		}
		return probeReject(
			"该地址返回的内容不是 RSS/Atom feed，页面上也未发现 feed 声明。替代方案：" +
				"用 web/contents 监控该页面的内容变化（url 原样），或用 web/search 以关键词订阅相关主题。")
	}

	// feed 可解析但条目全部无法入库：添加后必然持续零产出、告警，拒。
	// allDroppedErr 的 Message 是给管理员告警卡的（含 source_id=0 与 drop 分类摘要），
	// 试跑回执重新措辞。
	if errors.Is(err, errAllDropped) {
		return probeReject(
			"feed 可以解析，但当前条目全部无法入库（内容格式与解析器不兼容），添加后将持续零产出。" +
				"若只需关注该站点更新，可提供站点页面地址改用 web/contents 监控内容变化。")
	}

	var ae *types.AppError
	if errors.As(err, &ae) {
		switch {
		case ae.Code == types.CodeValidation:
			// 非法 URL / 私网拦截 / 响应超限：Message 是本包自己拼的中文（无上游原文，
			// 红线 3 安全），带上它比「参数可能有误」的笼统话术更能让用户改对。
			return probeReject(ae.Message + "。请检查 URL 后重试。")
		case ae.Code == types.CodeFetchTimeout && !ae.Retryable:
			// 4xx（404/403…）：确定性失败，多半是 URL 打错或页面不公开。
			return probeReject(ae.Message + "。请检查 URL 是否正确、页面是否公开可访问。")
		}
	}
	return err
}

// probeContents 试跑 web/contents：真调一次 Exa /contents（$0.001，记账 tool_calls）。
// 与周期路径的一处刻意分歧：Fetch 把「Exa 成功但无正文」当合法空轮（返回 0 条不报错，
// 下轮再抓）；试跑语义下它必须拒——用户正在添加一个提取不到任何内容的监控源，
// 放行就是 1.5 要消除的「假装成功」。
func (m *Multi) probeContents(ctx context.Context, src types.Source) (*ProbeReport, error) {
	items, err := m.exaContents.Fetch(ctx, src)
	if err != nil {
		if errors.Is(err, errPageUnreachable) {
			return nil, probeReject(
				"无法抓取该页面（可能不存在、需要登录或阻止了抓取）。请检查 URL 是否正确、" +
					"页面是否公开可访问；确认无误可稍后重试。")
		}
		// 其余（缺 key/鉴权/限流/超时/响应解析失败）：原样返回，agent 层按错误码给固定话术。
		return nil, err
	}
	if len(items) == 0 {
		return nil, probeReject(
			"未能从该页面提取到正文（可能是空页、纯前端渲染或需要登录），不适合作为内容监控源。" +
				"替代方案：改用该站的 feed 地址（web/feed），或用 web/search 以关键词订阅相关主题。")
	}
	rep := &ProbeReport{Extracted: len(items)}
	if t := strings.TrimSpace(items[0].Title); t != "" {
		rep.SampleTitles = []string{t}
	}
	return rep, nil
}

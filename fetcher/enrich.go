package fetcher

import (
	"context"
	"log/slog"
	"strings"

	"github.com/mmcdole/gofeed"

	"github.com/YouToco/vane/types"
)

// ============================================================
// RSS 正文补全：跟着条目链接去读原文
// ============================================================
//
// 2026-07-18 生产实证：订阅 Hacker News RSS 后一条内容都收不到。查下来它的每条 item
// 只有 `<description><![CDATA[<a href="...">Comments</a>]]></description>`——**正文一个字
// 都没有**，只给了标题、原文链接和评论区链接。整批被 §12.3 裸 HTML 护栏丢弃。
//
// HN 不是孤例：Lobsters、Reddit、各种「每日摘要」都是这种**纯链接聚合器**。
// 而 vane 早就有「按 URL 取正文」的能力（web/contents 走 Exa /contents），只是从没接到
// feed 条目上——所以这是产品缺口，不是结构性限制。
//
// ---- 为什么补全必须在打分之前 ----
//
// 直觉上更省钱的做法是「先按标题打分、只给选中的 TopN 补正文」（30 次 → 8 次）。
// **这条路走不通**：scoreSystemPrompt 明确要求「正文信息过少时给低分（0-20），不要凭标题
// 想象正文可能写了什么」——那是 2026-07-15 的缺陷修复（delivery 48 只有 8 个话题标签、
// 零正文却拿了 85 分占掉推送位，逼得 cardgen 为它编出观点）。没正文的条目一律低分，
// 永远进不了 TopN，也就永远轮不到补全。鸡生蛋。
//
// 所以补全放在抓取阶段，成本靠下面两道闸门压，而不是靠"少补几条"。
//
// ---- 两道成本闸门 ----
//
//  1. **SeenChecker**（复用 TikHub 付费下钻那套）：按 canonical_key 全局查"已入库且正文
//     已补全"，命中的直接跳过。这是稳态成本的关键——HN 每半小时一轮，但每轮真正新上榜
//     的只有几条，其余都在库里。且它按全局身份查，跨源命中同一篇时只有第一个源付费。
//  2. **每轮封顶**：首轮或大量翻榜时也不会一次打出几十个付费调用。
//
// 未做（明确记下，不假装考虑过）：Exa /contents 的实际单价我不掌握，代码里只写了
// "按搜索+取正文计费"没有费率。封顶值取保守，等真实账单出来再调。

// enrichMaxPerRound 是单个信源单轮最多补全的条目数。
//
// 取 10 是保守值：HN 这类高翻榜源首轮会触顶（30 条只补 10 条），但配合 SeenChecker，
// 后续每轮只需补新上榜的几条，很快追平。宁可首轮追不满，也不要在费率未知时一次打出
// 几十个付费调用——首轮漏掉的条目不会丢失，它们仍在库里，下一轮 SeenChecker 认得出
// 「未补全」并继续补。
const enrichMaxPerRound = 10

// probeEnrichCap 是**试跑轮**的补全上限（对抗审查 A-F3/A-F4）：远小于 enrichMaxPerRound。
// probe 只需补够几条证明「这个链接型源补全管用、能产出内容」即可判定准入，无谓为未落库
// 的判定付满额详情费（首轮正式抓取会按 enrichMaxPerRound 正常补），也把 probeFeed 的耗时
// 压在 probeBudget 内（5 条 × ~3s + GET ≈ 17s < 25s）。取 5 而非更小：留冗余，避免恰好
// 头几条补全瞬态失败就误判全灭（虽然那也只是「稍后再试」的可重试拒绝，非永久拒）。
const probeEnrichCap = 5

// enrichMinRunes 一个常量同时承担两件事，**这是刻意的**：
//   - 触发线：正文短于它 ⇒ 判定为摘要/存根，需要跟着链接补全。
//   - 已补全线：传给 SeenChecker，正文达到它 ⇒ 视为已补全，不再付费。
//
// 拆成两个数会踩一个永久重复付费的陷阱：设触发线 80、已补全线 200，而某篇文章补回来
// 只有 150 字 —— 它跨过了触发线不再被判定为存根，却够不着已补全线，于是 SeenChecker
// **永远**认为它没补全。这类条目每轮都会重新付费，而且因为它看起来"正常"，
// 账单涨了都不知道是谁在涨。同一个数就没有中间地带。
//
// 取 80 而非更高：这条线的职责只是识别「摘要/存根」（"阅读全文…"、一句话导语、
// 纯链接聚合器的锚点），不是判断内容够不够好。定高了会为本来就可用的短资讯付费补全，
// 那是纯亏。定低了漏掉的存根下轮还有机会（内容不丢）。
const enrichMinRunes = 80

// enrichCacheHours 允许 Exa 返回多久以内的缓存。
// 补全只关心"这篇文章写了什么"，不关心"变没变"（那是 web/contents 页面监控的诉求），
// 所以吃缓存既更快也更省钱。24 小时对资讯类内容足够——文章发布后正文极少变。
const enrichCacheHours = 24

// pageTextFetcher 按 URL 取网页正文。生产实现是 *ExaContentsFetcher。
// 收窄成接口而非直接依赖具体类型：RSS 抓取器的单测全是纯 httptest，不该为了补全
// 被迫拖进一个 Exa 客户端。
type pageTextFetcher interface {
	pageResultsWithEffectGate(
		ctx context.Context,
		pageURL string,
		maxAgeHours int,
		src *types.Source,
		beforeEffect func(context.Context) error,
	) ([]exaContentsResult, bool, error)
}

// needsEnrichment 判断一条 feed 条目是否需要跟着链接去读原文。
//
// 两种情况都要补：
//   - 正文为空或过短：纯链接聚合器（HN/Lobsters/Reddit）的典型形态。
//   - 正文含裸 HTML：feed 塞的是未抽取的 HTML 片段，它本来就会被 §12.3 护栏丢弃；
//     补全后拿到的是 Exa 抽好的纯文本，等于把"必然丢弃"变成"可用内容"。
//
// 没有 link 的条目不补——没有可读的地址，补了也无从谈起。
func needsEnrichment(link, content string) bool {
	if strings.TrimSpace(link) == "" {
		return false
	}
	c := strings.TrimSpace(content)
	if len([]rune(c)) < enrichMinRunes {
		return true
	}
	return htmlTagRe.MatchString(c)
}

// enrichItems 为需要补全的条目取回原文正文，**就地改写** items 的正文字段。
//
// 全程 best-effort：任何一条补全失败只记日志、保留原样，让它继续走后面的护栏
// （补不到正文的链接型条目仍会被 §12.3 丢弃，与改造前一致——补全只增不减）。
// 一条补不到不该拖垮整批，更不该让整个信源被判成故障。
//
// 返回：
//   - skippedSeen：因库里已有其正文而**刻意跳过**补全的条目数。调用方必须把它从
//     「全灭」判定的分母里扣掉——见 FetchRSS 里的用法与下方说明。
//   - enrichFailed：**尝试补全但失败**（上游报错或取回空正文）的条目数。它区分
//     「源格式与解析器不兼容」（确定性）与「补全上游 Exa 瞬态故障」（可重试）——
//     链接型源在 Exa 宕机/全 429/key 缺失时会全体补全失败 → 全部保持裸 HTML → 全灭，
//     但这**不是源坏了**。调用方据此在全灭时报可重试错误而非确定性拒绝
//     （对抗审查 A-F1：否则「Exa 挂了」会被诬告成「该源格式不兼容、持续零产出」，
//     用户再也不会重试一个实际健康的源）。
//
// maxEnrich 是本轮补全条数上限：周期抓取传 enrichMaxPerRound（10），probe 传更小的
// probeEnrichCap（5）——试跑只需补够几条证明「补全管用、能产出内容」，无谓为一个未落库
// 的准入判定付满额详情费，也让 probe 在 probeBudget 内稳定完成（对抗审查 A-F3/A-F4）。
func (f *Fetcher) enrichItems(ctx context.Context, src types.Source, items []*gofeed.Item, maxEnrich int) (skippedSeen, enrichFailed int) {
	skippedSeen, enrichFailed, _ = f.enrichItemsWithEffectGate(ctx, src, items, maxEnrich, nil)
	return skippedSeen, enrichFailed
}

func (f *Fetcher) enrichItemsWithEffectGate(
	ctx context.Context,
	src types.Source,
	items []*gofeed.Item,
	maxEnrich int,
	beforeEffect func(context.Context) error,
) (skippedSeen, enrichFailed int, err error) {
	if f.enricher == nil || len(items) == 0 {
		return 0, 0, nil // 未装配补全能力（测试/灰度）：退化为不补，行为与改造前一致。
	}

	// 先挑出候选，再一次性问 SeenChecker——逐条查会把一次批量查询放大成 N 次往返。
	type cand struct {
		it  *gofeed.Item
		key string
	}
	var cands []cand
	for _, it := range items {
		if it == nil || !needsEnrichment(it.Link, itemContent(it)) {
			continue
		}
		// 身份口径必须与 finalize 落库时**完全一致**，否则闸门查的是一个永远查不中的键，
		// SeenChecker 恒空 → 每轮都重新付费补全同样的内容。
		key := CanonicalKey(src, types.ContentItem{URL: it.Link})
		if key == "" {
			continue
		}
		cands = append(cands, cand{it: it, key: key})
	}
	if len(cands) == 0 {
		return 0, 0, nil
	}

	// 闸门①：已入库且正文已补全的直接跳过（跨源命中同一篇时只有第一个源付费）。
	skip := map[string]struct{}{}
	if f.seen != nil {
		keys := make([]string, len(cands))
		for i, c := range cands {
			keys[i] = c.key
		}
		got, err := f.seen.EnrichedCanonicalKeys(ctx, keys, enrichMinRunes)
		if err != nil {
			// 查不到就**全部跳过补全**，不是"全部补全"。闸门失效时宁可这轮不补
			// （下轮再补，内容不丢），也不要在数据库抖动时打出一批付费调用。
			slog.Warn("fetcher: 补全闸门查询失败，本轮跳过正文补全",
				"source_id", src.ID, "candidates", len(cands), "err", err)
			return 0, 0, nil
		}
		skip = got
	}

	done := 0
	for _, c := range cands {
		if done >= maxEnrich {
			slog.Info("fetcher: 正文补全达本轮上限，其余条目留待下轮",
				"source_id", src.ID, "enriched", done, "remaining", len(cands)-done)
			break
		}
		if _, ok := skip[c.key]; ok {
			skippedSeen++
			continue // 闸门①命中：库里已有补全正文，不重复付费。
		}
		results, _, fetchErr := f.enricher.pageResultsWithEffectGate(
			ctx, c.it.Link, enrichCacheHours, &src, beforeEffect)
		if isEffectGateError(fetchErr) {
			return skippedSeen, enrichFailed, fetchErr
		}
		if fetchErr != nil {
			slog.Warn("fetcher: 单条正文补全失败，保留原样",
				"source_id", src.ID, "url", c.it.Link, "err", fetchErr)
			enrichFailed++ // 尝试了但上游报错（对抗审查 A-F1：全灭时据此判瞬态 vs 确定性）。
			continue
		}
		text := firstNonEmptyText(results)
		if strings.TrimSpace(text) == "" {
			slog.Warn("fetcher: 正文补全取回空正文，保留原样",
				"source_id", src.ID, "url", c.it.Link)
			enrichFailed++ // 尝试了但取回空正文，同样计入补全失败。
			continue
		}
		// 与 web/contents 用同一套净化：Exa 抽出的正文里 `<` 是内容（比较符/代码）
		// 而非未抽取的 HTML，不净化会被 §12.3 护栏误伤——刚补回来的正文又被丢掉。
		c.it.Content = sanitizeContentsText(text)
		c.it.Description = "" // 正文已由 Content 承载，避免 itemContent 回退到旧的链接片段。
		done++
	}
	if done > 0 || skippedSeen > 0 || enrichFailed > 0 {
		slog.Info("fetcher: RSS 正文补全完成",
			"source_id", src.ID, "enriched", done, "candidates", len(cands),
			"skipped_seen", skippedSeen, "failed", enrichFailed)
	}
	return skippedSeen, enrichFailed, nil
}

// firstNonEmptyText 取第一条有正文的结果（与 mapExaContents 的挑法一致）。
func firstNonEmptyText(results []exaContentsResult) string {
	for i := range results {
		if strings.TrimSpace(results[i].Text) != "" {
			return results[i].Text
		}
	}
	return ""
}

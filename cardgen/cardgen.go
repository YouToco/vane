// Package cardgen 用 DeepSeek 为单条内容生成"标题 + 一句话摘要 +
// 为什么与你有关"的解读，再包成飞书交互卡片 JSON。
//
// M3 设计取舍：原文链接由本包**确定性拼接**，不依赖模型输出里带上它。
// 模型经常漏链接或把链接写错，而链接是推送卡片能不能点开原文的命门，
// 所以把"生成解读"交给模型、把"挂链接"留给代码，各干各擅长的。
package cardgen

import (
	"context"
	"strings"

	"github.com/YouToco/vane/feishu"
	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/types"
)

// maxContentRunes 截断喂给模型的正文：生成解读只需主旨，全文徒增 token。
const maxContentRunes = 800

// cardSystemPrompt 约束模型产出可直接入卡的 Markdown 片段。
// 明确禁止代码块包裹：模型爱把 markdown 用 ``` 裹起来，那样在飞书卡片里
// 会渲染成一坨代码而非富文本。
const cardSystemPrompt = "你是资讯解读助手。为给定内容生成简洁的中文推送解读，" +
	"包含三部分：一个吸引人的加粗标题、一句话摘要、以及一句解释为什么与该用户有关。" +
	"直接输出 Markdown 文本，控制在 120 字以内。不要用代码块（```）包裹，不要输出多余寒暄。"

// CardGen 持有 LLM 客户端与记账器。契约 B4 固定这两个字段。
type CardGen struct {
	cli *llm.Client
	rec *llm.Recorder
}

// New 构造 CardGen，依赖由 cmd/server 装配时注入。
func New(cli *llm.Client, rec *llm.Recorder) *CardGen {
	return &CardGen{cli: cli, rec: rec}
}

// Generate 为一条已打分内容生成解读卡片，返回飞书卡片 JSON 字符串。
// LLM 失败向上抛给 Temporal 重试；成功但正文为空时用标题兜底，
// 保证卡片始终有可读内容且带原文链接。
func (cg *CardGen) Generate(ctx context.Context, userID int64, item types.ScoredItem, traceID string) (string, error) {
	req := llm.Request{
		System:      cardSystemPrompt,
		User:        buildCardUser(item.Item),
		Temperature: f32ptr(0.7), // 解读文案要一点多样性，温度略高于打分
		MaxTokens:   iptr(400),
		// 关思维链：模板化摘要不需要 CoT；400 预算下 reasoning 偶发吃满会导致
		// content 空 → 卡片只剩标题+链接兜底（2026-07-14 生产 1/15 次命中）。
		DisableThinking: true,
	}

	meta := llm.CallMeta{
		TraceID:  traceID,
		SpanName: "cardgen",
		UserID:   &userID,
		RefType:  types.RefTypeContentItem,
	}
	if item.Item.ID != 0 {
		id := item.Item.ID
		meta.RefID = &id
	}

	resp, err := llm.Do(ctx, cg.cli, cg.rec, meta, req)
	if err != nil {
		return "", err
	}

	body := strings.TrimSpace(resp.Content)
	if body == "" {
		// 模型返回空内容也不能让卡片开天窗：用标题兜底。
		body = fallbackBody(item.Item)
	}

	return feishu.BuildReplyCard(buildMarkdown(body, item.Item)), nil
}

// buildMarkdown 把模型解读拼上确定性的原文链接行。
// URL 缺失时不硬塞空链接（飞书对空 href 渲染异常），只保留正文。
func buildMarkdown(body string, item types.ContentItem) string {
	var b strings.Builder
	b.WriteString(body)
	if strings.TrimSpace(item.URL) != "" {
		b.WriteString("\n\n[阅读原文](")
		b.WriteString(item.URL)
		b.WriteString(")")
	}
	return b.String()
}

// fallbackBody 在模型无输出时给出最小可读正文。
func fallbackBody(item types.ContentItem) string {
	title := strings.TrimSpace(item.Title)
	if title == "" {
		title = "新内容"
	}
	return "**" + title + "**"
}

// buildCardUser 拼装生成解读用的 user prompt。
func buildCardUser(item types.ContentItem) string {
	var b strings.Builder
	b.WriteString("标题：")
	b.WriteString(item.Title)
	b.WriteString("\n正文：")
	b.WriteString(truncateRunes(item.Content, maxContentRunes))
	return b.String()
}

// truncateRunes 按 rune 截断，避免切碎多字节字符。
func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// f32ptr / iptr：llm.Request 用指针区分"未设置"。
func f32ptr(v float32) *float32 { return &v }
func iptr(v int) *int           { return &v }

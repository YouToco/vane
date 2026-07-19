// Package profilehint 把用户画像渲染成单行提示句，并按 traceID 做 per-trace
// 快照缓存，供 scorer 与 cardgen 在同一 pipeline 内共享同一画像视图
// （画像不进 Temporal payload，Activities 签名因此不动，见 M5 契约 §4）。
// 依赖边界：只依赖 types 实体与 GetProfile 窄接口，不 import store。
package profilehint

import (
	"context"
	"strings"

	"github.com/YouToco/vane/types"
)

// Store 是画像读取的窄接口，生产实现为 *store.Store。
type Store interface {
	GetProfile(ctx context.Context, userID int64) (*types.Profile, error)
}

const (
	// maxTags 展示层截 10 是刻意分层（打分信号聚焦），非数据截断：库内上限 12（契约 §2）。
	maxTags = 10
	// summaryMaxRunes 是 summary 前段的截断预算；负面句保尾后另计，不占此预算。
	summaryMaxRunes = 300
	// hintMaxRunes 整串护栏：取 560 而非更紧，是为负面句保尾留余量（审查 F1）。
	hintMaxRunes = 560
	// maxEntries per-trace 缓存 FIFO 容量。
	maxEntries = 16
)

// negPrefix 是演化 prompt 规则 2 锁定的负面清单固定句式前缀（「不感兴趣：主题A、主题B。」，
// 恒在 summary 末尾）。打分器压低负偏好主题依赖这句完整存活于提示中——保尾逻辑全部围绕它。
const negPrefix = "不感兴趣："

// ellipsis 截断标记。截断预算均不含它：前段满预算后追加，让模型知道前文有省略。
const ellipsis = "……"

// NegPrefix / EllipsisRune 导出给 Gate 探针 ⑤ 构造 SQL 判据（store.GetNegTailStat）。
//
// 为什么必须从这里导出、而不是让探针侧自己写字面量：理由与 NegTail 的导出完全相同
// ——判据里的字面量与保尾逻辑一旦分处两地，就会各自漂移，而漂移的探针不会报错，
// 只会安静地测错东西。这两个常量是保尾形状的定义者，探针只能引用不能重述。
const (
	NegPrefix = negPrefix
	// EllipsisRune 是截断标记的**单个**字符。ellipsis 本身是它重复两次（中文省略号
	// 惯例），而探针要的是正则字符类 [^…] 里的排除项，用单字符才是正确的语义。
	EllipsisRune = "…"
)

// Build 纯函数：把画像渲染成单行提示。空字段跳过，"；"连接，全空返回 ""。
// 格式：行业：{industry}；职业：{occupation}；关注标签：{tags[:10] 顿号连}；摘要：{summary'}
//
// 负面清单保尾（审查 F1，慢通道生命线）：summary 单行化后，末尾的「不感兴趣：…」句
// 先摘出原样保留，剩余前段截 summaryMaxRunes 再拼回；整串最终截 hintMaxRunes 时同样
// 保尾优先——宁可多剪前段，绝不剪负面句。
//
// 单行是硬约束：多行会模糊与 user prompt 中后续定界块（【待评估内容】等）的边界。
func Build(p *types.Profile) string {
	if p == nil {
		return ""
	}
	var parts []string
	if v := singleLine(p.Industry); v != "" {
		parts = append(parts, "行业："+v)
	}
	if v := singleLine(p.Occupation); v != "" {
		parts = append(parts, "职业："+v)
	}
	if tags := cleanTags(p.Tags); len(tags) > 0 {
		parts = append(parts, "关注标签："+strings.Join(tags, "、"))
	}
	if s := buildSummary(singleLine(p.Summary)); s != "" {
		parts = append(parts, "摘要："+s)
	}
	if len(parts) == 0 {
		return ""
	}
	return capHint(strings.Join(parts, "；"))
}

// NegTail 返回本画像**预期原样出现在 Build 产物里**的负面句（无则空串）。
//
// 为什么导出：Gate 探针 ⑤（契约 §16.5，F1 的线上验证）要拿"期望的负面句"去比对
// llm_calls.user_prompt 的画像行。让探针自己 reimplement 一遍 singleLine+splitNegTail
// 必然与这里漂移，而漂移的后果是探针**假绿**——它会去找一个谁都不会写出的串，
// 永远比对不上却报"未命中即未截断"。故期望值必须由保尾逻辑的所有者亲自给出。
//
// 正确性依据（保尾三段路径，任一段都不剪 neg）：
//   - buildSummary:74  truncateEllipsis(front, …) + neg —— 只剪 front
//   - capHint:93       front[:headBudget] + ellipsis + neg —— 只剪 front
//   - Build 的 parts 顺序里"摘要"恒在末位，故 capHint 的 LastIndex 找到的就是本段
//
// 故本函数的返回值必然是 Build(p) 的后缀（当 Build(p) 非空时）。
func NegTail(p *types.Profile) string {
	if p == nil {
		return ""
	}
	_, neg := splitNegTail(singleLine(p.Summary))
	return neg
}

// buildSummary 对单行化后的 summary 做"前段截断 + 负面句保尾"。
func buildSummary(s string) string {
	front, neg := splitNegTail(s)
	if neg == "" {
		return truncateEllipsis(s, summaryMaxRunes)
	}
	return truncateEllipsis(front, summaryMaxRunes) + neg
}

// capHint 整串护栏。有负面句时保尾：前段预算 = hintMaxRunes - 负面句 - 省略号，
// 结果恰好压在 hintMaxRunes 内；极端下（负面句自身逼近护栏）前段清零、负面句仍完整——
// 此时允许略超护栏，这是 F1 的优先级排序（不得剪负面句 > 长度上限）。
func capHint(hint string) string {
	if runeLen(hint) <= hintMaxRunes {
		return hint
	}
	front, neg := splitNegTail(hint)
	if neg == "" {
		// 无负面句：直接截到护栏内（预算含省略号，保证结果不超 hintMaxRunes）。
		return truncateEllipsis(hint, hintMaxRunes-runeLen(ellipsis))
	}
	headBudget := hintMaxRunes - runeLen(neg) - runeLen(ellipsis)
	if headBudget < 0 {
		headBudget = 0
	}
	return string([]rune(front)[:headBudget]) + ellipsis + neg
}

// splitNegTail 从字符串中摘出末尾负面句：取 negPrefix 最后一次出现到串尾。
// 用 LastIndex：负面句自身内容里若再出现该前缀，仍以最靠后的为句首，尾段只会更短不丢。
func splitNegTail(s string) (front, neg string) {
	i := strings.LastIndex(s, negPrefix)
	if i < 0 {
		return s, ""
	}
	return s[:i], s[i:]
}

// cleanTags 单行化每个标签、跳过空标签，取前 maxTags 个。
func cleanTags(tags []string) []string {
	out := make([]string, 0, maxTags)
	for _, t := range tags {
		t = singleLine(t)
		if t == "" {
			continue
		}
		out = append(out, t)
		if len(out) == maxTags {
			break
		}
	}
	return out
}

// singleLine 把任意空白串（含换行）折叠为单个空格并去除首尾空白，保证单行硬约束。
func singleLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// truncateEllipsis 按 rune 截断到 n 并追加省略号；未截断时原样返回。
func truncateEllipsis(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + ellipsis
}

// runeLen 按 rune 计长（截断预算全部以 rune 计，避免切碎多字节字符）。
func runeLen(s string) int {
	return len([]rune(s))
}

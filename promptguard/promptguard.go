// Package promptguard 提供把不可信文本嵌入 LLM 提示词前的统一处理（M5 契约 §14）。
//
// 存在的理由：打分、画像演化、深度解读、追问上下文四处都要把外部抓取内容或
// 用户输入包进【…】/[…] 定界块，各包私写消毒逻辑迟早漂移——漏一处就等于漏掉
// 整条防线。定界前缀清单是全系统唯一事实来源，新增定界块必须同步登记到这里。
package promptguard

import (
	"strings"
	"unicode"
)

// systemDelimiterPrefixes 是本系统全部定界块的起始前缀（结束符以对应前缀开头，
// 替换前缀即一并失效）。新增定界块时必须在此登记，否则该块的终结符可被外部文本伪造。
// 一并作为 Sanitize 的替换表：起始括号换成全角龟甲括号「〔」。
var systemDelimiterPrefixes = []string{
	"【待评估内容",
	"【近期不感兴趣",
	"【反馈列表",
	"【内容",
	"[追问上下文",
	"[卡片回调",
	"[用户画像",
}

// delimiterSanitizer 由前缀清单构造的单遍替换器（见 Sanitize）。
var delimiterSanitizer = newSanitizer()

func newSanitizer() *strings.Replacer {
	pairs := make([]string, 0, len(systemDelimiterPrefixes)*2)
	for _, p := range systemDelimiterPrefixes {
		rest := strings.TrimPrefix(strings.TrimPrefix(p, "【"), "[")
		pairs = append(pairs, p, "〔"+rest)
	}
	return strings.NewReplacer(pairs...)
}

// Sanitize 消毒定界符（审查 F9）：把定界前缀的起始括号换成全角龟甲括号「〔」，
// 使其失去定界符效力。
//
// 攻击面：外部文本自带一个终结符（如「[追问上下文结束]」），使其后的注入文字
// 看起来位于定界块之外——即伪装成系统或用户的话而非块内数据；system prompt
// 只教了模型不服从"块内"的指令，对块外无防备。
//
// 换括号而非删除：删除会让紧邻的原文括号与替换产物重新拼出定界前缀
//（「【【内容」删首匹配后变回「【内容」）；换括号后所有产物都以「〔」开头，
// 任何拼接都无法重构出「【xxx」/「[xxx」形态。单遍替换（NewReplacer）而非
// 逐前缀 ReplaceAll：不重复扫描，也不会让前一次替换的产物参与后一次匹配。
func Sanitize(s string) string {
	return delimiterSanitizer.Replace(s)
}

// TruncateRunes 按 rune 截断，避免切碎多字节字符（2026-07-14 事故：字节截断
// 切裂中文 UTF-8 导致 Postgres 22021 拒写）。
func TruncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// StripInvisible 剥除对人不可见、对模型是 token 的字符（§12.2）：
// 零宽（U+200B-200D）、BOM（U+FEFF）、双向控制符（U+202A-202E, U+2066-2069）、
// Unicode Tags 块（U+E0000-E007F），以及其余 Cf 类。
func StripInvisible(s string) string {
	return strings.Map(func(r rune) rune {
		if isInvisible(r) {
			return -1
		}
		return r
	}, s)
}

func isInvisible(r rune) bool {
	switch {
	case r >= 0x200B && r <= 0x200D:
		return true
	case r == 0xFEFF:
		return true
	case r >= 0x202A && r <= 0x202E:
		return true
	case r >= 0x2066 && r <= 0x2069:
		return true
	case r >= 0xE0000 && r <= 0xE007F:
		return true
	case unicode.Is(unicode.Cf, r):
		return true
	}
	return false
}

// SingleLine 把任意空白串（含换行）折叠为单个空格并去除首尾空白。
// 用于必须占一行的注入位（画像提示行、定界块内的标题等）：多行会模糊
// 与定界块边界的视觉区分。
func SingleLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

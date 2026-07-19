package llm

// 模型上下文窗口声明（2026-07-18）。与 pricing.go 同一模式：模型属性集中在
// llm 包做单一事实来源，调用方按模型名查，不在各处硬编码数字。
//
// 存在的理由（Boss 质询「6000 字符对 1M 上下文太少」引出）：凡是「一次能塞多少
// 内容进上下文」的上限——端点结果内联、未来的历史裁剪/压缩——都该从窗口**推导**，
// 而不是写死。写死的常量必然随模型升级而失真：本项目的 6000 rune 是 64K 窗口
// 时代拍的，模型换到 1M 后没人记得改，直到用户在生产里撞见截断才发现差了一个
// 数量级。业界同款做法见 OpenClaw（按窗口分档 16k/32k/64k chars 且 ≤ 窗口 30%）。
//
// 数据来源：DeepSeek 官方文档，2026-07-18 查证（v4 系列 1M 上下文 / 384K 输出）。

// ContextWindowTokens 返回模型的上下文窗口（token 数）。
//
// 未知模型返回**保守下限**而非乐观值：与 CostUSD「未知按最贵档」同一取舍方向
// ——两处都是「猜错的那一侧不能伤人」。低估窗口只会让内联上限偏小（内容仍可经
// 句柄取回，见端点契约 §3.5）；高估则直接把请求打爆，模型端表现为整轮失败。
func ContextWindowTokens(model string) int {
	if n, ok := modelContextWindows[model]; ok {
		return n
	}
	return fallbackContextWindowTokens
}

// fallbackContextWindowTokens 未登记模型的假定窗口。取 64K：主流模型的普遍下限，
// 新模型登记进上表前不会让派生上限失控。
const fallbackContextWindowTokens = 64_000

var modelContextWindows = map[string]int{
	"deepseek-v4-flash": 1_000_000,
	"deepseek-v4-pro":   1_000_000,
}

// ────────── 由窗口推导「单次可内联多少字符」──────────

// 中文/JSON 混合内容的字符-token 比。OpenClaw 用 4（英文口径），本项目的工具
// 结果是中文社媒 JSON——中文约 1 char/token、ASCII 键名约 4 char/token，混合后
// 取 2 是偏保守的一侧（同样的字符数会被算成更多 token → 派生上限更小）。
const charsPerToken = 2

// 窗口占比上界（OpenClaw 同款 30%）：单条工具结果无论如何不得吃掉超过窗口三成，
// 否则一次调用就挤掉了对话历史与后续推理空间。
const maxWindowShare = 0.30

// InlineLimits 是从上下文窗口派生的一组内联预算（字符数）。
type InlineLimits struct {
	// PerCall 单次工具结果内联上限。
	PerCall int
	// MsgBudget 单条消息内全部工具结果的累计内联预算（= 3×PerCall）：单次上限管
	// 不住「一条消息调 N 次」的总量，更管不住 FC 多轮把历史重发 M 遍的成本乘数。
	MsgBudget int
	// MinPerCall 预算耗尽后每次仍保证的最小内联量（= PerCall/10，下限 2000）：
	// 给 0 会让模型看不见任何内容而瞎猜，而它本可以读结构摘要 + 按需句柄取回。
	MinPerCall int
}

// DeriveInlineLimits 按上下文窗口派生内联预算（OpenClaw 分档 + 窗口占比封顶）。
//
// 分档而非线性缩放，是因为「够读懂一屏结构化数据」有绝对下界（16k 字符量级），
// 而超过某个量级后再加也不改变模型能不能回答问题——只是更贵。
func DeriveInlineLimits(contextTokens int) InlineLimits {
	perCall := 16_000
	switch {
	case contextTokens >= 1_000_000:
		perCall = 100_000
	case contextTokens >= 500_000:
		perCall = 64_000
	case contextTokens >= 200_000:
		perCall = 64_000
	case contextTokens >= 100_000:
		perCall = 32_000
	}
	// 窗口占比封顶：小窗口模型即便命中高档也不得超三成。
	if cap := int(float64(contextTokens) * maxWindowShare * charsPerToken); perCall > cap {
		perCall = cap
	}
	minPerCall := perCall / 10
	if minPerCall < 2_000 {
		minPerCall = 2_000
	}
	if minPerCall > perCall {
		minPerCall = perCall
	}
	return InlineLimits{PerCall: perCall, MsgBudget: perCall * 3, MinPerCall: minPerCall}
}

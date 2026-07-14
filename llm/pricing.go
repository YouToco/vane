package llm

// DeepSeek V4 系列定价（USD / 1M tokens）。
// 来源：DeepSeek 官方定价页，2026-07-14 查证（M2 事实基准，M4 补 v4-pro）。
// 注意 deepseek-chat / deepseek-reasoner 是旧别名（2026-07-24 废弃）。
//
// M4 起同进程双模型并存：pipeline 打分/出卡用便宜档 v4-flash，agent loop 用
// v4-pro——记账必须按实际模型分价，否则 agent 成本被低估 3 倍以上。
type modelPrice struct {
	cacheHitPer1M   float64 // 前缀缓存命中的输入单价
	cacheMissPer1M  float64 // 缓存未命中的输入单价
	completionPer1M float64 // 输出单价
}

var modelPrices = map[string]modelPrice{
	"deepseek-v4-flash": {cacheHitPer1M: 0.0028, cacheMissPer1M: 0.14, completionPer1M: 0.28},
	"deepseek-v4-pro":   {cacheHitPer1M: 0.003625, cacheMissPer1M: 0.435, completionPer1M: 0.87},
}

// CostUSD 按模型三段单价计算单次调用成本。
// 未知模型按最贵档（v4-pro）计价：与 do.go 缓存未报告时的原则一致——
// 宁可略高估也不低估成本，成本监控是自用 MVP 的红线之一。
// 写成纯函数：不依赖 Client 状态，便于单测与后续多模型扩展时替换单价来源。
func CostUSD(model string, cacheHitTokens, cacheMissTokens, completionTokens int) float64 {
	p, ok := modelPrices[model]
	if !ok {
		p = modelPrices["deepseek-v4-pro"]
	}
	return float64(cacheHitTokens)/1e6*p.cacheHitPer1M +
		float64(cacheMissTokens)/1e6*p.cacheMissPer1M +
		float64(completionTokens)/1e6*p.completionPer1M
}

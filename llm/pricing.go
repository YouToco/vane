package llm

// DeepSeek deepseek-v4-flash 定价常量（USD / 1M tokens）。
// 来源：DeepSeek 官方定价页，2026-07-14 查证（M2 事实基准）。
// 注意 deepseek-chat / deepseek-reasoner 是旧别名（2026-07-24 废弃），
// 定价与默认模型一律以 deepseek-v4-flash 为准。
const (
	// PriceCacheHitUSDPer1M 前缀缓存命中的输入单价。
	PriceCacheHitUSDPer1M = 0.0028
	// PriceCacheMissUSDPer1M 缓存未命中的输入单价。
	PriceCacheMissUSDPer1M = 0.14
	// PriceCompletionUSDPer1M 输出单价。
	PriceCompletionUSDPer1M = 0.28
)

// CostUSD 按三段单价计算单次调用成本。
// 写成纯函数：不依赖 Client 状态，便于单测与后续多模型扩展时替换单价来源。
func CostUSD(cacheHitTokens, cacheMissTokens, completionTokens int) float64 {
	return float64(cacheHitTokens)/1e6*PriceCacheHitUSDPer1M +
		float64(cacheMissTokens)/1e6*PriceCacheMissUSDPer1M +
		float64(completionTokens)/1e6*PriceCompletionUSDPer1M
}

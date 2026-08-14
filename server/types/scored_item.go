package types

// ScoredItem 是打分后的内容条目：原始内容 + 相关分（0-100）。
//
// 为什么放在 types 而非 workflow 包：规格 B4 原意是把 ScoredItem 落在
// workflow 包，但 M3 里 scorer / selector / cardgen 三个 pipeline 包都要引用它，
// 而 workflow 包由 temporal agent 后建。若让三方各自本地定义，跨包传递时
// 就是三个不兼容的类型；统一沉到 types（只依赖标准库、无引用环）能让所有
// pipeline 包共享同一类型。temporal agent 组装 workflow 时应直接引用
// types.ScoredItem，不要在 workflow 包里重复声明。
type ScoredItem struct {
	Item  ContentItem `json:"item"`
	Score float64     `json:"score"` // 相关分，值域 0-100
}

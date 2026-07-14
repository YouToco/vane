// Package sourcespec 是 api 与 agent 工具共用的信源构造层（M4 契约 §6）：
// 把用户输入（HTTP 请求体或模型产出的工具参数）校验并构造成待 upsert 的
// types.Source。逻辑自 api/subscriptions.go 的 buildSource 原样迁移——
// 两个入口共用同一份校验/幂等键规则，避免 agent 加源与 API 加源语义漂移。
package sourcespec

import (
	"encoding/json"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/YouToco/vane/types"
)

// maxSourceParamRunes 是 query/keyword/title 的长度上限（字符数）。
// 无上限时超长输入会生成超长 sources.url/title（仅受 8KB 请求体约束）。
const maxSourceParamRunes = 256

// Spec 是信源构造入参，字段与原 api.addSubscriptionReq 一致。
// Type 决定必填字段：rss（默认）→ URL；exa → Query；tikhub_xhs → Keyword。
// Title 可选（缺省按类型生成展示名）；Category 是 exa 可选的结果类别
// （如 "news"），其余类型忽略。
type Spec struct {
	Type, URL, Query, Keyword, Title, Category string
}

// Build 校验并构造待 upsert 的信源；校验失败返回给用户的中文错误文案（空串=成功）。
//
// 关键设计：UpsertSource 以 sources.url 为幂等键，而 exa/tikhub 源没有天然 URL，
// 这里用确定性合成键（exa://search?q=... / tikhub://xhs/search?keyword=...）占位——
// 同一搜索词重复添加会命中同一信源，不产生重复行；真实请求参数放 config JSONB，
// 由对应 fetcher 解析。
func Build(spec Spec) (*types.Source, string) {
	// 归一化：去首尾空白再校验。否则全空白 query 穿透校验建出永久失败的源，
	// "AI" 与 "AI " 生成两个幂等键、产生重复信源双倍烧配额。
	spec.Query = strings.TrimSpace(spec.Query)
	spec.Keyword = strings.TrimSpace(spec.Keyword)
	spec.Title = strings.TrimSpace(spec.Title)
	for name, v := range map[string]string{"query": spec.Query, "keyword": spec.Keyword, "title": spec.Title} {
		if utf8.RuneCountInString(v) > maxSourceParamRunes {
			return nil, name + " 过长（上限 256 字符）"
		}
	}

	switch types.SourceType(spec.Type) {
	case types.SourceTypeRSS, "": // 缺省向后兼容为 rss
		// 只做结构校验（scheme 合法）；SSRF/私网拦截在抓取时由 fetcher 统一兜底，
		// 不在此重复一套 DNS 解析——加订阅与抓取是两个时点，抓取侧才是权威防线。
		u, err := url.Parse(spec.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, "url 必须是合法的 http/https 地址"
		}
		return &types.Source{
			Type:   types.SourceTypeRSS,
			URL:    spec.URL,
			Title:  spec.Title,
			Status: types.SourceStatusActive,
		}, ""

	case types.SourceTypeExa:
		if spec.Query == "" {
			return nil, "exa 信源必须提供 query（搜索词）"
		}
		cfgMap := map[string]string{"query": spec.Query}
		if spec.Category != "" {
			cfgMap["category"] = spec.Category
		}
		cfg, err := json.Marshal(cfgMap)
		if err != nil {
			return nil, "构造信源配置失败"
		}
		title := spec.Title
		if title == "" {
			title = "Exa: " + spec.Query
		}
		// category 参与幂等键：它改变抓取语义（news 与不限类别是两个信源），
		// 不入键会让同 query 不同 category 撞同一行、config 被静默覆盖。
		// 空 category 不追加，兼容已建的无 category 源行。
		syntheticURL := "exa://search?q=" + url.QueryEscape(spec.Query)
		if spec.Category != "" {
			syntheticURL += "&category=" + url.QueryEscape(spec.Category)
		}
		return &types.Source{
			Type:   types.SourceTypeExa,
			URL:    syntheticURL,
			Title:  title,
			Config: cfg,
			Status: types.SourceStatusActive,
		}, ""

	case types.SourceTypeTikHubXHS:
		if spec.Keyword == "" {
			return nil, "tikhub_xhs 信源必须提供 keyword（小红书搜索关键词）"
		}
		cfg, err := json.Marshal(map[string]string{"keyword": spec.Keyword})
		if err != nil {
			return nil, "构造信源配置失败"
		}
		title := spec.Title
		if title == "" {
			title = "小红书: " + spec.Keyword
		}
		return &types.Source{
			Type:   types.SourceTypeTikHubXHS,
			URL:    "tikhub://xhs/search?keyword=" + url.QueryEscape(spec.Keyword),
			Title:  title,
			Config: cfg,
			Status: types.SourceStatusActive,
		}, ""

	default:
		return nil, "type 仅支持 rss / exa / tikhub_xhs"
	}
}

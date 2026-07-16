// Package sourcespec 是 api 与 agent 工具共用的信源构造层（M4 契约 §6）：
// 把用户输入（HTTP 请求体或模型产出的工具参数）校验并构造成待 upsert 的
// types.Source。两个入口共用同一份校验/幂等键规则，避免 agent 加源与 API 加源语义漂移。
package sourcespec

import (
	"encoding/json"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/YouToco/vane/types"
)

// maxSourceParamRunes 是 query/keyword/title 的长度上限（字符数）。
const maxSourceParamRunes = 256

// Spec 是信源构造入参。Platform + Capability 决定必填 Params。
type Spec struct {
	Platform   string
	Capability string
	Params     map[string]string
	Title      string
}

// Build 校验并构造待 upsert 的信源；校验失败返回给用户的中文文案（空串=成功）。
//
// 幂等键规则（契约 §5.2）：
//   - web/feed → 真实 http(s) 地址（原样不合成）
//   - 其余    → vane://<platform>/<capability>?<params>（手工固定顺序拼接）
func Build(spec Spec) (*types.Source, string) {
	for _, v := range spec.Params {
		if utf8.RuneCountInString(v) > maxSourceParamRunes {
			return nil, "参数过长（上限 256 字符）"
		}
	}
	if utf8.RuneCountInString(spec.Title) > maxSourceParamRunes {
		return nil, "title 过长（上限 256 字符）"
	}

	p := types.Platform(spec.Platform)
	c := types.Capability(spec.Capability)

	switch p {
	case types.PlatformWeb:
		return buildWeb(c, spec.Params, spec.Title)
	case types.PlatformX:
		return buildX(c, spec.Params, spec.Title)
	case types.PlatformXHS:
		return buildXHS(c, spec.Params, spec.Title)
	default:
		return nil, "platform 仅支持 web / x / xhs"
	}
}

func buildWeb(cap types.Capability, params map[string]string, title string) (*types.Source, string) {
	switch cap {
	case types.CapFeed:
		rawURL := strings.TrimSpace(params["url"])
		u, err := url.Parse(rawURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, "url 必须是合法的 http/https 地址"
		}
		src := &types.Source{
			Platform:   types.PlatformWeb,
			Capability: types.CapFeed,
			URL:        rawURL,
			Title:      title,
			Status:     types.SourceStatusActive,
		}
		if catJSON := params["categories"]; catJSON != "" {
			var cats []string
			if json.Unmarshal([]byte(catJSON), &cats) == nil && len(cats) > 0 {
				cfg, _ := json.Marshal(map[string]interface{}{"categories": cats})
				src.Config = cfg
			}
		}
		return src, ""

	case types.CapSearch:
		query := strings.TrimSpace(params["query"])
		if query == "" {
			return nil, "web/search 必须提供 query（搜索词）"
		}
		cfgMap := map[string]string{"query": query}
		category := strings.TrimSpace(params["category"])
		if category != "" {
			cfgMap["category"] = category
		}
		cfg, err := json.Marshal(cfgMap)
		if err != nil {
			return nil, "构造信源配置失败"
		}
		if title == "" {
			title = "搜索: " + query
		}
		syntheticURL := "vane://web/search?q=" + url.QueryEscape(query)
		if category != "" {
			syntheticURL += "&category=" + url.QueryEscape(category)
		}
		return &types.Source{
			Platform:   types.PlatformWeb,
			Capability: types.CapSearch,
			URL:        syntheticURL,
			Title:      title,
			Config:     cfg,
			Status:     types.SourceStatusActive,
		}, ""

	default:
		return nil, "web 平台仅支持 feed / search 能力"
	}
}

func buildX(cap types.Capability, params map[string]string, title string) (*types.Source, string) {
	switch cap {
	case types.CapUserPosts:
		screenName := strings.TrimSpace(params["screen_name"])
		if screenName == "" {
			return nil, "x/user_posts 必须提供 screen_name"
		}
		screenName = strings.TrimPrefix(screenName, "@")
		cfg, err := json.Marshal(map[string]string{"screen_name": screenName})
		if err != nil {
			return nil, "构造信源配置失败"
		}
		if title == "" {
			title = "X: @" + screenName
		}
		return &types.Source{
			Platform:   types.PlatformX,
			Capability: types.CapUserPosts,
			URL:        "vane://x/user_posts?screen_name=" + url.QueryEscape(screenName),
			Title:      title,
			Config:     cfg,
			Status:     types.SourceStatusActive,
		}, ""

	default:
		return nil, "x 平台仅支持 user_posts 能力"
	}
}

func buildXHS(cap types.Capability, params map[string]string, title string) (*types.Source, string) {
	switch cap {
	case types.CapSearch:
		keyword := strings.TrimSpace(params["keyword"])
		if keyword == "" {
			return nil, "xhs/search 必须提供 keyword（小红书搜索关键词）"
		}
		cfg, err := json.Marshal(map[string]string{"keyword": keyword})
		if err != nil {
			return nil, "构造信源配置失败"
		}
		if title == "" {
			title = "小红书: " + keyword
		}
		return &types.Source{
			Platform:   types.PlatformXHS,
			Capability: types.CapSearch,
			URL:        "vane://xhs/search?keyword=" + url.QueryEscape(keyword),
			Title:      title,
			Config:     cfg,
			Status:     types.SourceStatusActive,
		}, ""

	default:
		return nil, "xhs 平台仅支持 search 能力"
	}
}

// BuildLegacy 接受 M6 前的扁平入参（type + url/query/keyword/category），
// 映射到 (platform, capability, params) 后转 Build。
// 存在的唯一理由是仓库外前端 vane-web 的兼容窗口（契约 §13.2）。
func BuildLegacy(typ, rawURL, query, keyword, title, category string) (*types.Source, string) {
	switch types.SourceType(typ) {
	case types.SourceTypeRSS, "":
		return Build(Spec{
			Platform:   string(types.PlatformWeb),
			Capability: string(types.CapFeed),
			Params:     map[string]string{"url": rawURL},
			Title:      title,
		})
	case types.SourceTypeExa:
		params := map[string]string{"query": query}
		if category != "" {
			params["category"] = category
		}
		return Build(Spec{
			Platform:   string(types.PlatformWeb),
			Capability: string(types.CapSearch),
			Params:     params,
			Title:      title,
		})
	case types.SourceTypeTikHubXHS:
		return Build(Spec{
			Platform:   string(types.PlatformXHS),
			Capability: string(types.CapSearch),
			Params:     map[string]string{"keyword": keyword},
			Title:      title,
		})
	default:
		return nil, "type 仅支持 rss / exa / tikhub_xhs"
	}
}

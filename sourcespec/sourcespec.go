// Package sourcespec 是 api 与 agent 工具共用的信源构造层（M4 契约 §6）：
// 把用户输入（HTTP 请求体或模型产出的工具参数）校验并构造成待 upsert 的
// types.Source。两个入口共用同一份校验/幂等键规则，避免 agent 加源与 API 加源语义漂移。
package sourcespec

import (
	"encoding/json"
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/YouToco/vane/sourcecatalog"
	"github.com/YouToco/vane/types"
)

// maxSourceParamRunes 是 query/keyword/title 的长度上限（字符数）。
const maxSourceParamRunes = 256

// xhsProfileRe 从小红书用户主页链接里抽 user_id：形如
// https://www.xiaohongshu.com/user/profile/6a5578b3000000000e03cc00 。
// user_id 恒为 24 位小写十六进制（实测），据此锚定，避免把 query 串里别的段误当 id。
var xhsProfileRe = regexp.MustCompile(`/user/profile/([0-9a-f]{24})`)

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

	// 能力门禁（契约 §2）：组合在注册表里但标记为不可用（如 x/search）时，直接把
	// sourcecatalog 的 Reason 回给用户/agent，绝不构造一个注定静默失败的坏源。
	// 不在表里的组合（如 xhs/feed）不在此拦——交给下面各 build* 的 default 给出更贴切的提示。
	if entry, ok := sourcecatalog.Lookup(p, c); ok && !entry.Available() {
		return nil, entry.Reason
	}

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

	case types.CapPageWatch:
		rawURL := strings.TrimSpace(params["url"])
		u, err := url.Parse(rawURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, "url 必须是合法的 http/https 地址"
		}
		if title == "" {
			title = "监控: " + u.Host + u.Path
		}
		return &types.Source{
			Platform:   types.PlatformWeb,
			Capability: types.CapPageWatch,
			URL:        rawURL,
			Title:      title,
			Status:     types.SourceStatusActive,
		}, ""

	default:
		return nil, "web 平台仅支持 feed / search / page_watch 能力"
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

	case types.CapUserPosts:
		// 接受 user_id（24 位 hex）直填，或从用户主页链接 profile_url/url 里抽取——
		// 后者对人更友好（没人记得住 24 位 hex），两种输入都归一到同一个 user_id，
		// 故同一个博主无论怎么加，幂等键 vane://xhs/user_posts?user_id=<id> 都相同。
		userID := extractXHSUserID(params)
		if userID == "" {
			return nil, "xhs/user_posts 必须提供 user_id（小红书用户 ID，24 位十六进制）或 profile_url（用户主页链接）"
		}
		cfg, err := json.Marshal(map[string]string{"user_id": userID})
		if err != nil {
			return nil, "构造信源配置失败"
		}
		if title == "" {
			title = "小红书用户: " + userID
		}
		return &types.Source{
			Platform:   types.PlatformXHS,
			Capability: types.CapUserPosts,
			URL:        "vane://xhs/user_posts?user_id=" + url.QueryEscape(userID),
			Title:      title,
			Config:     cfg,
			Status:     types.SourceStatusActive,
		}, ""

	default:
		return nil, "xhs 平台仅支持 search / user_posts 能力"
	}
}

// extractXHSUserID 从 params 里解析小红书 user_id：优先 user_id 直填，其次从
// profile_url / url 里按 /user/profile/<24hex> 抽取。都拿不到返回空串。
func extractXHSUserID(params map[string]string) string {
	if uid := strings.TrimSpace(params["user_id"]); uid != "" {
		// 若误把整条主页链接填进了 user_id，也从中抽一次，容错。
		if m := xhsProfileRe.FindStringSubmatch(uid); m != nil {
			return m[1]
		}
		return uid
	}
	for _, k := range []string{"profile_url", "url"} {
		if raw := strings.TrimSpace(params[k]); raw != "" {
			if m := xhsProfileRe.FindStringSubmatch(raw); m != nil {
				return m[1]
			}
		}
	}
	return ""
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

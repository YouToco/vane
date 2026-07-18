// Package sourcespec 是 api 与 agent 工具共用的信源构造层（M4 契约 §6）：
// 把用户输入（HTTP 请求体或模型产出的工具参数）校验并构造成待 upsert 的
// types.Source。两个入口共用同一份校验/幂等键规则，避免 agent 加源与 API 加源语义漂移。
package sourcespec

import (
	"encoding/json"
	"net/url"
	"regexp"
	"sort"
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
		cats, cerr := parseCategories(params["categories"])
		if cerr != "" {
			return nil, cerr
		}
		src := &types.Source{
			Platform:   types.PlatformWeb,
			Capability: types.CapFeed,
			URL:        rawURL,
			Title:      title,
			Status:     types.SourceStatusActive,
		}
		if len(cats) > 0 {
			cfg, err := json.Marshal(map[string]any{"categories": cats})
			if err != nil {
				return nil, "构造信源配置失败"
			}
			src.Config = cfg
			// 分类过滤会改变抓取结果集，因此**必须进入幂等键**（不变量 I-S2）。
			// 否则 A 订阅 [AI]、B 订阅同一 RSS 要 [区块链]，两人共用一行 source，
			// 后写者把前写者的过滤条件改掉——A 的信源从此抓的是别人要的东西。
			//
			// 用 URL fragment 承载判别位：Go 的 http 客户端**不会把 fragment 发到线上**
			// （(*url.URL).RequestURI() 不含 Fragment，已实测），所以 url 列既是幂等键、
			// 又仍是可直接抓取的真实地址，fetcher 零改动。
			//
			// cats 已归一化（trim+小写+去重+升序，与 applyCategories 的匹配口径逐字对齐），
			// 排序是幂等键正确性的关键：分类是无序集合，集合相同、顺序不同必须是同一个源。
			// 逐个 QueryEscape 再 join，避免分类名里含 "," / "#" 时判别位产生歧义。
			esc := make([]string, len(cats))
			for i, c := range cats {
				esc[i] = url.QueryEscape(c)
			}
			marker := "vane-categories=" + strings.Join(esc, ",")
			// 无分类时 URL 保持逐字节不变——存量 feed 源的幂等键不受本次改动影响。
			if strings.Contains(rawURL, "#") {
				src.URL = rawURL + "&" + marker
			} else {
				src.URL = rawURL + "#" + marker
			}
		}
		return src, ""

	case types.CapSearch:
		query := strings.TrimSpace(params["query"])
		if query == "" {
			return nil, "web/search 必须提供 query（搜索词）"
		}
		// include_domains（D-2 修复 / §0.3 追新的解药）：入参是 JSON 字符串数组（与
		// categories 入参格式一致），归一化为「TrimSpace + 小写 + 去空 + 去重 + 升序」。
		// 排序是幂等键正确性的关键：域名是无序集合，集合相同、顺序不同必须是同一个源。
		domains, derr := parseIncludeDomains(params["include_domains"])
		if derr != "" {
			return nil, derr
		}
		// config 用 map[string]any：include_domains 要以 JSON 数组落库，与
		// fetcher/exa.go 的 exaSourceConfig.IncludeDomains []string 逐字节对齐。
		cfgMap := map[string]any{"query": query}
		category := strings.TrimSpace(params["category"])
		if category != "" {
			cfgMap["category"] = category
		}
		if len(domains) > 0 {
			cfgMap["include_domains"] = domains
		}
		cfg, err := json.Marshal(cfgMap)
		if err != nil {
			return nil, "构造信源配置失败"
		}
		if title == "" {
			title = "搜索: " + query
		}
		// 幂等键参数顺序（M6 §5.2 规则 B，不可重排）：q → category → include_domains。
		// include_domains 已排序，逗号 join 后整体 QueryEscape；手工拼接，绝不用
		// url.Values.Encode()（它按字母序重排，破坏 008 回填一致性）。
		syntheticURL := "vane://web/search?q=" + url.QueryEscape(query)
		if category != "" {
			syntheticURL += "&category=" + url.QueryEscape(category)
		}
		if len(domains) > 0 {
			syntheticURL += "&include_domains=" + url.QueryEscape(strings.Join(domains, ","))
		}
		return &types.Source{
			Platform:   types.PlatformWeb,
			Capability: types.CapSearch,
			URL:        syntheticURL,
			Title:      title,
			Config:     cfg,
			Status:     types.SourceStatusActive,
		}, ""

	case types.CapContents:
		rawURL := strings.TrimSpace(params["url"])
		u, err := url.Parse(rawURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, "web/contents 必须提供合法的 http/https 页面地址（url）"
		}
		cfgMap := map[string]string{"url": rawURL}
		if t := strings.TrimSpace(params["title"]); t != "" {
			cfgMap["title"] = t
		}
		cfg, err := json.Marshal(cfgMap)
		if err != nil {
			return nil, "构造信源配置失败"
		}
		if title == "" {
			title = "页面监控: " + rawURL
		}
		// 幂等键含 url（一个页面一个监控源）；rawURL 本身是 http(s)://，与 contents:// 前缀
		// 的内容 canonical_key 不同命名空间，互不干扰。
		return &types.Source{
			Platform:   types.PlatformWeb,
			Capability: types.CapContents,
			URL:        "vane://web/contents?url=" + url.QueryEscape(rawURL),
			Title:      title,
			Config:     cfg,
			Status:     types.SourceStatusActive,
		}, ""

	default:
		return nil, "web 平台仅支持 feed / search / contents 能力"
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

// parseIncludeDomains 解析 web/search 的 include_domains 入参（JSON 字符串数组，与
// categories 入参格式对齐），归一化为「逐项 TrimSpace + ToLower + 去空 + 去重 + 升序」。
// 空串 / 空数组 / 全空项 → (nil, "")；非法 JSON → (nil, 中文自纠文案)。
//
// 排序 + 去重是幂等键正确性的保证（§5.2 规则 B）：include_domains 是无序集合，
// ["b.com","a.com"] 与 ["a.com","b.com"] 必须产出同一个源；小写化避免 A.com/a.com
// 撞成两个源（域名大小写不敏感）。此归一化结果同时写进 config 与幂等键，二者恒一致。
func parseIncludeDomains(raw string) ([]string, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, ""
	}
	var arr []string
	if err := json.Unmarshal([]byte(raw), &arr); err != nil {
		return nil, `include_domains 必须是 JSON 字符串数组（如 ["anthropic.com","claude.com"]）`
	}
	seen := make(map[string]struct{}, len(arr))
	out := make([]string, 0, len(arr))
	for _, d := range arr {
		d = strings.ToLower(strings.TrimSpace(d))
		if d == "" {
			continue
		}
		if _, dup := seen[d]; dup {
			continue
		}
		seen[d] = struct{}{}
		out = append(out, d)
	}
	if len(out) == 0 {
		return nil, ""
	}
	sort.Strings(out)
	return out, ""
}

// parseCategories 解析 web/feed 的 categories 入参（JSON 字符串数组），
// 归一化为「TrimSpace + 小写 + 去空 + 去重 + 升序」。
//
// 归一化口径与 fetcher.applyCategories 的匹配口径（ToLower(TrimSpace(c))）逐字对齐：
// 两边不一致的话，幂等键会把行为相同的两组分类判成不同的源。
//
// 与旧实现的一个行为差异：JSON 解析失败**不再静默忽略**。旧代码 `if Unmarshal(...) == nil`
// 把打错的 categories 当成「不过滤」，用户以为设了过滤、实际收到全量，且没有任何提示。
func parseCategories(raw string) ([]string, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, ""
	}
	var arr []string
	if err := json.Unmarshal([]byte(raw), &arr); err != nil {
		return nil, `categories 必须是 JSON 字符串数组（如 ["Product","Research"]）`
	}
	seen := make(map[string]struct{}, len(arr))
	out := make([]string, 0, len(arr))
	for _, c := range arr {
		c = strings.ToLower(strings.TrimSpace(c))
		if c == "" {
			continue
		}
		if _, dup := seen[c]; dup {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	if len(out) == 0 {
		return nil, ""
	}
	sort.Strings(out)
	return out, ""
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

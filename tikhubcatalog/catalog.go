// Package tikhubcatalog 是 TikHub 端点注册表（lookup 层，端点注册表契约 §2）：
// 把 TikHub 全量社媒数据端点（排除平台管理类与个别写/越界端点后约 1000 个）作为**可搜索的数据**
// 暴露给 agent——agent 用 search_endpoints 元工具按需发现端点，命中的端点被动态
// 注入为一等 FC 工具（agent/toolset.go）。
//
// 与 capabilitycatalog 的分界（契约 §1）：
//   - capabilitycatalog：订阅信源（追新入库→打分→推送）的实测准入注册表，每行背后是
//     手写 fetcher + 归一化 + 实测结论，Available 是质量承诺。
//   - tikhubcatalog（本包）：一次性查询（lookup）的端点目录，**未逐个实测**，
//     结果原样回给 agent 阅读、不进 content_items——错误会被模型和用户直接看到，
//     不存在「静默失败流进推送」的通道，所以准入门槛可以放低到"spec 里存在"。
//
// 数据来源：catalog.json 由 tikhubcatalog/gen 从 TikHub OpenAPI spec 生成后提交，
// 上游 spec 变更必须经 re-gen + code review 才影响本表（生成纪律见 gen/main.go 头注）。
package tikhubcatalog

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

//go:embed catalog.json
var catalogJSON []byte

// Param 是端点的一个参数。
type Param struct {
	Name string `json:"name"`
	// In 参数位置：query / path / body。对 agent 不可见（工具参数一律平铺为
	// JSON object），invoker 按此决定参数进 URL 还是请求体。
	In       string `json:"in"`
	Required bool   `json:"required,omitempty"`
	// Type 归一化类型：string / integer / number / boolean / object / array:<item>。
	Type    string `json:"type"`
	Desc    string `json:"desc,omitempty"`
	Default any    `json:"default,omitempty"`
	Enum    []any  `json:"enum,omitempty"`
}

// Entry 是注册表的一行（一个端点）。
type Entry struct {
	// Name 即 FC 工具名（path-slug，全表唯一，≤64 字符，gen 硬校验）。
	Name     string `json:"name"`
	Method   string `json:"method"` // GET / POST
	Path     string `json:"path"`
	Tag      string `json:"tag"`      // 上游 API 分组，如 Douyin-Web-API
	Platform string `json:"platform"` // 平台名小写，如 douyin / tiktok / xiaohongshu
	Summary  string `json:"summary"`
	// Description 上游文档截断版（600 rune，见 gen）。
	Description string  `json:"description,omitempty"`
	Params      []Param `json:"params,omitempty"`
}

var (
	entries []Entry
	byName  map[string]int
	// agentEntries is the provider-neutral, read-only discovery directory.
	// entries/byName remain the complete internal invocation registry because
	// long-running source bindings may rely on transport tokens that must never
	// become model-callable parameters.
	agentEntries []Entry
	agentByName  map[string]int
	// platforms 平台名 → 端点数，供 search_endpoints 工具描述枚举可搜索范围。
	platforms map[string]int
	idx       *bm25Index
)

// init 解析嵌入数据并建索引。catalog.json 是编译期常量（go:embed），解析失败
// 意味着构建产物损坏——panic 让进程根本起不来，比带着空注册表静默运行安全；
// 数据合法性由 catalog_test.go 在 CI 锁住，生产到不了 panic 分支。
func init() {
	if err := json.Unmarshal(catalogJSON, &entries); err != nil {
		panic(fmt.Sprintf("tikhubcatalog: 嵌入的 catalog.json 解析失败: %v", err))
	}
	byName = make(map[string]int, len(entries))
	agentByName = make(map[string]int, len(entries))
	platforms = make(map[string]int)
	for i, e := range entries {
		if _, dup := byName[e.Name]; dup {
			panic(fmt.Sprintf("tikhubcatalog: 端点名重复 %q（catalog.json 损坏，请重新生成）", e.Name))
		}
		byName[e.Name] = i
		if !agentEligible(e) {
			continue
		}
		agentByName[e.Name] = len(agentEntries)
		agentEntries = append(agentEntries, e)
		platforms[e.Platform]++
	}
	idx = buildIndex(agentEntries)
}

// Lookup 按工具名取端点。ok=false 表示注册表里没有这个端点。
func Lookup(name string) (Entry, bool) {
	i, ok := byName[name]
	if !ok {
		return Entry{}, false
	}
	return entries[i], true
}

// AgentLookup resolves only entries admitted to the model-callable discovery
// directory. Use Lookup for trusted internal source-binding execution.
func AgentLookup(name string) (Entry, bool) {
	i, ok := agentByName[name]
	if !ok {
		return Entry{}, false
	}
	return agentEntries[i], true
}

// Len 返回注册表端点总数。
func Len() int { return len(entries) }

// Entries returns a defensive copy of the model-callable discovery directory.
// Provider routing and source-binding-only entries remain internal.
func Entries() []Entry {
	return append([]Entry(nil), agentEntries...)
}

// AgentLen returns the number of model-callable dynamic tools.
func AgentLen() int { return len(agentEntries) }

// Platforms 返回全部平台名（字典序），供工具描述与参数校验枚举。
func Platforms() []string {
	out := make([]string, 0, len(platforms))
	for p := range platforms {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// PlatformCount 返回平台的端点数；未知平台返回 0。
func PlatformCount(p string) int { return platforms[p] }

var agentForbiddenCapabilityMarkers = []string{
	"guest_cookie", "generate_real_mstoken", "generate_wss_xb_signature",
	"generate_a_bogus", "generate_x_bogus", "generate_xbogus",
	"generate_xgnarly", "generate_ttwid", "generate_verify_fp",
	"generate_fingerprint", "generate_hashed_id", "generate_s_v_web_id",
	"generate_x_mssdk_info", "fetch_sec_token", "register_device",
	"private_message", "login_request", "encrypt_decrypt", "ttencrypt",
	"decrypt_strdata", "encrypt_strdata", "encrypt_uid", "encrypt_user_id",
	"get_sign_image", "add_video_play_count", "increase_post_view_count",
}

var agentForbiddenCredentialParams = map[string]bool{
	"access_token": true, "auth_token": true, "authorization": true,
	"csrf_token": true, "ms_token": true, "mstoken": true,
	"password": true, "passwd": true, "refresh_token": true,
	"sec_token": true, "session_token": true, "signature": true,
	"xsec_token": true,
}

func agentEligible(entry Entry) bool {
	haystack := strings.ToLower(strings.Join([]string{
		entry.Name, entry.Path, entry.Summary, entry.Description,
	}, " "))
	if strings.Contains(haystack, "cookie") {
		return false
	}
	for _, marker := range agentForbiddenCapabilityMarkers {
		if strings.Contains(haystack, marker) {
			return false
		}
	}
	for _, param := range entry.Params {
		name := strings.ToLower(strings.TrimSpace(param.Name))
		if strings.Contains(name, "cookie") ||
			strings.Contains(name, "secret") ||
			agentForbiddenCredentialParams[name] {
			return false
		}
	}
	return true
}

// Hit 是一次搜索命中。
type Hit struct {
	Entry Entry
	Score float64
}

// Search 在注册表上做 BM25 检索（中英双语：ASCII 词 + CJK bigram，见 bm25.go）。
// platform 非空时只在该平台的端点内检索（大小写不敏感）；topK 上限截断。
// 检索域覆盖：工具名、平台、tag、summary、description、参数名与参数描述——
// 与 Anthropic Tool Search Tool 的检索面对齐（工具名/描述/参数名/参数描述）。
func Search(query, platform string, topK int) []Hit {
	if topK <= 0 {
		return nil
	}
	return idx.search(query, platform, topK)
}

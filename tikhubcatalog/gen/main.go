// tikhubcatalog/gen 从 TikHub 的 OpenAPI 3.1 spec 生成 catalog.json（端点注册表数据）。
//
// 为什么是「生成后提交」而不是运行时拉取：注册表是 agent 工具面的一部分，必须随二进制
// 确定性发布——上游 spec 变更（加端点/改描述）只能经 re-gen + code review 进入生产，
// 不能让上游一次 spec 发布静默改变 agent 能调用什么（与 sourcecatalog「能力变更必须过
// 代码评审」同一价值观）。刷新方式：
//
//	go run ./tikhubcatalog/gen -spec <openapi.json 本地路径> -out tikhubcatalog/catalog.json
//
// 不带 -spec 时从 https://api.tikhub.io/openapi.json 在线拉取（仅开发机使用）。
//
// 瘦身规则（产物 ~1MB，控制 go:embed 体积与搜索索引规模）：
//   - 排除平台管理类 tag（excludedTags）：TikHub 账户/下载器/Demo/临时邮箱等与信源无关。
//   - description 截断到 maxDescRunes；响应 schema 一律丢弃（体积大头，agent 用不上）。
//   - 参数保留 query/path 参数 + POST body（$ref 解析一层，FastAPI 的 Body_* 都是平铺
//     object），各带类型/必填/描述/默认值/枚举。
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	defaultSpecURL = "https://api.tikhub.io/openapi.json"
	// maxDescRunes 端点 description 截断上限。TikHub 的 description 是 markdown
	// 全文（含返回字段说明、备注），前 600 rune 已覆盖用途/参数/返回三节的要点，
	// 后面的响应示例对搜索与调用都没有增量价值。
	maxDescRunes = 600
	// maxParamDescRunes 参数 description 截断上限。
	maxParamDescRunes = 200
)

// excludedTags 平台管理类 tag：与「社媒信源数据」无关，不进注册表（Boss 拍板 2026-07-18）。
var excludedTags = map[string]bool{
	"TikHub-User-API":       true, // TikHub 账户管理
	"TikHub-Downloader-API": true, // 下载器版本检查
	"Demo-API":              true, // Demo 缓存接口
	"Health-Check":          true, // 健康检查
	"Temp-Mail-API":         true, // 临时邮箱
	"iOS-Shortcut":          true, // iOS 快捷指令
}

// excludedEndpoints 按 path-slug 精确排除单个端点（两类原因，见各条注释）：
//
//  1. 会改变第三方平台状态的**写端点**（对抗审查 HIGH 缺陷）：lookup 层的免确认前提
//     是「查询不改系统状态」（契约 §0.3/§3），而这些端点会真实涨播放/涨浏览/注册
//     设备——被刷量灰产利用的写操作，绝不能进 agent 免确认直调的信源查询目录。
//  2. 越界/有社工风险的能力（Boss 拍板 2026-07-18）：生成「唤起 APP 给指定用户发私信」
//     链接的端点本身只读（只返回链接），但对信源查询产品偏门，且 agent 生成「点此
//     私信某人」链接有社工风险，排除。
//
// 精确匹配而非关键词：避免误伤「获取点赞列表」「获取关注列表」这类内容涉及点赞/关注
// 但本身只读的 fetch_ 端点。二类端点若确有需要，另走专门路径单独实现。
var excludedEndpoints = map[string]bool{
	// —— 写端点（改第三方平台状态）——
	"douyin_app_v3_add_video_play_count":         true, // 增加作品播放数（刷量写）
	"tiktok_app_v3_add_video_play_count":         true, // 增加作品播放数（刷量写）
	"pipixia_app_fetch_increase_post_view_count": true, // 增加作品浏览数（刷量写）
	"douyin_app_v3_register_device":              true, // 注册设备（产生真实设备指纹）
	"tiktok_web_device_register":                 true, // 注册设备（产生真实设备指纹）
	// —— 越界/社工风险（只读但排除）——
	"douyin_app_v3_open_douyin_app_to_send_private_message": true, // 生成私信唤起链接
	"tiktok_app_v3_open_tiktok_app_to_send_private_message": true, // 生成私信唤起链接
}

// toolNameRe 是 FC 工具名的合法字符集（DeepSeek/OpenAI 兼容面）。
var toolNameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// ---- OpenAPI spec 的最小读取结构（只解需要的字段）----

type spec struct {
	Paths      map[string]map[string]json.RawMessage `json:"paths"`
	Components struct {
		Schemas map[string]schemaObj `json:"schemas"`
	} `json:"components"`
}

type operation struct {
	Tags        []string    `json:"tags"`
	Summary     string      `json:"summary"`
	Description string      `json:"description"`
	Parameters  []parameter `json:"parameters"`
	RequestBody *struct {
		Content map[string]struct {
			Schema schemaObj `json:"schema"`
		} `json:"content"`
	} `json:"requestBody"`
}

type parameter struct {
	Name        string    `json:"name"`
	In          string    `json:"in"`
	Required    bool      `json:"required"`
	Description string    `json:"description"`
	Schema      schemaObj `json:"schema"`
}

type schemaObj struct {
	Ref         string               `json:"$ref"`
	Type        string               `json:"type"`
	AnyOf       []schemaObj          `json:"anyOf"`
	Items       *schemaObj           `json:"items"`
	Enum        []any                `json:"enum"`
	Default     any                  `json:"default"`
	Description string               `json:"description"`
	Properties  map[string]schemaObj `json:"properties"`
	Required    []string             `json:"required"`
}

// ---- 输出结构（与 tikhubcatalog.Entry/Param 的 JSON 形态一致）----

type outParam struct {
	Name     string `json:"name"`
	In       string `json:"in"` // query / path / body
	Required bool   `json:"required,omitempty"`
	Type     string `json:"type"`
	Desc     string `json:"desc,omitempty"`
	Default  any    `json:"default,omitempty"`
	Enum     []any  `json:"enum,omitempty"`
}

type outEntry struct {
	Name        string     `json:"name"`
	Method      string     `json:"method"`
	Path        string     `json:"path"`
	Tag         string     `json:"tag"`
	Platform    string     `json:"platform"`
	Summary     string     `json:"summary"`
	Description string     `json:"description,omitempty"`
	Params      []outParam `json:"params,omitempty"`
}

func main() {
	specPath := flag.String("spec", "", "OpenAPI spec 本地路径；为空时从 "+defaultSpecURL+" 拉取")
	outPath := flag.String("out", "tikhubcatalog/catalog.json", "输出文件路径")
	flag.Parse()

	raw, err := loadSpec(*specPath)
	if err != nil {
		fatal("读取 spec 失败: %v", err)
	}
	var sp spec
	if err := json.Unmarshal(raw, &sp); err != nil {
		fatal("解析 spec 失败: %v", err)
	}

	entries, err := buildEntries(&sp)
	if err != nil {
		fatal("%v", err)
	}

	out, err := json.MarshalIndent(entries, "", " ")
	if err != nil {
		fatal("序列化输出失败: %v", err)
	}
	out = append(out, '\n')
	if err := os.WriteFile(*outPath, out, 0o644); err != nil {
		fatal("写出失败: %v", err)
	}
	fmt.Printf("已生成 %s：%d 个端点（排除 %d 个平台管理类 tag）\n", *outPath, len(entries), len(excludedTags))
}

func loadSpec(path string) ([]byte, error) {
	if path != "" {
		return os.ReadFile(path)
	}
	resp, err := http.Get(defaultSpecURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func buildEntries(sp *spec) ([]outEntry, error) {
	var entries []outEntry
	seen := make(map[string]string) // name -> path，重名硬报错
	for path, methods := range sp.Paths {
		for method, rawOp := range methods {
			if method != "get" && method != "post" {
				continue // spec 实测只有 GET/POST；其他方法出现说明 spec 大改，宁可漏也不猜
			}
			var op operation
			if err := json.Unmarshal(rawOp, &op); err != nil {
				return nil, fmt.Errorf("解析 %s %s 失败: %w", method, path, err)
			}
			if len(op.Tags) == 0 || excludedTags[op.Tags[0]] {
				continue
			}

			name := toolName(path)
			if excludedEndpoints[name] ||
				excludedByRisk(name, path, op) {
				continue // 能力级排除，不以 GET/POST 推断安全性。
			}
			if !toolNameRe.MatchString(name) {
				return nil, fmt.Errorf("生成的工具名 %q（来自 %s）不符合 FC 命名约束，需人工处理", name, path)
			}
			if prev, dup := seen[name]; dup {
				return nil, fmt.Errorf("工具名冲突: %q 同时来自 %s 与 %s", name, prev, path)
			}

			e := outEntry{
				Name:        name,
				Method:      strings.ToUpper(method),
				Path:        path,
				Tag:         op.Tags[0],
				Platform:    platformOf(op.Tags[0]),
				Summary:     strings.TrimSpace(op.Summary),
				Description: truncateRunes(strings.TrimSpace(op.Description), maxDescRunes),
			}
			e.Params = append(e.Params, queryParams(op.Parameters)...)
			bodyParams, err := bodyParams(sp, &op)
			if err != nil {
				return nil, fmt.Errorf("%s %s: %w", method, path, err)
			}
			e.Params = append(e.Params, bodyParams...)
			seen[name] = path
			entries = append(entries, e)
		}
	}
	// 排序保证 re-gen 产物确定性：diff 只反映 spec 真实变化，不含遍历序噪音。
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries, nil
}

var forbiddenCapabilityMarkers = []string{
	"guest_cookie",
	"generate_real_mstoken",
	"generate_wss_xb_signature",
	"generate_a_bogus",
	"generate_x_bogus",
	"generate_xbogus",
	"generate_xgnarly",
	"generate_ttwid",
	"generate_verify_fp",
	"generate_fingerprint",
	"generate_hashed_id",
	"generate_s_v_web_id",
	"generate_x_mssdk_info",
	"fetch_sec_token",
	"register_device",
	"private_message",
	"login_request",
	"encrypt_decrypt",
	"ttencrypt",
	"decrypt_strdata",
	"encrypt_strdata",
	"encrypt_uid",
	"encrypt_user_id",
	"get_sign_image",
	"add_video_play_count",
	"increase_post_view_count",
}

var remoteMutationName = regexp.MustCompile(
	`(?:^|_)(?:add|increase|create|delete|remove|send|publish|upload|update|modify)(?:_|$)`,
)

// excludedByRisk classifies business capability, not transport method. Read-
// looking endpoints that mint credentials, device identity, signatures or
// social-engineering links are forbidden alongside genuine remote mutations.
// Fetch/get/search/list analytics stay available even when their subject is a
// like, follow or comment.
func excludedByRisk(name, path string, op operation) bool {
	haystack := strings.ToLower(strings.Join([]string{
		name, path, op.Summary, op.Description,
	}, " "))
	for _, marker := range forbiddenCapabilityMarkers {
		if strings.Contains(haystack, marker) {
			return true
		}
	}
	if remoteMutationName.MatchString(strings.ToLower(name)) &&
		!strings.Contains(name, "_fetch_") &&
		!strings.Contains(name, "_get_") &&
		!strings.Contains(name, "_search_") &&
		!strings.Contains(name, "_list_") {
		return true
	}
	return false
}

// toolName 从 path 生成 FC 工具名：/api/v1/tiktok/web/fetch_post_detail →
// tiktok_web_fetch_post_detail。不用 operationId：FastAPI 生成的 operationId
// 过半超 64 字符（实测 561/1024），而 path-slug 全量唯一且最长 61（2026-07-18 实测）。
func toolName(path string) string {
	s := strings.TrimPrefix(path, "/api/v1/")
	s = strings.TrimPrefix(s, "/api/")
	s = strings.Trim(s, "/")
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "-", "_")
	// path 参数如 {note_id} → note_id（目前 spec 无 path 参数，防御未来出现）。
	s = strings.NewReplacer("{", "", "}", "").Replace(s)
	return s
}

// platformOf 取 tag 的首段小写作为平台名：Douyin-Web-API → douyin。
// 实测对全部收录 tag 成立（TikTok/Douyin/Xiaohongshu/Weibo/…首段即平台）。
func platformOf(tag string) string {
	head, _, _ := strings.Cut(tag, "-")
	return strings.ToLower(head)
}

func queryParams(params []parameter) []outParam {
	var out []outParam
	for _, p := range params {
		if p.In != "query" && p.In != "path" {
			continue // header/cookie 参数（如有）是传输细节，不进工具面
		}
		desc := p.Description
		if desc == "" {
			desc = p.Schema.Description
		}
		out = append(out, outParam{
			Name:     p.Name,
			In:       p.In,
			Required: p.Required,
			Type:     schemaType(p.Schema),
			Desc:     truncateRunes(strings.TrimSpace(desc), maxParamDescRunes),
			Default:  p.Schema.Default,
			Enum:     p.Schema.Enum,
		})
	}
	return out
}

func bodyParams(sp *spec, op *operation) ([]outParam, error) {
	if op.RequestBody == nil {
		return nil, nil
	}
	media, ok := op.RequestBody.Content["application/json"]
	if !ok {
		return nil, nil // 非 JSON body（如 multipart）不支持，端点仍可注册但无 body 参数
	}
	sch := media.Schema
	if sch.Ref != "" {
		name := strings.TrimPrefix(sch.Ref, "#/components/schemas/")
		resolved, ok := sp.Components.Schemas[name]
		if !ok {
			return nil, fmt.Errorf("requestBody $ref %q 无法解析", sch.Ref)
		}
		sch = resolved
	}
	required := make(map[string]bool, len(sch.Required))
	for _, r := range sch.Required {
		required[r] = true
	}
	names := make([]string, 0, len(sch.Properties))
	for n := range sch.Properties {
		names = append(names, n)
	}
	sort.Strings(names) // map 遍历序不进产物
	var out []outParam
	for _, n := range names {
		ps := sch.Properties[n]
		out = append(out, outParam{
			Name:     n,
			In:       "body",
			Required: required[n],
			Type:     schemaType(ps),
			Desc:     truncateRunes(strings.TrimSpace(ps.Description), maxParamDescRunes),
			Default:  ps.Default,
			Enum:     ps.Enum,
		})
	}
	return out, nil
}

// schemaType 归一化参数类型。FastAPI 的 Optional[T] 表现为 anyOf[T, null]，
// 取首个非 null 类型；未知/复合类型退化为 string（工具面宁可宽松，权威校验在上游）。
func schemaType(s schemaObj) string {
	if s.Type != "" && s.Type != "null" {
		if s.Type == "array" {
			item := "string"
			if s.Items != nil {
				if t := schemaType(*s.Items); t != "" {
					item = t
				}
			}
			return "array:" + item
		}
		return s.Type
	}
	for _, alt := range s.AnyOf {
		if alt.Type == "null" {
			continue
		}
		return schemaType(alt)
	}
	return "string"
}

func truncateRunes(s string, max int) string {
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	runes := []rune(s)
	return string(runes[:max]) + "…"
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "tikhubcatalog/gen: "+format+"\n", args...)
	os.Exit(1)
}

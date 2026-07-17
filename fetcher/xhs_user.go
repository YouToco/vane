// xhs/user_posts 抓取器：订阅一个小红书博主，周期性抓其最新发布的笔记。
// 与 xhs/search（tikhub.go）同平台、同供应商（TikHub）、同身份（note_id），
// 但走不同端点、响应结构不同，且**拿不到 xsec_token**——这带来两处与 search 的关键差异
// （见 §响应差异 与 §URL）。
//
// 契约（2026-07-17 用真实 key 只读实测确认，key 名 vane）：
//   - 端点 GET {base}/api/v1/xiaohongshu/app_v2/get_user_posted_notes?user_id=&cursor=
//   - 鉴权头 Authorization: Bearer <key>（与 search / twitter 共用同一个 TikHub key）
//   - 外壳 code=200 且 data.success=true 时，笔记在 data.data.notes[]（注意是 notes 不是
//     search 的 items，且每项**直接是 note 对象**，没有 search 那层 {model_type, note} 包裹）
//   - 笔记字段：id / title / display_title / desc / create_time(Unix 秒) / type / user.{userid,nickname}
//
// §响应差异（相对 tikhub.go 的 search）：
//  1. desc 被上游截到 **100 rune**（search 是 60）。仍是半句，但 100 已明显越过 M5 那条
//     "≤60 rune 半句话诱发模型编造" 的坎（tikhub.go 开头有据）。故本抓取器**不做详情补全**：
//     user_posted_notes 的笔记项里**没有 xsec_token**（已正则扫全响应确认），而 web_v3 详情
//     接口必填 xsec_token；改用 app_v2 的 get_image/get_video_note_detail 虽只需 note_id，
//     但其一 get_video_note_detail 对图文笔记会静默串号（tikhub.go:38 实测），其二响应里
//     混着大量相关笔记/评论的 id、必须按 note_id 精确提取——这套闸门+计费+串号校验属于
//     契约警示的高风险面，样板阶段不引入，留作后续（见文件末 TODO）。
//  2. 身份仍是 note_id（xhsKey），与 search **同一命名空间**：用户同时订「AI编程」关键词源
//     和某个博主，博主的某条笔记又恰好被搜到时，canonical_key 相同→全局只落一份、不重复
//     打分推送。这正是身份按 platform（而非 provider/capability）分派的红利（identity.go）。
//
// §URL：笔记直链 https://www.xiaohongshu.com/explore/<note_id> **不拼 xsec_token**
//
//	（拿不到）。小红书 2024 起 web 直链缺 token 可能跳登录/唤起 App（tikhub.go:485 有据），
//	但 note_id 是稳定引用，卡片仍有完整标题+作者+预览。这是 user_posts 无 token 的已知取舍。
package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/types"
)

// tikhubUserPostedPath 是小红书"用户已发布笔记列表"端点。
const tikhubUserPostedPath = "/api/v1/xiaohongshu/app_v2/get_user_posted_notes"

// XHSUserFetcher 调用 TikHub 抓取某个小红书用户已发布的笔记。
// 与 TikHubFetcher 平行（同 base/key，不同端点与响应结构），刻意不复用其类型：
// 响应形状不同，硬套会像契约 §9.2 警告的那样埋隐性字段错位。
type XHSUserFetcher struct {
	apiKey   string
	baseURL  string // 可覆盖以便单测指向 httptest.Server
	client   *http.Client
	maxBytes int64
}

// NewXHSUser 构造 XHSUserFetcher。与 NewTikHub / NewTwitter 共用 cfg.TikhubAPIKey。
// 超时/响应上限兜底与其余 TikHub 抓取器一致（20s / 5MB）。apiKey 为空不在此报错——
// 留到 Fetch 时返回明确的 CodeValidation。
func NewXHSUser(cfg config.FetchConfig) *XHSUserFetcher {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	maxMB := cfg.MaxResponseMB
	if maxMB <= 0 {
		maxMB = 5
	}
	return &XHSUserFetcher{
		apiKey:  cfg.TikhubAPIKey,
		baseURL: tikhubDefaultBaseURL,
		// 禁跟随重定向：与 search/twitter 一致，防 Bearer key 被 30x 外带（noRedirect）。
		client:   &http.Client{Timeout: timeout, CheckRedirect: noRedirect},
		maxBytes: int64(maxMB) * 1024 * 1024,
	}
}

// xhsUserSourceConfig 是 xhs/user_posts 信源的 config JSONB 结构。user_id 必填。
type xhsUserSourceConfig struct {
	UserID string `json:"user_id"` // 小红书用户 ID（24 位十六进制），源身份，必填
}

// ────────── 响应建模（实测结构，勿复用 tikhubEnvelope）──────────

type xhsUserEnvelope struct {
	Code int         `json:"code"` // 外壳状态，成功为 200
	Data xhsUserData `json:"data"`
}

type xhsUserData struct {
	Success bool             `json:"success"`
	Msg     json.RawMessage  `json:"msg"` // 类型不稳定（null/string/对象），原样保留只用于错误信息
	Data    xhsUserNotesWrap `json:"data"`
}

type xhsUserNotesWrap struct {
	Notes   []xhsUserNote `json:"notes"`
	HasMore bool          `json:"has_more"`
}

// xhsUserNote 只取真正要用的字段。刻意不解析 last_update_time（实测恒为 0）与各类
// 计数（likes/collected_count…）：它们不进内容管线。
type xhsUserNote struct {
	ID           string        `json:"id"`
	Title        string        `json:"title"`
	DisplayTitle string        `json:"display_title"`
	Desc         string        `json:"desc"`
	CreateTime   int64         `json:"create_time"` // 发布时间，Unix 秒（实测与 search 的 timestamp 同值同单位）
	Type         string        `json:"type"`        // normal（图文）/ video
	User         xhsUserAuthor `json:"user"`
}

type xhsUserAuthor struct {
	UserID   string `json:"userid"`
	Nickname string `json:"nickname"`
}

// Fetch 按信源 config 里的 user_id 抓该用户最新发布的笔记，返回映射后的内容条目。
// 失败语义与 tikhub.go / x.go 一致：缺 key / 缺 user_id / 非法 config → CodeValidation
// （不可重试）；超时 → CodeFetchTimeout；429 → CodeFetchRateLimit；非 2xx 按 5xx/4xx 定可否重试。
func (f *XHSUserFetcher) Fetch(ctx context.Context, src types.Source) ([]types.ContentItem, error) {
	if f.apiKey == "" {
		return nil, types.NewAppError(types.CodeValidation,
			"小红书用户信源需要配置 VANE_FETCH_TIKHUB_API_KEY，当前为空", nil)
	}

	var sc xhsUserSourceConfig
	if len(src.Config) > 0 {
		if err := json.Unmarshal(src.Config, &sc); err != nil {
			return nil, types.NewAppError(types.CodeValidation,
				fmt.Sprintf("解析 xhs/user_posts 信源 config 失败（source_id=%d）", src.ID), err)
		}
	}
	if sc.UserID == "" {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("xhs/user_posts 信源缺少 user_id（source_id=%d）", src.ID), nil)
	}

	q := url.Values{}
	q.Set("user_id", sc.UserID)
	q.Set("cursor", "") // MVP 单页（最新一屏，约 20-30 条）；追新靠 canonical_key 全局去重增量，与 search 同策略
	reqURL := f.baseURL + tikhubUserPostedPath + "?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, types.NewAppError(types.CodeValidation, "构造小红书用户请求失败", err)
	}
	req.Header.Set("Authorization", "Bearer "+f.apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, classifyDoError(reqURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, types.NewAppError(types.CodeFetchRateLimit, "小红书用户笔记被限流(429)", nil)
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("TikHub 鉴权失败（HTTP %d），请检查 API key 与 scopes", resp.StatusCode), nil)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		ae := types.NewAppError(types.CodeFetchTimeout,
			fmt.Sprintf("小红书用户笔记返回非 2xx 状态 %d", resp.StatusCode), nil)
		ae.Retryable = resp.StatusCode >= 500 // 5xx 瞬态可重试，4xx 确定性不可重试。
		return nil, ae
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, f.maxBytes+1))
	if err != nil {
		return nil, classifyDoError(reqURL, err)
	}
	if int64(len(data)) > f.maxBytes {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("小红书用户笔记响应体超过 %d 字节上限", f.maxBytes), nil)
	}

	var env xhsUserEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		ae := types.NewAppError(types.CodeFetchTimeout, "解析小红书用户笔记响应失败", err)
		ae.Retryable = false
		return nil, ae
	}
	// 业务层错误：外壳 code 非 200 或 data.success=false（用户不存在、被风控、user_id 非法等）。
	// 保守按确定性不重试——瞬态故障通常直接表现为 HTTP 5xx（与 tikhub.go 同判据）。
	if env.Code != http.StatusOK || !env.Data.Success {
		ae := types.NewAppError(types.CodeFetchTimeout,
			fmt.Sprintf("小红书用户笔记业务失败（code=%d, success=%v, msg=%s，source_id=%d）",
				env.Code, env.Data.Success, string(env.Data.Msg), src.ID), nil)
		ae.Retryable = false
		return nil, ae
	}

	items := mapXHSUserNotes(src, sc.UserID, env.Data.Data.Notes)
	slog.Info("xhs/user_posts: 抓取完成",
		"source_id", src.ID, "user_id", sc.UserID,
		"notes_count", len(env.Data.Data.Notes), "items_count", len(items), "has_more", env.Data.Data.HasMore)
	return items, nil
}

// mapXHSUserNotes 把用户笔记映射为 ContentItem，指纹与身份由 finalize 统一补齐。
//
// URL 见文件头 §URL：explore/<note_id>，不拼 xsec_token（user_posted_notes 不返回）。
// 身份用 note_id（xhsKey），与 search 同源——CanonicalKey 按 PlatformXHS 取 ExternalID。
func mapXHSUserNotes(src types.Source, wantUserID string, notes []xhsUserNote) []types.ContentItem {
	now := time.Now().UTC()
	out := make([]types.ContentItem, 0, len(notes))
	for i := range notes {
		n := notes[i]
		if n.ID == "" {
			continue // 无 note_id = 无身份，跳过（finalize 也会挡，这里早退省事）。
		}
		// 防串号：只保留确实属于所订用户的笔记。get_user_posted_notes 实测只返回该用户的
		// 笔记，但上游偶发串号的先例（tikhub.go:38）值得一道廉价校验——user.userid 非空且
		// 不等于所订 user_id 时，这条不是我们要的，丢弃而非安在错误的源上。
		// （userid 为空则宽容保留：部分字段偶发缺失，靠 note_id 身份兜底。）
		if n.User.UserID != "" && n.User.UserID != wantUserID {
			slog.Warn("xhs/user_posts: 笔记作者与所订用户不符，疑似上游串号，跳过",
				"source_id", src.ID, "want_user_id", wantUserID, "got_user_id", n.User.UserID, "note_id", n.ID)
			continue
		}

		title := n.Title
		if title == "" {
			title = n.DisplayTitle // 部分笔记 title 空、display_title 有值。
		}

		item := types.ContentItem{
			SourceID:    src.ID,
			ExternalID:  n.ID,
			URL:         "https://www.xiaohongshu.com/explore/" + url.PathEscape(n.ID),
			Title:       title,
			Content:     truncateUTF8(n.Desc, tikhubMaxDescBytes),
			Author:      n.User.Nickname,
			PublishedAt: parseUnixSeconds(n.CreateTime), // Unix 秒；0/负视为未提供（复用 tikhub.go 的 helper）
			FetchedAt:   now,
			Kind:        types.KindArticle, // 一篇笔记是"一篇内容"（M6 契约 §7.2(b)：构造处赋值，finalize 只校验）
		}
		if !finalize(src, &item) {
			continue
		}
		out = append(out, item)
	}
	return out
}

// TODO(后续，非样板范围)：正文补全到全文。get_user_posted_notes 的 desc 截到 100 rune，
// 若要更全，可对 type=="normal" 走 app_v2/get_image_note_detail、type=="video" 走
// get_video_note_detail（均只需 note_id），但必须：①按 note_id 从混杂的相关笔记中精确
// 提取本条 desc；②校验返回 note_id 匹配防串号；③加与 tikhub.go enrichDescs 同款的
// 计费闸门+限速。实测 get_image_note_detail 能把 100→~199 rune，note_id 可匹配，但不返回
// xsec_token（URL 仍无法补 token）。评估收益/风险后再做。

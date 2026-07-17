// x/user_posts 抓取器：调 TikHub Twitter API 获取用户时间线。
// 与小红书（tikhub.go）共用 TikHub 供应商和 API key，但响应结构完全不同（§9.2），
// 且不需要详情补全——原创推文一次给全文（§9.3）。
package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/types"
)

const (
	twitterFetchPath = "/api/v1/twitter/web/fetch_user_post_tweet"
	// Twitter 原生时间格式（§9.5）——第三种时间表示（RSS=RFC3339, XHS=Unix秒）。
	twitterTimeLayout = "Mon Jan 02 15:04:05 -0700 2006"
)

// TwitterFetcher 调用 TikHub 的 Twitter Web API。
type TwitterFetcher struct {
	apiKey   string
	baseURL  string
	client   *http.Client
	maxBytes int64
}

// NewTwitter 构造 TwitterFetcher。与 NewTikHub 共用 cfg.TikhubAPIKey。
func NewTwitter(cfg config.FetchConfig) *TwitterFetcher {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	maxMB := cfg.MaxResponseMB
	if maxMB <= 0 {
		maxMB = 5
	}
	return &TwitterFetcher{
		apiKey:   cfg.TikhubAPIKey,
		baseURL:  tikhubDefaultBaseURL,
		client:   &http.Client{Timeout: timeout, CheckRedirect: noRedirect},
		maxBytes: int64(maxMB) * 1024 * 1024,
	}
}

// twitterSourceConfig 是 x/user_posts 信源的 config JSONB 结构。
type twitterSourceConfig struct {
	ScreenName string `json:"screen_name"`
}

// ────────── 响应建模（§9.2：不复用 tikhubEnvelope）──────────

type twitterResponse struct {
	Code int         `json:"code"`
	Data twitterData `json:"data"`
}

type twitterData struct {
	Status   string           `json:"status"`
	Timeline []twitterTweet   `json:"timeline"`
	Pinned   json.RawMessage  `json:"pinned"`
	User     json.RawMessage  `json:"user"`
}

type twitterTweet struct {
	TweetID        string          `json:"tweet_id"`
	Text           string          `json:"text"`
	CreatedAt      string          `json:"created_at"`
	ConversationID string          `json:"conversation_id"`
	Views          string          `json:"views"`
	Source         string          `json:"source,omitempty"`
	Author         twitterAuthor   `json:"author"`
	// 以下全部多态（§9.5 + 生产实测）：可能是 bool/string/对象/数组/null。
	Retweeted      json.RawMessage `json:"retweeted"`
	RetweetedTweet json.RawMessage `json:"retweeted_tweet"`
	Quoted         json.RawMessage `json:"quoted"`
	ReplyTo        json.RawMessage `json:"reply_to"`
	Media          json.RawMessage `json:"media"`
	Entities       json.RawMessage `json:"entities"`
}

type twitterAuthor struct {
	ScreenName string `json:"screen_name"`
	Name       string `json:"name"`
	RestID     string `json:"rest_id"`
}

// ────────── Fetch ──────────

func (f *TwitterFetcher) Fetch(ctx context.Context, src types.Source) ([]types.ContentItem, error) {
	if f.apiKey == "" {
		return nil, types.NewAppError(types.CodeValidation,
			"TikHub API key 未配置，无法抓取 X 信源", nil)
	}

	var sc twitterSourceConfig
	if len(src.Config) > 0 {
		if err := json.Unmarshal(src.Config, &sc); err != nil {
			return nil, types.NewAppError(types.CodeValidation,
				fmt.Sprintf("x/user_posts config 解析失败（source_id=%d）", src.ID), err)
		}
	}
	if sc.ScreenName == "" {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("x/user_posts 缺少 screen_name（source_id=%d）", src.ID), nil)
	}

	reqURL := f.baseURL + twitterFetchPath + "?screen_name=" + sc.ScreenName
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, types.NewAppError(types.CodeInternal, "构造 X 请求失败", err)
	}
	req.Header.Set("Authorization", "Bearer "+f.apiKey)

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, types.NewAppError(types.CodeFetchTimeout, "X API 调用失败", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, f.maxBytes))
	if err != nil {
		return nil, types.NewAppError(types.CodeFetchTimeout, "读取 X API 响应失败", err)
	}

	if resp.StatusCode != http.StatusOK {
		slog.Warn("x/user_posts: 非 200 响应",
			"source_id", src.ID, "status", resp.StatusCode, "screen_name", sc.ScreenName)
		return nil, types.NewAppError(types.CodeFetchTimeout,
			fmt.Sprintf("X API 返回 HTTP %d（source_id=%d）", resp.StatusCode, src.ID), nil)
	}

	var envelope twitterResponse
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, types.NewAppError(types.CodeFetchTimeout, "X API 响应解析失败", err)
	}

	// 判据是 code==200 && len(timeline)>0，不依赖 data.status（§9.2）。
	if envelope.Code != 200 {
		slog.Warn("x/user_posts: 业务层非 200",
			"source_id", src.ID, "code", envelope.Code, "screen_name", sc.ScreenName)
		return nil, types.NewAppError(types.CodeFetchTimeout,
			fmt.Sprintf("X API 业务错误 code=%d（source_id=%d）", envelope.Code, src.ID), nil)
	}

	// 只读 data.timeline，忽略 data.pinned（§9.6①）。
	tweets := envelope.Data.Timeline
	if len(tweets) == 0 {
		return nil, nil
	}

	var items []types.ContentItem
	for _, tw := range tweets {
		item := tweetToItem(tw, sc.ScreenName)
		if item == nil {
			continue
		}
		item.SourceID = src.ID
		if !finalize(src, item) {
			continue
		}
		items = append(items, *item)
	}

	slog.Info("x/user_posts: 抓取完成",
		"source_id", src.ID, "screen_name", sc.ScreenName,
		"timeline_count", len(tweets), "items_count", len(items))

	return items, nil
}

// tweetToItem 将一条推文映射为 ContentItem。转推拆包（§9.4）。
func tweetToItem(tw twitterTweet, fallbackScreenName string) *types.ContentItem {
	// 转推拆包（§9.4）：转推不是新内容，被转推的那条才是。
	// ExternalID 取被转推那条的 tweet_id → 同一原创推经多号转发只落一行 content_item。
	if rt := extractRetweet(tw); rt != nil {
		author := rt.Author.ScreenName
		if author == "" {
			author = fallbackScreenName
		}
		return &types.ContentItem{
			ExternalID:  rt.TweetID,
			Title:       "",
			Content:     rt.Text,
			URL:         "https://x.com/" + author + "/status/" + rt.TweetID,
			Author:      author,
			PublishedAt: parseTwitterTime(rt.CreatedAt),
			Kind:        types.KindArticle, // 一条推文是"一篇内容"（M6 契约 §7.2(b)：构造处赋值，finalize 只校验）
		}
	}

	// 原创 / 引用 / 回复——全部保留（§9.4）。
	author := tw.Author.ScreenName
	if author == "" {
		author = fallbackScreenName
	}
	return &types.ContentItem{
		ExternalID:  tw.TweetID,
		Title:       "",
		Content:     tw.Text,
		URL:         "https://x.com/" + author + "/status/" + tw.TweetID,
		Author:      author,
		PublishedAt: parseTwitterTime(tw.CreatedAt),
		Kind:        types.KindArticle, // 同上：原创/引用/回复同样是"一篇内容"
	}
}

// extractRetweet 从多态的 retweeted / retweeted_tweet 字段提取被转推的推文。
// 生产实测 retweeted 可能是 bool(true) 或对象（含完整推文数据），retweeted_tweet
// 可能存在或不存在。优先取 retweeted_tweet，其次尝试 retweeted 本身。
func extractRetweet(tw twitterTweet) *twitterTweet {
	if rt := tryParseTweet(tw.RetweetedTweet); rt != nil {
		return rt
	}
	if rt := tryParseTweet(tw.Retweeted); rt != nil {
		return rt
	}
	return nil
}

func tryParseTweet(raw json.RawMessage) *twitterTweet {
	if len(raw) == 0 || raw[0] != '{' {
		return nil
	}
	var t twitterTweet
	if json.Unmarshal(raw, &t) == nil && t.TweetID != "" {
		return &t
	}
	return nil
}

func parseTwitterTime(s string) *time.Time {
	t, err := time.Parse(twitterTimeLayout, s)
	if err != nil {
		return nil
	}
	u := t.UTC()
	return &u
}

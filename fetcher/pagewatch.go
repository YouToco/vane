package fetcher

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"

	"github.com/YouToco/vane/promptguard"
	"github.com/YouToco/vane/types"
)

// SnapshotStore 是 page_watch 所需的快照存储窄接口（同 SeenChecker 的设计动机）。
type SnapshotStore interface {
	Baseline(ctx context.Context, sourceID int64) (*types.PageSnapshot, error)
	PutSnapshot(ctx context.Context, snap *types.PageSnapshot) error
	SettleSnapshot(ctx context.Context, sourceID int64, canonicalKey string, v types.SnapshotVerdict) error
}

type pageWatchConfig struct {
	MinRows int `json:"min_rows,omitempty"`
}

const (
	defaultMinRows     = 5
	collapseThreshold  = 0.5
)

// PageWatchFetcher 监控页面变化（§10）。复用 RSS Fetcher 的 SSRF 保护栈。
type PageWatchFetcher struct {
	f     *Fetcher // 复用其 client（含 SSRF 保护）+ lookupIP/isBlocked
	snaps SnapshotStore
}

func NewPageWatch(f *Fetcher, snaps SnapshotStore) *PageWatchFetcher {
	return &PageWatchFetcher{f: f, snaps: snaps}
}

func (pw *PageWatchFetcher) Fetch(ctx context.Context, src types.Source) ([]types.ContentItem, error) {
	u, err := url.Parse(src.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("非法 page_watch URL: %q", src.URL), err)
	}

	host := u.Hostname()
	if ips, lerr := pw.f.lookupIP(host); lerr == nil {
		for _, ip := range ips {
			if pw.f.isBlocked(ip) {
				return nil, types.NewAppError(types.CodeValidation,
					fmt.Sprintf("page_watch %q 解析到私网地址 %s，已拒绝", host, ip), nil)
			}
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.URL, nil)
	if err != nil {
		return nil, types.NewAppError(types.CodeValidation, "构造抓取请求失败", err)
	}
	// 不设浏览器 User-Agent（§10.1）：ai.google.dev 设 Chrome UA 会无限重定向。

	resp, err := pw.f.client.Do(req)
	if err != nil {
		return nil, classifyDoError(src.URL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusForbidden {
		ae := types.NewAppError(types.CodeFetchTimeout,
			fmt.Sprintf("page_watch %s 返回 403 challenge", src.URL), nil)
		ae.Retryable = true
		return nil, ae
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		ae := types.NewAppError(types.CodeFetchTimeout,
			fmt.Sprintf("page_watch %s 返回非 2xx 状态 %d", src.URL, resp.StatusCode), nil)
		ae.Retryable = resp.StatusCode >= 500
		return nil, ae
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, pw.f.maxBytes+1))
	if err != nil {
		return nil, classifyDoError(src.URL, err)
	}
	if int64(len(data)) > pw.f.maxBytes {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("page_watch %s 响应体超过 %d 字节上限", src.URL, pw.f.maxBytes), nil)
	}

	extracted, err := extractTableText(data)
	if err != nil {
		ae := types.NewAppError(types.CodeFetchTimeout,
			fmt.Sprintf("page_watch %s HTML 解析失败", src.URL), err)
		ae.Retryable = false
		return nil, ae
	}
	extracted = promptguard.StripInvisible(extracted)

	var cfg pageWatchConfig
	if len(src.Config) > 0 {
		_ = json.Unmarshal(src.Config, &cfg)
	}
	minRows := cfg.MinRows
	if minRows == 0 {
		minRows = defaultMinRows
	}
	if minRows > 0 {
		rows := countRows(extracted)
		if rows < minRows {
			return nil, types.NewAppError(types.CodeValidation,
				fmt.Sprintf("page_watch %s 抽出 %d 行 < min_rows %d", src.URL, rows, minRows), nil)
		}
	}

	newHash := sha256Hex(extracted)

	base, err := pw.snaps.Baseline(ctx, src.ID)
	if err != nil {
		return nil, err
	}

	if base == nil {
		err = pw.snaps.PutSnapshot(ctx, &types.PageSnapshot{
			SourceID:      src.ID,
			CanonicalKey:  watchKey(src.URL, "", newHash),
			ContentHash:   newHash,
			ExtractedText: extracted,
			Verdict:       types.SnapshotVerdictBaseline,
		})
		if err != nil {
			return nil, err
		}
		slog.Info("page_watch: 首轮建基线", "source_id", src.ID, "url", src.URL, "hash", newHash[:12])
		return nil, nil
	}

	if base.ContentHash == newHash {
		return nil, nil
	}

	// 塌缩检测（§10.5）
	if len(base.ExtractedText) > 0 {
		ratio := float64(len(extracted)) / float64(len(base.ExtractedText))
		if ratio < collapseThreshold {
			return nil, types.NewAppError(types.CodeValidation,
				fmt.Sprintf("page_watch %s 内容塌缩 %.0f%%（%d→%d 字节），跳过", src.URL, (1-ratio)*100, len(base.ExtractedText), len(extracted)), nil)
		}
	}

	key := watchKey(src.URL, base.ContentHash, newHash)
	err = pw.snaps.PutSnapshot(ctx, &types.PageSnapshot{
		SourceID:      src.ID,
		CanonicalKey:  key,
		ContentHash:   newHash,
		ExtractedText: extracted,
		Verdict:       types.SnapshotVerdictPending,
	})
	if err != nil {
		return nil, err
	}

	diff := simpleDiff(base.ExtractedText, extracted)

	item := types.ContentItem{
		SourceID:     src.ID,
		Kind:         types.KindChange,
		CanonicalKey: key,
		URL:          src.URL,
		Title:        src.Title + " has changed",
		Content:      diff,
	}
	if !finalize(src, &item) {
		return nil, nil
	}
	return []types.ContentItem{item}, nil
}

func watchKey(rawURL, prevHash, newHash string) string {
	return fmt.Sprintf("watch://%s#%s->%s", rawURL, prevHash, newHash)
}

func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", h)
}

// extractTableText 用 goquery 把 HTML 中的 <tr> 行展平为管道分隔文本（§10.2）。
// 若页面无 <tr>，退回 body 纯文本。
func extractTableText(html []byte) (string, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(html)))
	if err != nil {
		return "", err
	}

	doc.Find("script, style, noscript").Remove()

	var lines []string
	doc.Find("tr").Each(func(_ int, tr *goquery.Selection) {
		var cells []string
		tr.Find("td, th").Each(func(_ int, cell *goquery.Selection) {
			text := strings.TrimSpace(cell.Text())
			if text != "" {
				cells = append(cells, text)
			}
		})
		if len(cells) > 0 {
			lines = append(lines, strings.Join(cells, " | "))
		}
	})

	if len(lines) == 0 {
		return strings.TrimSpace(doc.Find("body").Text()), nil
	}
	return strings.Join(lines, "\n"), nil
}

func countRows(text string) int {
	if text == "" {
		return 0
	}
	return strings.Count(text, "\n") + 1
}

// simpleDiff 生成两段文本的行级变化摘要。
func simpleDiff(old, new string) string {
	oldLines := strings.Split(old, "\n")
	newLines := strings.Split(new, "\n")

	oldSet := make(map[string]struct{}, len(oldLines))
	for _, l := range oldLines {
		oldSet[l] = struct{}{}
	}
	newSet := make(map[string]struct{}, len(newLines))
	for _, l := range newLines {
		newSet[l] = struct{}{}
	}

	var b strings.Builder
	for _, l := range oldLines {
		if _, ok := newSet[l]; !ok {
			b.WriteString("- ")
			b.WriteString(l)
			b.WriteString("\n")
		}
	}
	for _, l := range newLines {
		if _, ok := oldSet[l]; !ok {
			b.WriteString("+ ")
			b.WriteString(l)
			b.WriteString("\n")
		}
	}
	result := strings.TrimRight(b.String(), "\n")
	if result == "" {
		return "(no visible line-level changes)"
	}
	return result
}

// nowUTC 用于 FetchedAt — 可被测试覆盖的时间源在 Fetcher 上。
func nowUTC() time.Time { return time.Now().UTC() }

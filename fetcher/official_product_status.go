package fetcher

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/types"
)

const (
	kimiPricingPath       = "/membership/pricing"
	kimiGoodsPath         = "/apiv2/kimi.gateway.order.v1.GoodsService/ListGoods"
	kimiGoodsURL          = "https://www.kimi.com" + kimiGoodsPath
	productStatusMaxBytes = 2 * 1024 * 1024
)

// ProductStatusFetcher turns a supported official, dynamically rendered
// pricing page into a deterministic purchase-state observation. The upstream
// endpoint is first-party, public and allowlisted in code; arbitrary endpoint
// URLs never come from the model or task definition.
type ProductStatusFetcher struct {
	client   *http.Client
	kimiURL  string
	recorder BindingCallRecorder
}

func NewProductStatus(cfg config.FetchConfig, recorder BindingCallRecorder) *ProductStatusFetcher {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	return &ProductStatusFetcher{
		client:   &http.Client{Timeout: timeout, CheckRedirect: noRedirect},
		kimiURL:  kimiGoodsURL,
		recorder: recorder,
	}
}

type productStatusConfig struct {
	URL   string `json:"url"`
	Title string `json:"title,omitempty"`
}

type kimiGoodsResponse struct {
	Goods []kimiGood `json:"goods"`
}

type kimiGood struct {
	ID              string `json:"id"`
	Title           string `json:"title"`
	MembershipLevel string `json:"membershipLevel"`
	Amounts         []struct {
		Currency     string `json:"currency"`
		PriceInCents string `json:"priceInCents"`
	} `json:"amounts"`
	TransitionSummary struct {
		Reason string `json:"reason"`
	} `json:"transitionSummary"`
	BillingCycle struct {
		Duration int    `json:"duration"`
		TimeUnit string `json:"timeUnit"`
	} `json:"billingCycle"`
}

func (f *ProductStatusFetcher) Fetch(ctx context.Context, src types.FetchTarget) ([]types.ContentItem, error) {
	return f.fetchWithEffectGate(ctx, src, nil)
}

func (f *ProductStatusFetcher) fetchWithEffectGate(
	ctx context.Context,
	src types.FetchTarget,
	beforeEffect func(context.Context) error,
) ([]types.ContentItem, error) {
	var cfg productStatusConfig
	if err := json.Unmarshal(src.Config, &cfg); err != nil {
		return nil, types.NewAppError(types.CodeValidation,
			"解析 web/product_status 配置失败", err)
	}
	pageURL, err := supportedKimiPricingURL(cfg.URL)
	if err != nil {
		return nil, types.NewAppError(types.CodeValidation, err.Error(), nil)
	}
	payload := []byte(`{"pageSize":0,"pageToken":"","domains":[]}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.kimiURL, bytes.NewReader(payload))
	if err != nil {
		return nil, types.NewAppError(types.CodeInternal, "构造 Kimi 官方商品请求失败", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Connect-Protocol-Version", "1")
	req.Header.Set("x-msh-platform", "web")
	req.Header.Set("X-Language", "zh-CN")
	if err := checkEffectGate(ctx, beforeEffect); err != nil {
		return nil, err
	}

	start := time.Now()
	resp, err := f.client.Do(req)
	elapsed := time.Since(start)
	if err != nil {
		ae := classifyDoError(f.kimiURL, err)
		f.record(ctx, src, payload, nil, 0, elapsed, ae)
		return nil, ae
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, productStatusMaxBytes+1))
	if readErr != nil {
		ae := classifyDoError(f.kimiURL, readErr)
		f.record(ctx, src, payload, body, resp.StatusCode, elapsed, ae)
		return nil, ae
	}
	if len(body) > productStatusMaxBytes {
		ae := types.NewAppError(types.CodeValidation, "Kimi 官方商品响应超过大小上限", nil)
		f.record(ctx, src, payload, body, resp.StatusCode, elapsed, ae)
		return nil, ae
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		ae := types.NewAppError(types.CodeFetchTimeout,
			fmt.Sprintf("Kimi 官方商品接口返回 HTTP %d", resp.StatusCode), nil)
		ae.Retryable = resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
		f.record(ctx, src, payload, body, resp.StatusCode, elapsed, ae)
		return nil, ae
	}
	var result kimiGoodsResponse
	if err := json.Unmarshal(body, &result); err != nil {
		ae := types.NewAppError(types.CodeFetchTimeout, "解析 Kimi 官方商品响应失败", err)
		ae.Retryable = false
		f.record(ctx, src, payload, body, resp.StatusCode, elapsed, ae)
		return nil, ae
	}
	content, status, err := normalizeKimiProductStatus(result)
	if err != nil {
		ae := types.NewAppError(types.CodeFetchTimeout, err.Error(), nil)
		ae.Retryable = false
		f.record(ctx, src, payload, body, resp.StatusCode, elapsed, ae)
		return nil, ae
	}
	f.record(ctx, src, payload, body, resp.StatusCode, elapsed, nil)

	sum := sha256.Sum256([]byte(content))
	hash := hex.EncodeToString(sum[:])[:exaContentsHashLen]
	title := strings.TrimSpace(cfg.Title)
	if title == "" {
		title = "Kimi 会员套餐购买状态：" + status
	}
	item := types.ContentItem{
		SourceID: src.ID, ExternalID: hash,
		CanonicalKey: "product-status://" + pageURL + "#" + hash,
		Kind:         types.KindPageContent, URL: pageURL, Title: title,
		Content: content, FetchedAt: time.Now().UTC(),
	}
	if reason := finalize(src, &item); reason != dropNone {
		var tally dropTally
		tally.add(reason)
		return nil, allDroppedErr(src, 1, tally)
	}
	return []types.ContentItem{item}, nil
}

func supportedKimiPricingURL(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme != "https" || u.User != nil {
		return "", fmt.Errorf("web_product_status 当前只支持 Kimi 官方 HTTPS 定价页")
	}
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	path := strings.TrimSuffix(u.EscapedPath(), "/")
	if (host != "www.kimi.com" && host != "kimi.com") || path != kimiPricingPath {
		return "", fmt.Errorf("web_product_status 当前只支持 https://www.kimi.com/membership/pricing")
	}
	return "https://www.kimi.com" + kimiPricingPath, nil
}

type normalizedKimiPlan struct {
	Title, Cycle, Currency, Price, Reason string
}

func normalizeKimiProductStatus(response kimiGoodsResponse) (string, string, error) {
	plans := make([]normalizedKimiPlan, 0, len(response.Goods))
	for _, good := range response.Goods {
		if good.MembershipLevel == "LEVEL_FREE" {
			continue
		}
		for _, amount := range good.Amounts {
			cents, err := strconv.ParseInt(amount.PriceInCents, 10, 64)
			if err != nil || cents <= 0 {
				continue
			}
			cycle := fmt.Sprintf("%d %s", good.BillingCycle.Duration, good.BillingCycle.TimeUnit)
			plans = append(plans, normalizedKimiPlan{
				Title: strings.TrimSpace(good.Title), Cycle: cycle,
				Currency: strings.ToUpper(amount.Currency), Price: strconv.FormatInt(cents, 10),
				Reason: strings.TrimSpace(good.TransitionSummary.Reason),
			})
		}
	}
	if len(plans) == 0 {
		return "", "", fmt.Errorf("Kimi 官方商品接口未返回任何付费套餐")
	}
	sort.Slice(plans, func(i, j int) bool {
		a, b := plans[i], plans[j]
		return a.Title+a.Cycle+a.Currency+a.Price < b.Title+b.Cycle+b.Currency+b.Price
	})
	status := "暂不可购买"
	allNeedApply := true
	for _, plan := range plans {
		if plan.Reason == "" {
			status = "可直接购买"
			allNeedApply = false
			break
		}
		if plan.Reason != "REASON_SUBSCRIPTION_NEED_APPLY" {
			allNeedApply = false
		}
	}
	if status != "可直接购买" && allNeedApply {
		status = "仅可预约（尚不可直接购买）"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "购买状态: %s\n", status)
	b.WriteString("官方页面: https://www.kimi.com/membership/pricing\n")
	b.WriteString("官方判定字段: transitionSummary.reason\n")
	b.WriteString("付费套餐:\n")
	for _, plan := range plans {
		fmt.Fprintf(&b, "- %s | %s | %s %s cents | reason=%s\n",
			plan.Title, plan.Cycle, plan.Currency, plan.Price, valueOr(plan.Reason, "NONE"))
	}
	return strings.TrimSpace(b.String()), status, nil
}

func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func (f *ProductStatusFetcher) record(
	ctx context.Context, src types.FetchTarget, arguments, result []byte,
	status int, elapsed time.Duration, callErr error,
) {
	if f.recorder == nil {
		return
	}
	recordCtx, cancel := detachedBindingRecordContext(ctx)
	defer cancel()
	trace, tenantID, userID, runSnapshotID := bindingAttribution(recordCtx)
	rec := &types.ToolCall{
		RunSnapshotID: runSnapshotID, TraceID: trace,
		TenantID: tenantID, UserID: userID,
		ToolName: "kimi:goods_list", ToolKind: types.ToolCallKindOfficialFetch,
		Provider: "kimi", EndpointPath: kimiGoodsPath,
		Arguments:     append(json.RawMessage(nil), arguments...),
		ResultPreview: toolResultPreview(result), ResultSize: len(result),
		HTTPStatus: &status, DurationMs: int(elapsed.Milliseconds()), UsageQuantity: 1,
	}
	if src.ID > 0 {
		id := src.ID
		rec.SourceID = &id
	}
	if callErr != nil {
		rec.ErrorType = types.ToolErrInternal
		rec.Error = truncateUTF8(callErr.Error(), 500)
	}
	f.recorder.RecordBindingCall(recordCtx, rec)
}

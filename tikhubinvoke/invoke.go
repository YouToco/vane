// Package tikhubinvoke 是 TikHub 端点的通用调用器（端点注册表契约 §4）：
// 按 tikhubcatalog.Entry 的元数据把平铺参数装配成 HTTP 请求（GET query / POST JSON
// body / path 替换），结果原样返回给调用方（agent 端点工具）。
//
// 与 fetcher 各 TikHub 抓取器的分界：fetcher 是订阅信源管道（归一化进 content_items，
// 逐端点手写、实测准入）；本包是 lookup 面（约 1000 端点共用一个装配器，零归一化），
// 二者只共享 config 里的同一个 API key。目标恒为固定可信主机 api.tikhub.io
// （URL 非用户可控），与 fetcher/tikhub.go 同理不需要 SSRF 拦截。
package tikhubinvoke

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/tikhubcatalog"
	"github.com/YouToco/vane/types"
)

const (
	defaultBaseURL = "https://api.tikhub.io"
	// requestTimeout 单次调用硬超时，对齐 fetcher/tikhub.go 的 client 预算量级：
	// agent loop 单条消息预算分钟级，一个挂死端点不该吃掉整条消息。
	requestTimeout = 20 * time.Second
	// maxBodyBytes 响应体读取上限（防超大响应打爆内存；截断给模型的预算另在
	// agent 端点工具，那是 token 护栏、这是内存护栏，两层各管各的）。
	maxBodyBytes = 2 << 20 // 2 MiB
)

// Invoker 零状态（除只读配置），多 goroutine 可并发复用。
type Invoker struct {
	hc      *http.Client
	baseURL string
	apiKey  string
	bodyCap int64 // 响应体读取上限；读到 cap+1 截止（见 WithBodyCap）
}

// Option 构造选项：测试注入 baseURL；绑定引擎（fetcher）对齐旧抓取器的
// 超时/响应上限配置（agent lookup 面不传，保持 20s/2MiB 默认不变）。
type Option func(*Invoker)

// WithBaseURL 覆盖上游地址（仅测试）。
func WithBaseURL(u string) Option {
	return func(v *Invoker) { v.baseURL = u }
}

// WithTimeout 覆盖单次调用超时（绑定引擎：cfg.Fetch.TimeoutSeconds）。
func WithTimeout(d time.Duration) Option {
	return func(v *Invoker) {
		if d > 0 {
			v.hc.Timeout = d
		}
	}
}

// WithBodyCap 覆盖响应体读取上限（绑定引擎：cfg.Fetch.MaxResponseMB）。
// 读取按 cap+1 截止，调用方可据 len(Body)>cap 显式判超限而非静默截断。
func WithBodyCap(n int64) Option {
	return func(v *Invoker) {
		if n > 0 {
			v.bodyCap = n
		}
	}
}

// New 构造调用器，复用抓取配置里的 TikHub key（同一账号同一计费池）。
func New(cfg config.FetchConfig, opts ...Option) *Invoker {
	inv := &Invoker{
		// 禁跟随重定向：与 fetcher 各抓取器一致，防 Bearer key 被 30x 外带到别的主机
		//（2026-07-18 绑定引擎迁移时补齐——lookup 面此前缺这道防线，同样受益）。
		hc: &http.Client{Timeout: requestTimeout, CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}},
		baseURL: defaultBaseURL,
		apiKey:  cfg.TikhubAPIKey,
		bodyCap: maxBodyBytes,
	}
	for _, o := range opts {
		o(inv)
	}
	return inv
}

// Result 一次端点调用的结果。Body 是上游响应原文（读到 maxBodyBytes 截止）。
type Result struct {
	Status     int
	Body       []byte
	DurationMs int
}

// Invoke 调用一个注册表端点。params 是模型产出的平铺参数（已经 agent 侧校验）。
//
// 错误分层：参数装配失败/未配置 key → CodeValidation（确定性，模型或运维可修）；
// 网络/超时 → CodeUpstream。上游非 2xx **不是 error**——状态码带在 Result 里由
// 调用方决定怎么回给模型（4xx 多半是参数语义错误，模型看得懂原文才能自纠）。
func (v *Invoker) Invoke(ctx context.Context, entry tikhubcatalog.Entry, params map[string]any) (*Result, error) {
	if v.apiKey == "" {
		return nil, types.NewAppError(types.CodeValidation,
			"TikHub API key 未配置（fetch.tikhub_api_key），端点调用不可用", nil)
	}

	req, err := v.buildRequest(ctx, entry, params)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	resp, err := v.hc.Do(req)
	if err != nil {
		// 超时（ctx 到期或 client.Timeout）用抓取细分码，其余传输层失败归 Internal
		// ——刻意不为 lookup 面新增错误码：非 2xx 都不走 error 通道（见 Result），
		// 真正到这里的只剩网络层病态，两类码已够分诊。
		if errors.Is(err, context.DeadlineExceeded) || isClientTimeout(err) {
			return nil, types.NewAppError(types.CodeFetchTimeout,
				fmt.Sprintf("TikHub 端点 %s 调用超时", entry.Name), err)
		}
		return nil, types.NewAppError(types.CodeInternal,
			fmt.Sprintf("TikHub 端点 %s 调用失败", entry.Name), err)
	}
	defer resp.Body.Close()

	cap := v.bodyCap
	if cap <= 0 {
		cap = maxBodyBytes // 零值兜底：结构体直构（含旧测试）不经 New 的默认赋值
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, cap+1))
	if err != nil {
		return nil, types.NewAppError(types.CodeInternal,
			fmt.Sprintf("TikHub 端点 %s 响应读取失败", entry.Name), err)
	}
	return &Result{
		Status:     resp.StatusCode,
		Body:       body,
		DurationMs: int(time.Since(start).Milliseconds()),
	}, nil
}

// buildRequest 按参数位置装配请求：path 参数替换进 URL、query 参数进查询串、
// body 参数进 JSON 请求体。未在 params 里出现的可选参数不发送（让上游用自己的
// 默认值，而不是我们猜一份默认值快照——上游改默认值时快照会静默漂移）。
func (v *Invoker) buildRequest(ctx context.Context, entry tikhubcatalog.Entry, params map[string]any) (*http.Request, error) {
	path := entry.Path
	query := url.Values{}
	body := map[string]any{}

	for _, p := range entry.Params {
		val, ok := params[p.Name]
		if !ok {
			continue // 必填缺失在 agent 侧已拦，这里不重复校验
		}
		switch p.In {
		case "path":
			path = strings.ReplaceAll(path, "{"+p.Name+"}", url.PathEscape(toString(val)))
		case "query":
			// 数组参数按重复键展开（FastAPI 的 query 数组标准形态）。
			if arr, isArr := val.([]any); isArr {
				for _, item := range arr {
					query.Add(p.Name, toString(item))
				}
			} else {
				query.Set(p.Name, toString(val))
			}
		case "body":
			body[p.Name] = val
		}
	}

	u := v.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	var reqBody io.Reader
	if entry.Method == http.MethodPost {
		// POST 恒带 JSON body（可为 {}）：上游是 FastAPI，声明了 requestBody 的端点
		// 对空 body 返回 422，空对象才是"全用默认值"的表达。
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, types.NewAppError(types.CodeValidation, "端点参数无法序列化为 JSON", err)
		}
		reqBody = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, entry.Method, u, reqBody)
	if err != nil {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("构造 TikHub 请求失败（%s）", entry.Name), err)
	}
	req.Header.Set("Authorization", "Bearer "+v.apiKey)
	req.Header.Set("Accept", "application/json")
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

// isClientTimeout 识别 http.Client.Timeout 触发的超时（错误链上是 *url.Error
// 且 Timeout()==true，不是 context.DeadlineExceeded）。
func isClientTimeout(err error) bool {
	var ue interface{ Timeout() bool }
	return errors.As(err, &ue) && ue.Timeout()
}

// toString 把模型产出的标量参数转为字符串。
//
// json.Number（agent 侧用 UseNumber 解析，对抗审查 HIGH 缺陷）原样透传其十进制串：
// 社媒雪花 ID（如 TikTok uid ~6.8e18 > 2^53）经 float64 会丢精度查错对象，保原串是
// 唯一正确解。float64 分支保留兜底（非 agent 调用方或未过 UseNumber 的路径）：
// 整数值去小数点（上游 id/page 收到 "1.000000" 会解析失败），真小数保留。
func toString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	case float64:
		if t == float64(int64(t)) {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	case nil:
		return ""
	default:
		raw, _ := json.Marshal(t)
		return string(raw)
	}
}

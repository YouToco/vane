// 运行统计只读端点（M7 功能 6.5）：成本与运行监控页的取数面。
// 只读（不写表、不调模型）；挂 /api/ 前缀自动继承会话中间件（单用户阶段
// Dashboard 密码即 owner 凭证，理由见 observability.go 头注释）。
//
// 与 /api/admin/observability 的分工：那边跑探针判定（红线视角），这边纯展示
// 运行指标（趋势视角）——6.5 页面不需要每次刷新都把 7 条探针跑一遍。
package api

import (
	"net/http"
	"time"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

// runstatsResp 是 GET /api/admin/runstats 的响应体。
// days/models 复用探针 ⑥ 的两个既有聚合（同一份 SQL，不做平行实现）。
type runstatsResp struct {
	GeneratedAt time.Time           `json:"generated_at"` // UTC
	WindowHours int                 `json:"window_hours"`
	Spans       []store.SpanRunStat `json:"spans"`
	Days        []types.SpanDayCost `json:"days"`
	Models      []types.ModelUsage  `json:"models"`
}

// handleRunstats 返回窗口内的运行统计。
// GET /api/admin/runstats?window_hours=24 → 200 runstatsResp
//
// 不解析 owner：llm_calls 的成本/延迟统计是系统级的（含无 user_id 的行），
// 单用户阶段全库即 owner 视角；窗口校验复用 parseWindowHours（1h–30 天）。
func (s *server) handleRunstats(w http.ResponseWriter, r *http.Request) {
	if !s.requirePlatformOwner(w, r) {
		return
	}
	window, errMsg := parseWindowHours(r.URL.Query())
	if errMsg != "" {
		writeError(w, http.StatusBadRequest, errMsg)
		return
	}

	now := time.Now().UTC()
	since := now.Add(-window)

	spans, err := s.deps.Store.ListSpanRunStats(r.Context(), since)
	if err != nil {
		writeAppError(w, err)
		return
	}
	days, err := s.deps.Store.ListSpanDayCosts(r.Context(), since)
	if err != nil {
		writeAppError(w, err)
		return
	}
	models, err := s.deps.Store.ListModelUsage(r.Context(), since)
	if err != nil {
		writeAppError(w, err)
		return
	}

	if spans == nil {
		spans = []store.SpanRunStat{}
	}
	if days == nil {
		days = []types.SpanDayCost{}
	}
	if models == nil {
		models = []types.ModelUsage{}
	}
	writeJSON(w, http.StatusOK, runstatsResp{
		GeneratedAt: now,
		WindowHours: int(window.Hours()),
		Spans:       spans,
		Days:        days,
		Models:      models,
	})
}

// Gate 探针的只读看板端点（M5 契约 §16）：一次跑完 7 条判定并回全部原始指标。
//
// 为什么挂在 /api/ 下而不是自立门户的 /admin/*：api.go 末尾把整个 /api/ 前缀
// 交给 requireSession，挂进来就自动继承会话中间件，零新增鉴权面。当前是单用户
// 飞书自用阶段，Dashboard 密码即管理员凭证——能登录的就是 owner 本人，再造一套
// admin 角色/令牌是给一个人的系统加一层空转。路径里的 admin 只是语义分组，
// 不代表另一重身份；将来真要区分角色，鉴权收口在中间件一处，改那里即可。
//
// 端点只读（probe 包不写表、不调模型），故 GET 语义完整、无写副作用。
package api

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/YouToco/vane/probe"
)

// window_hours 的合法区间。
//
// 下界 1h：再小连一个打分批次都框不住，探针只会全 yellow，纯属误导。
// 上界 30 天：llm_calls 无 TTL、只增不减（AGENTS.md 红线 5 数据不清理），
// 窗口不封顶等于放任按时间线性变慢的全表聚合；且契约 §16 的红线本就以 24h
// 为准（probe.DefaultWindow），更长的窗口是看趋势而非判定，30 天足够。
const (
	minWindowHours = 1
	maxWindowHours = 24 * 30
)

// parseWindowHours 把 query 里的 window_hours 解析成统计窗口，缺省为 probe.DefaultWindow。
//
// 返回 (窗口, 人话错误)：与 fetchspec.BuildTarget 同形——
// 参数校验的失败是"用户填错了"，不是链路故障，造 AppError 再映射回 400 是绕远路。
//
// 空值（?window_hours=）按缺省处理而非报错：前端输入框清空后拼出来的就是这个形状，
// 用户意思是"用默认窗口"，回 400 只会莫名其妙。
//
// 拆成独立函数是为了可测：handler 后半段要 *store.Store，起不了 DB 就测不了，
// 而参数校验是本端点唯一有分支的逻辑，必须能单独钉死（见 observability_test.go）。
func parseWindowHours(q url.Values) (time.Duration, string) {
	raw := q.Get("window_hours")
	if raw == "" {
		return probe.DefaultWindow, ""
	}
	h, err := strconv.Atoi(raw)
	if err != nil || h < minWindowHours || h > maxWindowHours {
		return 0, fmt.Sprintf("window_hours 必须是 %d 到 %d 之间的整数（小时）",
			minWindowHours, maxWindowHours)
	}
	return time.Duration(h) * time.Hour, ""
}

// handleObservability 跑一次 Gate 探针全套体检。
// GET /api/admin/observability?window_hours=24 → 200 probe.Report
//
// 校验参数在解析 owner 之前：越界的 window_hours 无论有没有 owner 都是 400，
// 先查 owner 只会让参数错误偶尔伪装成 409。
func (s *server) handleObservability(w http.ResponseWriter, r *http.Request) {
	if !s.requirePlatformOwner(w, r) {
		return
	}
	window, errMsg := parseWindowHours(r.URL.Query())
	if errMsg != "" {
		writeError(w, http.StatusBadRequest, errMsg)
		return
	}

	// 单 owner 模型：探针 ④⑤⑦ 的画像/演化判定都以 owner 为对象。
	// 无 owner 时 ownerUserID 回 CodeConflict → writeAppError 映射成 409，
	// 语义是"飞书向导还没走完"而非故障，与 M3 端点一致，不在这里另造。
	userID, err := s.ownerUserID(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}

	// now 显式取 UTC：探针内部一律 UTC，换算只在前端（红线 6 的三时区陷阱）。
	rep, err := probe.Run(r.Context(), s.deps.Store, userID, time.Now().UTC(), window)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

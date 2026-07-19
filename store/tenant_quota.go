package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// ============================================================
// per-tenant 配额（契约 §2.7）
// ============================================================
//
// D3 下平台垫付全部第三方 API 成本，于是每个租户的调用量直接等于平台的账单。
// D4（邀请制）用"发出的邀请数"给敞口封顶是第一道；本文件是第二道——
// **被邀请进来的人也可能把额度跑光**，无论有意还是无意（一个配错的高频信源就够了）。
//
// ---- 与既有 TikHub 日限额的关系（两套机制，刻意并存）----
//
// store/toolcalls.go 的 CountTikHubEndpointCallsSince 是"数账本"式限额，它有个
// 本文件比不了的优点，其注释说得很好：**限额判定读的就是记账表，记账与限额同源，
// 不会出现「拦截口径与账本对不上」**。
//
// token bucket 是独立状态，天然有漂移风险：记账写失败、或有人手改 tenant_quota，
// 桶与账本就对不上了。接受这个代价换来的是：O(1)（不必在增长的表上扫窗口）、
// 且能表达"速率 + 突发"而不只是"窗口内计数"。
//
// 两者职责不同，不合并：日限额管"一天最多多少次"，桶管"多快"。

// QuotaBucket 是配额桶的名字。分两类，因为超限的含义不同。
type QuotaBucket string

const (
	// ---- 财务面：超限意味着"再花就超预算"，硬拦 ----

	// QuotaLLMTokens 按 token 计（LLM 是按 token 计费的，按次数计会严重失真：
	// 一次深挖的 token 量可能是一次打分的上百倍）。
	QuotaLLMTokens QuotaBucket = "llm_tokens"
	// QuotaTikHubCalls TikHub 按次计费。
	QuotaTikHubCalls QuotaBucket = "tikhub_calls"
	// QuotaExaCalls Exa 按次计费（搜索 + 取正文）。
	// **契约 §2.7 的桶清单漏了这一项**——真正按次计费的是 LLM / TikHub / Exa 三家，
	// 而 Exa 恰恰是 RSS 正文补全上线后增长最快的那一项（2026-07-19 Boss 纠正）。
	QuotaExaCalls QuotaBucket = "exa_calls"

	// ---- DoS 面：超限意味着"这个租户太吵"，拦的是资源占用而非钱 ----

	// QuotaPush 推送次数。
	QuotaPush QuotaBucket = "push"
	// QuotaFetch 抓取轮次。
	QuotaFetch QuotaBucket = "fetch"
)

// financialBuckets 是"超限即超预算"的那几个。单列出来供调用方判断失败语义，
// 也供守卫测试断言"新增桶必须显式归类"。
var financialBuckets = map[QuotaBucket]bool{
	QuotaLLMTokens:   true,
	QuotaTikHubCalls: true,
	QuotaExaCalls:    true,
}

// IsFinancial 报告该桶是否属于财务面。
func (b QuotaBucket) IsFinancial() bool { return financialBuckets[b] }

// ErrQuotaExceeded 是配额不足。**刻意是哨兵错误而非 AppError**：
// 调用方需要能 errors.Is 判定并走各自的降级路径（打分跳过、抓取推迟、推送延后），
// 而不是把它当成一般数据库错误一路上抛。
var ErrQuotaExceeded = errors.New("配额不足")

// TryConsume 原子地从桶里取 n 个令牌，取不到返回 ErrQuotaExceeded。
//
// 补充与取用在**同一条 UPDATE** 里完成，这是整个设计的关键：
// 「先 SELECT 看够不够、再 UPDATE 扣减」在并发下必然超发——两个请求同时看到余额充足，
// 各自扣减，结果透支。而在单条语句里，Postgres 的行锁保证了判定与扣减不可分割。
//
// 补充量按 updated_at 到 now() 的**实际经过秒数**算，所以不需要任何后台补充任务：
// 桶在被读取的那一刻才"长出"令牌。没人用的桶不消耗任何资源。
//
// LEAST(burst, ...) 封顶：长期不用的桶不会攒出天量令牌，否则一个沉寂三个月的租户
// 一朝醒来就能瞬间打满上游——那正是 burst 要防的事。
func (s *Store) TryConsume(ctx context.Context, tenantID int64, bucket QuotaBucket, n float64) error {
	if n <= 0 {
		return nil // 取 0 或负数是调用方 bug，但不该因此拦下业务；当作无操作。
	}
	var remaining float64
	err := s.pool.QueryRow(ctx,
		`UPDATE tenant_quota
		    SET tokens = LEAST(burst, tokens + rate * EXTRACT(EPOCH FROM (now() - updated_at))) - $3,
		        updated_at = now()
		  WHERE tenant_id = $1 AND bucket = $2
		    AND LEAST(burst, tokens + rate * EXTRACT(EPOCH FROM (now() - updated_at))) >= $3
		RETURNING tokens`,
		tenantID, string(bucket), n).Scan(&remaining)
	if errors.Is(err, pgx.ErrNoRows) {
		// 两种情况都走这里，且**都必须拒绝**：
		//   - 余额不足（WHERE 的第二个条件没过）
		//   - 桶不存在（租户没被 seed 过）
		// 后者尤其重要：没有配额行 = 没有额度，而不是"无限额度"。
		// 反过来（缺行即放行）会让"忘了 seed"变成一个静默的无限额度洞——
		// 而它恰恰最可能发生在新租户身上，也就是最需要设防的那一刻。
		return ErrQuotaExceeded
	}
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("扣减配额（tenant=%d bucket=%s）", tenantID, bucket), err)
	}
	return nil
}

// QuotaDefault 是一个桶的初始参数。
type QuotaDefault struct {
	Bucket QuotaBucket
	Rate   float64 // 每秒补充
	Burst  float64 // 容量
}

// defaultQuotas 是新租户的初始额度。
//
// 取值依据（2026-07-19 生产实测，不是拍脑袋）：
//   - 单次推送约 45 条打分 + 8 张卡，LLM 用量约 4 万 token 量级；每天一轮定时
//     加若干次手动，给 200 万 token/天（≈23 tok/s）留足余量而不至于失控。
//   - Exa：单次搜索 $0.007–0.012、单次取正文 $0.001。500 次/天约 $1–3，
//     对应一个正常使用强度的租户；跑满一个月约 $30–90，是可承受的单租户上限。
//   - TikHub 沿用既有日限额量级。
//   - push / fetch 是 DoS 面，按"正常使用不可能触到"设。
//
// burst 一律取"约一天的量"：让偶发的密集使用（比如一次性配了十几个源）能跑完，
// 而不是在正常操作中途被拦——被拦的体验是"系统坏了"，而不是"我超额了"。
var defaultQuotas = []QuotaDefault{
	{QuotaLLMTokens, 2_000_000.0 / 86400, 2_000_000},
	{QuotaExaCalls, 500.0 / 86400, 500},
	{QuotaTikHubCalls, 500.0 / 86400, 500},
	{QuotaPush, 200.0 / 86400, 200},
	{QuotaFetch, 2000.0 / 86400, 2000},
}

// SeedTenantQuota 给租户建初始配额桶。幂等：已存在的桶不动
// （否则重复调用会把用掉的额度刷回满格，配额形同虚设）。
//
// 必须在租户创建时调用——没有配额行的租户会被 TryConsume 一律拒绝，
// 那是刻意的失败方向（见 TryConsume 的说明），但对新租户来说表现为"什么都用不了"。
func (s *Store) SeedTenantQuota(ctx context.Context, tenantID int64) error {
	for _, q := range defaultQuotas {
		if _, err := s.pool.Exec(ctx,
			`INSERT INTO tenant_quota (tenant_id, bucket, tokens, rate, burst)
			 VALUES ($1, $2, $3, $4, $5)
			 ON CONFLICT (tenant_id, bucket) DO NOTHING`,
			tenantID, string(q.Bucket), q.Burst, q.Rate, q.Burst); err != nil {
			return types.NewAppError(types.CodeDatabase,
				fmt.Sprintf("初始化配额桶（tenant=%d bucket=%s）", tenantID, q.Bucket), err)
		}
	}
	return nil
}

// QuotaStatus 是一个桶的当前状态（只读，供展示与排查）。
type QuotaStatus struct {
	Bucket     QuotaBucket
	Tokens     float64 // 已按经过时间补充后的可用量
	Burst      float64
	RatePerDay float64
}

// ListQuota 返回租户各桶的当前额度。
// tokens 按读取时刻补充后计算，与 TryConsume 的判据一致——否则展示出来的数字
// 会和实际能不能用对不上，排查时反而添乱。
func (s *Store) ListQuota(ctx context.Context, tenantID int64) ([]QuotaStatus, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT bucket,
		        LEAST(burst, tokens + rate * EXTRACT(EPOCH FROM (now() - updated_at))),
		        burst, rate * 86400
		   FROM tenant_quota WHERE tenant_id = $1 ORDER BY bucket`, tenantID)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "查询配额", err)
	}
	defer rows.Close()
	var out []QuotaStatus
	for rows.Next() {
		var st QuotaStatus
		var b string
		if err := rows.Scan(&b, &st.Tokens, &st.Burst, &st.RatePerDay); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描配额", err)
		}
		st.Bucket = QuotaBucket(b)
		out = append(out, st)
	}
	return out, rows.Err()
}

package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

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
	// QuotaOfficialCalls bounds credentialless first-party reads for DoS
	// control. It is not a financial bucket and never represents provider cost.
	QuotaOfficialCalls QuotaBucket = "official_calls"
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

// enforcedBuckets 记录**每个桶实际在哪里被扣减**。空串 = 尚未接线：
// 桶已建好、有余额、能查询，但没有任何代码去扣它，因此它拦不住任何东西。
//
// 单列这张表的唯一理由是：**半接的配额比完全不接更危险**——桶存在、
// ListQuota 查得到数字、看起来一切就绪，于是没人会再去想"Exa 到底受不受限"。
// 把未接线写成代码里的显式空串，比写在 PR 描述里可靠：PR 会沉底，代码不会。
//
// 当前接线状态：
//   - exa_calls：V3 research step 在 Store 内把配额扣减、started step 和
//     不可变 spend reservation 原子提交；重放不会再次放行供应商请求。
//   - tikhub_calls：Fetcher.Fetch(ctx, src) 拿不到 user_id——
//     source 是跨租户共享的，"谁触发了这次抓取"这个信息在抓取层根本不存在。
//     要接线得先让抓取携带触发者身份，那是接缝②的活。
//   - push / fetch：DoS 面，不花钱，优先级低于财务面。
var enforcedBuckets = map[QuotaBucket]string{
	// 值是**包名**，不是自由文案：守卫测试拿它去源码里核实那个包真的调用了
	// TryConsumeForUser/TryConsume。前一版这里写的是一句人类可读的描述，
	// 于是守卫只能校验"字符串非空"——把空串改成任意一句话就能谎称已接线，
	// 而测试全绿（2026-07-19 审查实测）。自证式的守卫比没有守卫更糟，
	// 因为它让人以为这件事有人在看。
	QuotaLLMTokens:     "llm",
	QuotaExaCalls:      "store",
	QuotaTikHubCalls:   "",
	QuotaPush:          "",
	QuotaFetch:         "",
	QuotaOfficialCalls: "store",
}

// IsEnforced 报告该桶是否真的有代码在扣它。
func (b QuotaBucket) IsEnforced() bool { return enforcedBuckets[b] != "" }

// Valid reports whether b is a registered bucket, including buckets whose
// enforcement is deliberately not wired yet.
func (b QuotaBucket) Valid() bool {
	_, ok := enforcedBuckets[b]
	return ok
}

// ErrAmbiguousTenant 是"这个用户属于多个租户，无法判定该扣谁的额度"。
//
// **必须与一般数据库错误分开**，因为两者的正确处置相反：
//   - DB 抖动是暂时的 ⇒ 放行（让抖动升级成全局 LLM 停摆，比超支一点糟糕得多）
//   - 归属不明是确定性的 ⇒ 拒绝（重试一万次还是多行；而且此刻我们**根本不知道
//     该记谁的账**，花一笔无法归属的钱正是这道护栏存在的理由）
//
// tenantderive.go 的设计说明里写着，用户加入多个租户时子查询会"在正确的时刻
// 响亮失败"。若把它混进 CodeDatabase 走放行分支，那句承诺在花钱路径上就被消音了：
// 该用户获得无限额度，而现场只留下一行 WARN。
var ErrAmbiguousTenant = errors.New("用户归属多个租户，无法判定配额归属")

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
	// 缺行即拒绝（余额不足或桶不存在）：没有配额行 = 没有额度，而不是"无限额度"。
	// 反过来会让"忘了 seed"变成静默的无限额度洞——而它恰恰最可能发生在新租户身上，
	// 也就是最需要设防的那一刻。
	if err != nil {
		return classifyQuotaErr(err, fmt.Sprintf("扣减配额（tenant=%d bucket=%s）", tenantID, bucket))
	}
	return nil
}

// classifyQuotaErr 把底层错误翻译成配额层的语义。
//
// 单列出来是因为两条扣减路径（TryConsume / TryConsumeForUser）必须给出一致的
// 判定——判据分散在两处迟早漂移，而漂移的表现是"同一种故障在两条路径上一个
// 拒绝一个放行"，极难发现。
func classifyQuotaErr(err error, what string) error {
	if errors.Is(err, pgx.ErrNoRows) {
		// 两种情况都走这里，且都必须拒绝：余额不足、或桶不存在（租户没被 seed）。
		return ErrQuotaExceeded
	}
	// 21000 cardinality_violation：子查询返回多行 ⇒ 用户归属多个租户。
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "21000" {
		return ErrAmbiguousTenant
	}
	return types.NewAppError(types.CodeDatabase, what, err)
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
//   - Exa：500 次/天对应一个正常使用强度的租户。真实金额由
//     provider_price_rules 的当期版本计算，不在配额代码里冻结易过期单价。
//   - TikHub 沿用既有日限额量级。
//   - push / fetch 是 DoS 面，按"正常使用不可能触到"设。
//
// burst 一律取"约一天的量"：让偶发的密集使用（比如一次性配了十几个源）能跑完，
// 而不是在正常操作中途被拦——被拦的体验是"系统坏了"，而不是"我超额了"。
var defaultQuotas = []QuotaDefault{
	{QuotaLLMTokens, 2_000_000.0 / 86400, 2_000_000},
	{QuotaExaCalls, 500.0 / 86400, 500},
	{QuotaOfficialCalls, 500.0 / 86400, 500},
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

// TryConsumeForUser 按 user_id 扣减配额——租户在 SQL 里推导，调用方不必知道 tenant。
//
// 这是给 llm / fetcher 这些**拿不到 tenant_id 的层**准备的。它们只知道 userID，
// 而把 tenantID 一路穿下去要改一串签名与替身。记账路径（InsertLLMCall）早就用
// 同一个 tenantOfUser 子查询解决了这个问题，配额沿用它，两处口径也因此天然一致。
//
// 用户不属于任何租户时子查询返回 NULL，WHERE 不成立 ⇒ 拒绝。与"桶不存在即拒绝"
// 同一个失败方向：**没有归属就没有额度**，而不是"没有归属就不受限"。
func (s *Store) TryConsumeForUser(ctx context.Context, userID int64, bucket QuotaBucket, n float64) error {
	if n <= 0 {
		return nil
	}
	var remaining float64
	err := s.pool.QueryRow(ctx,
		`UPDATE tenant_quota
		    SET tokens = LEAST(burst, tokens + rate * EXTRACT(EPOCH FROM (now() - updated_at))) - $3,
		        updated_at = now()
		  WHERE tenant_id = `+tenantOfUser+`$1)
		    AND bucket = $2
		    AND LEAST(burst, tokens + rate * EXTRACT(EPOCH FROM (now() - updated_at))) >= $3
		RETURNING tokens`,
		userID, string(bucket), n).Scan(&remaining)
	if err != nil {
		return classifyQuotaErr(err, fmt.Sprintf("扣减配额（user=%d bucket=%s）", userID, bucket))
	}
	return nil
}

// AdjustForUser 按 delta **无条件**调整余额（正数=退还，负数=补扣），允许扣成负数。
//
// 这是"事前预扣估算、事后对账实际"里的事后那一半，与 TryConsumeForUser 配对使用。
// 三个设计点，每一个都是从一种具体的失效方式反推出来的：
//
//  1. **无条件**（WHERE 里没有余额判据）。带判据的版本在余额不足时整条 UPDATE
//     不匹配任何行，于是超出的用量被永久丢弃——桶显示还有余额，钱却已经花了。
//     2026-07-19 审查实测：桶余额一旦低于单次用量，事后扣减每次都失败，
//     只有事前那点预扣生效，而补充速率反超消耗速率，**桶不降反升**，
//     放行 4.9 倍日额度且无上界。
//  2. **允许为负**（迁移 026 刻意不加 tokens >= 0）。负数就是欠账。欠账被如实
//     记下，下一次事前预扣就过不了，直到时间把余额补回正数——护栏因此自愈。
//  3. **不封顶到 burst**。退还（delta>0）时若封顶，估算偏高的那部分就退不回来，
//     长期使用会把桶系统性地压低。封顶只属于"按时间补充"，不属于对账。
//
// 桶不存在时静默无操作：这条路径跑在调用**之后**，此时拦截已无意义，
// 而报错只会把一次成功的调用变成失败。缺行的拦截职责在事前那一半。
func (s *Store) AdjustForUser(ctx context.Context, userID int64, bucket QuotaBucket, delta float64) error {
	if delta == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE tenant_quota
		    SET tokens = LEAST(burst, tokens + rate * EXTRACT(EPOCH FROM (now() - updated_at))) + $3,
		        updated_at = now()
		  WHERE tenant_id = `+tenantOfUser+`$1)
		    AND bucket = $2`,
		userID, string(bucket), delta)
	if err != nil {
		return classifyQuotaErr(err, fmt.Sprintf("对账配额（user=%d bucket=%s）", userID, bucket))
	}
	return nil
}

// TestQuota_DebtIsAllowedBySchema 在 store 侧单独钉住"表允许负余额"这条 schema 性质。
//
// 为什么它值得一条独立守卫：ReconcileQuota 把对账失败降级成一行 Warn（刻意的——
// 调用已经发生，报错只会把成功的回复变成失败）。于是**如果表上还有 tokens >= 0
// 的 CHECK，负债写入会被约束拒绝、被 Warn 吞掉，整个欠账机制静默失效**，
// 而上层行为测试只会看到"余额没变负"，很容易被误判成代码逻辑写错。
//
// 2026-07-19 实测过这个误导：本地库因迁移已应用而保留着旧 CHECK，行为测试变红，
// 现象与"AdjustForUser 写错了"完全一致。这条守卫直接对着 schema 断言，一眼分清。

// ReconcileTenantQuota 给所有缺配额行的租户补齐默认额度，返回补齐的租户数。
//
// 为什么需要它（而不是只靠建租户时 seed 一次）：
//
//  1. **seed 失败是静默的**。CreateTenantWithInvite / RegisterWithPassword 都把
//     SeedTenantQuota 的错误降级成一行日志——那是对的，把它塞进事务会让一次
//     seed 失败升级成整个注册失败。代价是用户注册"成功"却什么都用不了，
//     而且他和管理员都看不出为什么。启动 reconcile 把这个代价收了回来。
//  2. **迁移可能漏回填**。026 第一版就漏了：只 CREATE TABLE，没给 018 建的存量
//     租户（生产租户本人）建行，配合"缺行即拒绝"上线即锁死推送，而且下游把
//     额度用尽当正常终态处理——Temporal 一片绿、零告警。回填已补，
//     但"下一个迁移会不会再漏"不该靠记性。
//
// 与本仓 scheduler.ReconcileActions 同一个模式：启动时把存量对象补齐到当前代码
// 期望的形状，幂等且 best-effort。
//
// 刻意**不做懒加载**（在 TryConsume 里发现缺行就补）：那会让"缺行即拒绝"这条
// 失败方向失效——而它防的是"新增了桶却忘了 seed"变成静默的无限额度洞。
// 启动时补齐是一次可观测的批量动作，懒加载是每次调用都可能悄悄放行。
func (s *Store) ReconcileTenantQuota(ctx context.Context) (int, error) {
	buckets := make([]string, 0, len(defaultQuotas))
	for _, quota := range defaultQuotas {
		buckets = append(buckets, string(quota.Bucket))
	}
	rows, err := s.pool.Query(ctx,
		`SELECT t.id FROM tenants t
		  WHERE t.status <> 'deleting'
		    AND EXISTS (
		      SELECT 1 FROM unnest($1::text[]) expected(bucket)
		       WHERE NOT EXISTS (SELECT 1 FROM tenant_quota q
		                          WHERE q.tenant_id=t.id AND q.bucket=expected.bucket)
		    )
		  ORDER BY t.id`, buckets)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "查询缺配额的租户", err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, types.NewAppError(types.CodeDatabase, "扫描缺配额的租户", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "遍历缺配额的租户", err)
	}

	n := 0
	for _, id := range ids {
		if err := s.SeedTenantQuota(ctx, id); err != nil {
			// best-effort：一个租户补不上不该挡住其余租户，更不该挡住服务启动。
			slog.Error("补齐租户配额失败", "tenant_id", id, "err", err)
			continue
		}
		n++
	}
	return n, nil
}

// SetQuota 调整某个桶的每日额度（同时改速率与容量），余额按比例保留。
//
// 按比例而非重置：管理员调额度时，"已经用掉多少"这个事实不该被抹掉——
// 重置成满格会让"调高额度"变成一条绕过配额的免费通道（用完了就调一下）。
func (s *Store) SetQuota(ctx context.Context, tenantID int64, bucket QuotaBucket, perDay float64) error {
	if perDay < 0 {
		return types.NewAppError(types.CodeValidation, "每日额度不能为负", nil)
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE tenant_quota
		    SET tokens = CASE WHEN burst > 0 THEN LEAST($3, tokens / burst * $3) ELSE $3 END,
		        rate   = $3 / 86400,
		        burst  = $3,
		        updated_at = now()
		  WHERE tenant_id = $1 AND bucket = $2`,
		tenantID, string(bucket), perDay)
	if err != nil {
		return classifyQuotaErr(err, fmt.Sprintf("设置配额（tenant=%d bucket=%s）", tenantID, bucket))
	}
	if tag.RowsAffected() == 0 {
		return types.NewAppError(types.CodeNotFound,
			fmt.Sprintf("租户 %d 没有 %s 桶", tenantID, bucket), nil)
	}
	return nil
}

// RefillQuota 把余额直接补满到桶容量。救援用：额度用尽时立刻恢复服务，
// 不必等按秒补充，也不必为了救急而永久调高额度。
func (s *Store) RefillQuota(ctx context.Context, tenantID int64, bucket QuotaBucket) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE tenant_quota SET tokens = burst, updated_at = now()
		  WHERE tenant_id = $1 AND bucket = $2`,
		tenantID, string(bucket))
	if err != nil {
		return classifyQuotaErr(err, fmt.Sprintf("补满配额（tenant=%d bucket=%s）", tenantID, bucket))
	}
	if tag.RowsAffected() == 0 {
		return types.NewAppError(types.CodeNotFound,
			fmt.Sprintf("租户 %d 没有 %s 桶", tenantID, bucket), nil)
	}
	return nil
}

# 企业级三接缝契约 — 多租户 / 多推送渠道 / 信源 SDK

> 草案 2026-07-18（Mac 端）。**本文是接缝定义与迁移路径的契约,不是排期计划**。
> 依据:业内调研 5 路（OpenClaw 源码实读 + Bot Framework/Apprise/Courier/Novu/HumanLayer
> 渠道抽象 + Postgres RLS/pgx 多租户 + Airbyte/Singer/Steampipe 连接器 SDK）+ vane 现状
> file:line 级耦合面测绘。关键论断均带一手来源，见 §附。
> **形态与九项决议已由 Boss 拍板（§0.1），本文按决议定稿。**

## §0 定位

### §0.1 已拍板决议（2026-07-18，全部为设计前提，改动须先改本节）

| # | 决议 | 直接后果 |
|---|---|---|
| D1 | **形态 = 真 SaaS，陌生人自助注册** | RLS 必做（非可选）；凭证隔离必做；租户不可信 |
| D2 | ~~认证 = 商用注册 + 国内外社交（X/Facebook/Google/微信/QQ）~~ **已推翻，见 D2′** | — |
| **D2′** | **认证 = 邮箱 + 密码起步，社交登录全部推后**（2026-07-18 技术验证后修订） | 大幅简化接缝①；**但引入发信服务依赖**（见 §1.1）。principal 接口形状不变，将来接社交/托管 IdP 上层零改动 |
| **D10** | **大陆线暂不启动**：境外线跑通再说，不办个体工商户 | 微信/QQ 登录、大陆收费、AI 备案全部推后；服务器留在境外 VPS，不做 ICP 备案 |
| D3 | **第三方 API 成本 = 平台全垫付** | 配额从「防 DoS」升级为**财务护栏**，硬配额 + 成本归集是必做项 |
| D4 | **准入 = 邀请码 / waitlist 制** | D3 的对冲：财务敞口由发出的邀请数封顶 |
| D5 | **飞书 = 仅租户自建应用**（各租户填自己的 app_id/secret） | 无需过飞书审核、限流各自独立、数据不过平台应用；**代价:Manager 从单例单 WS 改为 per-tenant 连接池** |
| D6 | **I-T1 修订 = 公开源共享 + 私有源隔离** | 保住全局去重与 TikHub 付费闸门，同时堵住带凭证源的跨租户泄漏 |
| D7 | **Channel 接口现在就立**（不等第二个渠道） | 与 D5 的 Manager 改造、接缝② 的 users/deliveries 迁移**一次做完**，避免二次迁移 |
| D8 | **租户模型 = 混合**:现在建 `tenants` + `memberships`，初期每租户恒 1 人 | 多一张表的成本，换以后做团队时不必再经历一次改造性迁移 |
| D9 | **注销 = 软删除 + 30 天保留期后硬删** | 修订 007「数据一律不清理」；**共享表豁免**（见 §2.6） |

### §0.1.1 D2 为什么被推翻（2026-07-18 技术验证结论，含一手实测）

**Casdoor 本身验证通过**（本机实跑 v3.119.0）：40MB RSS / 10MB DB、复用现有 Postgres 自动建 44 表、
1 秒就绪、**标准 `coreos/go-oidc` 直接可接（无需私有 SDK）**、完整授权码流程 + JWKS 验签全通；
且源码级确认它是自托管 IdP 里唯一把微信/QQ/微博/钉钉/飞书全做成一等公民的。**技术上没有问题。**

**推翻 D2 的是资质墙，不是 Casdoor**（换任何 IdP——Auth0/Authing/Logto/腾讯云——一视同仁）：

1. **微信/QQ 登录：个人主体 100% 做不到。** 微信开放平台开发者资质认证只接受组织主体
   （企业/个体工商户，300 元/年），且要求域名 ICP 备案、**备案主体 = 认证主体**。
   替代路径全部不成立：公众号网页授权仅限认证服务号（同样要组织主体）、个人小程序做不了
   Web 扫码、第三方代认证属主体挂靠违规。QQ 互联「免备案海外站」是陷阱——
   **未过审的 appid 只有开发者本人能登录**，陌生人注册场景不可用。
2. **商用 IdP 厂商没有捷径。** 逐个查证 Authing/腾讯云 CIAM/阿里云 IDaaS/易盾/极光/MobTech，
   **没有任何一家提供微信登录的「代申请」或「共享主体」**——厂商只是协议适配层，
   不是资质承载方，集成文档一律要求填入你自己的微信 AppID。
3. **向大陆用户收费需 ICP 经营许可证**（大陆企业 + 注册资本 100 万，个体工商户一般不受理）
   ——这直接冲击 D3（平台垫付要可持续就得收费）。
4. **AI 备案**：LLM 产品面向境内公众需算法备案，网信办已备案主体清一色是企业。
5. **架构级冲突**：ICP 备案要求境内服务器，而 vane 在境外 VPS。

**顺带的事实**：Google/Facebook/X 在大陆被墙、微信/QQ 境外用户不装——
「国内外社交登录都要」在单一部署下本就意义有限，它实质是两套产品。

**未来解锁路径（D10 推后，非放弃）**：微信开放平台**接受个体工商户**，且无对公账户者
可用**电子营业执照**认证——线上可办、无注册资本要求。即「必须成立公司」实为「办个执照」，
**但它只解开微信/QQ 登录，解不开向大陆收费**。

**未拍板、按默认执行（可随时推翻）**:
- ~~Casdoor 自托管~~ 已随 D2′ 作废。邮箱+密码的**实现方式待定**：自建，或用托管 IdP
  免费档（Logto Cloud / Supabase Auth 的 Free 档本身就含邮箱+密码，含验证信与重置流，
  比自建更省事且 $0，并顺带保留社交登录的门）。**无论哪种，§1.1 的接口形状不变。**
- **本次迁移走短停机窗口**——当前真实用户仅 1 人（Boss），一次性迁移远比五步在线简单。
  §2.7 的五步在线法**保留为「已有真实租户后」的迁移范式**。
- Source SDK 走**棘轮式分批迁移**（新连接器强制走 SDK，老 fetcher 逐个搬）。
- 第一个新推送渠道**推后决定**——Channel 接口立起来后，加渠道是独立小事。

### §0.2 为什么是「接缝」而不是「功能」

多租户 / 多渠道 / 多信源是**横切**的:三者都碰 agent loop、store、feishu 包。
若按功能分工，两台机器会同时改同一批核心文件，必然陷入合并地狱。组织原则:

> **先把接缝定成 interface 并合进 main,再在 interface 背后并行。**
> 并行度 = 接缝的稳定度。接缝未合并前，任何扇出都是伪并行。

### §0.3 三接缝与依赖顺序

| 接缝 | 内容 | 依赖 |
|---|---|---|
| **① 认证与租户**（D1/D2′/D4/D8/D9） | 邮箱+密码认证、tenants/memberships、邀请码、注销 | **无（地基）** |
| **② 租户隔离**（D1/D3/D6） | tenant_id 贯穿、RLS、per-tenant 凭证与配额 | 依赖 ① |
| **③ Channel**（D5/D7） | 渠道抽象、能力协商、审批降级、飞书 per-tenant 连接池 | 依赖 ①②（凭证/身份挂租户） |
| **④ Source SDK** | 信源连接器声明式化 | 弱依赖 ②（配额归租户），可与 ③ 并行 |

## §1 接缝①:认证、租户与准入

### §1.1 认证:邮箱 + 密码（D2′），principal 收敛为唯一来源

**现状（测绘实证）**:`api/owner.go:20-49` 的 `ownerUserID` 从 `settings.feishu_owner`
读全局单行拿 principal，**与请求身份完全无关**；同一逻辑被逐字复述三份
（`a2a/chat.go:97-120`、`cmd/gate/main.go:182-216`，后者注释自认「与 api.ownerUserID 逐字一致」）；
Dashboard 只有一个共享密码（`api/auth.go:38`），无用户表参与鉴权。

**契约（与实现方式无关的不变量）**:

principal 解析收敛为**唯一一处**，无论认证由谁实现。**已落地**（`auth` 包，2026-07-18）:
```go
// package auth
type Principal struct { TenantID types.TenantID; UserID int64 }

type PrincipalResolver interface {                      // 接缝：api/a2a/gate 依赖它
    FromContext(ctx context.Context) (Principal, error)
}

func NewOwnerResolver(store OwnerStore, settingKey string) PrincipalResolver  // 过渡期实现
```
**为什么是接口方法而非契约原写的包级 `PrincipalFromContext`**:过渡期实现要查库
（owner 存在 settings 里），做不成无依赖的包级函数。真实认证落地后 principal 由认证中间件
注入 ctx、store 依赖消失，届时可退化为纯 ctx 读取——**接口形状不变，上层不受影响**。

`settingKey` 由装配方传入而非包内硬编码:**auth 不 import feishu**——地基层不该知道
任何推送渠道的存在（§3 要把飞书降为一个 channel），也让「owner 来自飞书设置」
显式成为一处过渡期装配细节。
三份复述全部删除。**这是零风险的第一刀**——过渡期该函数可先返回固定
`{1, ownerUserID}`，行为完全不变，但调用点从此拿的是「上下文里的 principal」
而非「全局 owner」。

> **不变量 I-A1**:vane 代码中**不得出现**除本函数以外的 principal 来源。
> 在此之前加任何 tenant_id 列都是装饰——列加了但 principal 仍是全局单例，隔离不成立。

> **不变量 I-A4（保住将来的路）**:认证实现细节**不得泄漏到 `PrincipalFromContext` 之外**。
> 上层（API handler / agent / a2a）只认 `Principal`，不认「密码」「token」「OIDC」任何概念。
> 满足此条时，将来从自建邮箱密码换成托管 IdP（或加社交登录）是**单点替换，上层零改动**。

**实现方式二选一（未定，见 §0.1）**:

| | 自建邮箱+密码 | 托管 IdP 免费档只开邮箱+密码 |
|---|---|---|
| 密码哈希/校验 | 自己写（argon2id） | 厂商 |
| **发信（验证信/重置信）** | **自己接 Resend/SES + 配 SPF/DKIM/DMARC + 盯送达率** | 厂商内置 |
| 会话 | 自建 session 表 | 自建 session 表（同） |
| 撞库/枚举防护 | 自己写（可复用 `api/ratelimit.go`） | 厂商 |
| 社交登录门 | 将来要从头接 | 免费留着（Logto 甚至原生带微信/飞书） |
| 成本 | $0 + 发信费 | $0（免费档） |

> **被低估的一条**:自建的真正成本不是密码哈希，是**发信**——验证与重置邮件的
> 送达率、反垃圾配置是长期运维负担。
> **D4 邀请制带来一个简化**:邀请码本身就是把关，**首版可不做邮箱验证**；
> 但密码重置绕不开发信。

### §1.2 租户模型（D8）与准入（D4）

```sql
CREATE TABLE tenants (
  id BIGSERIAL PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'active',        -- active | suspended | deleting
  plan   TEXT NOT NULL DEFAULT 'free',
  deleted_at TIMESTAMPTZ,                       -- D9 软删除标记
  purge_after TIMESTAMPTZ,                      -- D9 硬删期限（deleted_at + 30d）
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE memberships (                      -- D8：初期每租户恒 1 行
  tenant_id BIGINT NOT NULL REFERENCES tenants(id),
  user_id   BIGINT NOT NULL,
  role      TEXT   NOT NULL DEFAULT 'owner',    -- owner | admin | member（预留）
  PRIMARY KEY (tenant_id, user_id)
);

CREATE TABLE invites (                          -- D4：财务敞口的闸门
  code TEXT PRIMARY KEY, issued_by BIGINT, issued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at TIMESTAMPTZ, consumed_by_tenant BIGINT, consumed_at TIMESTAMPTZ,
  max_uses INT NOT NULL DEFAULT 1, used_count INT NOT NULL DEFAULT 0
);
```

**注册流**:认证成功（邮箱+密码校验通过）→ vane 校验邀请码（未消费/未过期/未超次）→
建 `tenants` 行 + `memberships` 行（role=owner）→ 消费邀请码。
**邀请码校验必须在建租户之前**，且消费是原子的（`UPDATE ... WHERE used_count < max_uses RETURNING`）。

> **不变量 I-A2**:无有效邀请码不得创建租户。这是 D3（平台全垫付）唯一的财务闸门——
> 绕过它等于把按次计费的 TikHub/LLM 敞口对公网开放。

### §1.3 注销与数据删除（D9，修订 007 决策）

- 注销 → `tenants.status='deleting'`、`deleted_at=now()`、`purge_after=now()+30d`，
  租户立即从产品面消失（登录被拒、调度停用、推送停发）。
- 保留期内可恢复（清空三字段）。
- 到期由定时任务硬删**该租户所有租户所有表**的行。

> **不变量 I-A3（红线）**:硬删**只删租户所有表**。
> `sources` / `content_items` / `content_sources` / `page_snapshots` 是**跨租户客观事实**，
> 不随单租户注销删除——否则会摧毁其他租户的去重结果与已付费的 TikHub 补全内容。
> 该租户与内容的**关联**（subscriptions/deliveries）被删除，内容本身留在共享池。

## §2 接缝②:租户隔离

### §2.1 表分级矩阵（写第一行迁移 SQL 前必须定死）

| 类别 | 表 | tenant_id | 理由 |
|---|---|---|---|
| **平台级** | `tenants`、`invites` | 不适用 | 租户本身 |
| **客观事实（共享）** | `sources`(public)、`content_items`、`content_sources`、`page_snapshots` | **不加** | 共享抓取是设计意图。`sources.url` 全局 UNIQUE（007:19-22）为「多用户加同源 → 重复抓取 + 重复付费」而设；`content_items.canonical_key` 全局 UNIQUE（007:158）承载跨源去重与 TikHub 付费闸门 |
| **租户所有** | `memberships`、`users`、`subscriptions`、`push_batches`、`deliveries`、`feedbacks`、`profiles`、`schedules`、`agent_sessions`、`pending_actions`、`llm_calls`、`tool_calls`、`a2a_tasks`、`tenant_credentials`、`tenant_quota`、`channel_identities` | **必须加** + RLS | |
| **必须重构** | `settings` | 结构改造 | 现为 `(key PRIMARY KEY, value JSONB)` 全局单行 KV（002:11-15），改 `PRIMARY KEY (tenant_id, key)` |

### §2.2 I-T1 修订:公开源共享 + 私有源隔离（D6）

**问题（D1 暴露）**:陌生人租户下，若 A 加了一个 URL 带私有 token 的 RSS，
`sources.url` 全局 UNIQUE 会让该 source 行被跨租户共享——B 加同一 URL 即可读到内容，
甚至能通过「插入报 duplicate key」侧信道探知其存在。

**契约**:给 `sources` 加可见性维度:
```sql
ALTER TABLE sources ADD COLUMN visibility TEXT NOT NULL DEFAULT 'public'; -- public | private
ALTER TABLE sources ADD COLUMN owner_tenant_id BIGINT;                    -- private 时必填
-- 唯一约束按可见性分裂：public 全局唯一（保共享）；private 按租户唯一（保隔离）
DROP INDEX uq_sources_url;
CREATE UNIQUE INDEX uq_sources_url_public  ON sources (url) WHERE visibility = 'public';
CREATE UNIQUE INDEX uq_sources_url_private ON sources (owner_tenant_id, url) WHERE visibility = 'private';
```

**判定为 private 的条件**（任一成立）:URL 含疑似凭证（query 里有
`token`/`key`/`secret`/`auth`/`sig` 等参数）、源配置里带认证信息、或用户显式标记私有。
**判定必须保守——拿不准即 private**（误判为 private 只损失去重效率，
误判为 public 是数据泄漏）。

> **不变量 I-T1（红线，保留）**:严禁给 `canonical_key` 加租户前缀。
> 那会让同一篇小红书笔记被 N 个租户各付费补全一次，一次性摧毁全局去重与付费闸门。
> 「谁能看到这条内容」由 `subscriptions`/`deliveries` 的租户维度表达，**不由内容身份表达**。
>
> **private 源的内容怎么办**:其 `content_items` 仍进共享池（canonical_key 全局唯一不变），
> 但**只有 owner_tenant 有 subscription 指向该 source**，故只有它能收到。
> 内容本身若确需隔离（如私有 RSS 正文），由 §2.3 的 RLS 在 `deliveries` 层挡住。

### §2.3 隔离机制:应用层显式过滤 + RLS 兜底（D1 下 RLS 是必做）

**依据（一手）**:schema-per-tenant 被 Atlas/GopherCon 2025 当事人称为
「one of my biggest regrets」——迁移时长随租户数线性增长、schema 漂移是
「needle in haystack」。vane 已有 16 个 goose 迁移，扇出代价不可承受。

RLS 有**五个签名级细节**，缺一即静默失效:

```sql
-- ① FORCE：不加则表 owner（通常就是应用连接角色）完全绕过策略。「策略写了却不生效」头号成因。
ALTER TABLE deliveries ENABLE ROW LEVEL SECURITY;
ALTER TABLE deliveries FORCE  ROW LEVEL SECURITY;

-- ② 显式 WITH CHECK：FOR INSERT 只认 WITH CHECK，缺了则租户能把行写成别人的 tenant_id。
-- ③ RESTRICTIVE：AND 语义，保证以后任何新增 PERMISSIVE 策略都无法意外放宽隔离。
-- ④ current_setting(..., true) 的 missing_ok：未设 GUC 时返回 NULL → 谓词为假 → 默认拒绝。
-- ⑤ (SELECT ...) 包裹：把逐行求值的 SubPlan 提升为只求值一次的 InitPlan。
--    PlanetScale 实测 cost 34,828 → 10,095、latency 1.96s → 102ms（~19x）。
--    注意：即使函数标了 STABLE 仍然需要包。
CREATE POLICY tenant_isolation ON deliveries AS RESTRICTIVE
  USING      (tenant_id = (SELECT current_setting('app.tenant_id', true))::bigint)
  WITH CHECK (tenant_id = (SELECT current_setting('app.tenant_id', true))::bigint);
```

**唯一约束必须全部改成 `(tenant_id, ...)`**:全局唯一约束会因「插入报 duplicate key」
泄漏不可见租户的数据存在性（Postgres 官方 CREATE POLICY 文档承认此侧信道）。
逐条排查:`push_batches.idempotency_key`（004:12 全局唯一）等。

**索引形状**:租户所有表索引首列必须是 `tenant_id`。
`ListUnpushedByUser`/`ListRecentSimhashesByUser` 等以 `user_id` 打头的索引改为
`(tenant_id, user_id, ...)`。形状要一次改对，事后 rebuild 更贵。

**双角色双池（必做）**:
```sql
CREATE ROLE vane_admin LOGIN;                 -- 拥有表，跑 goose 迁移 + 全局抓取写共享表
CREATE ROLE vane_app   LOGIN NOBYPASSRLS;     -- 不拥有任何表，业务连接
```
现状是同一 DSN 跑迁移与业务（同角色）。分离后「RLS 真的在生效」才可被验证。

### §2.4 连接层:只能在事务内 `set_config`，禁用 AfterConnect

**依据（一手）**:pgx 作者 jackc 本人裁定（issue #288）——`AfterConnect` 会
**永久污染连接**，等于每租户一个连接池。

```go
// package tenantdb —— 全仓唯一持有 *pgxpool.Pool 的地方
type Pool struct{ pool *pgxpool.Pool }          // 字段不导出
type Conn struct{ tx pgx.Tx; tenant TenantID }  // 只能由 InTenant 构造

func (p *Pool) InTenant(ctx context.Context, t TenantID,
    fn func(context.Context, *Conn) error) error {
    // BEGIN → SELECT set_config('app.tenant_id', $1, true) → fn → COMMIT
}
```

**pgx 专属坑**:pgx 默认缓存 prepared statement，RLS 辅助函数若误标 `IMMUTABLE`
会返回**上一个租户的数据**（issue #2007 真实事故）。一律标 `STABLE`。

### §2.5 「忘记传 tenant」的编译期拦截

业界普遍只做到运行时（`GetTenant(ctx)` 失败返回错误或 panic）。
Go 没有 phantom type，但**可见性**能做到真正的编译期强制:
`*pgxpool.Pool` 关进 `tenantdb` 包且字段不导出，查询方法只挂 `*Conn`，
`Conn` 只能由 `InTenant` 交出 → **拿不到 Conn 就写不出查询**。

**代价与节奏（诚实说明）**:`store` 包有 ~25 个文件、上百个 `func (s *Store)` 方法，
不可能一个 PR 改完。规定**棘轮式分批迁移**:建 `tenantdb` + `Conn`；`Store` 保留但内部
委托；新方法一律挂 `Conn`，旧方法逐批搬；CI 守卫用**只减不增的白名单**锁棘轮。

### §2.6 Temporal:context 不跨边界

`context.Context` 的值**不跨 Temporal 边界**（走序列化 Header）。必须用
`workflow.ContextPropagator` 四方法接口（`Inject`/`Extract`/`InjectFromWorkflow`/
`ExtractToWorkflow`），四个都实现值才能 Client → Workflow → Activity 全程传下去。

**最易漏的一条**:WorkflowID 必须加租户前缀。跨租户 WorkflowID 冲突会被
`WorkflowIDReusePolicy` **静默拒绝或复用旧执行**——推送直接丢失，且 **RLS 兜不住**。

### §2.7 per-tenant 凭证与配额（D3 下配额是财务护栏）

**凭证**:`tenant_credentials(tenant_id, provider, key_version, kek_id, wrapped_dek,
nonce, ciphertext, ...)`，信封加密 + **AAD 绑定 tenant_id**（密文行不可跨租户搬运，
成本为零，必做）。KEK 抽象:
```go
type KEK interface {
    Wrap(ctx context.Context, dek []byte) ([]byte, error)
    Unwrap(ctx context.Context, wrapped []byte) ([]byte, error)
}
```
先用 local 实现（宿主机文件 0400），以后换阿里云 KMS 不改调用方。
**D5 下这张表的首要客户是飞书 app_id/app_secret**（每租户自建应用）。

**配额（D3 ⇒ 必做，非可选）**:vane 栈无 Redis，**不要为限流引入 Redis**。
Postgres 单行原子 UPDATE 做 token bucket:
```sql
CREATE TABLE tenant_quota (
  tenant_id BIGINT NOT NULL, bucket TEXT NOT NULL,   -- 'llm_tokens'|'push'|'fetch'|'tikhub_calls'
  tokens DOUBLE PRECISION NOT NULL, rate DOUBLE PRECISION NOT NULL,
  burst  DOUBLE PRECISION NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, bucket)
);
```

**跨租户 DoS 面（测绘实证，D1 下全部变成真问题）**:
- `llm/client.go` 的 `MaxConcurrent=5` 信号量被打分/出卡/agent/深挖/A2A 共用。
  `agent/loop.go` 注释已实证「5 条卡死消息即可瘫痪全部 LLM 面」。
- `store.go` 的 `pgxpool MaxConns=10`:一个租户的批量推送即可占满。
- `store/toolcalls.go` 的 TikHub 每日限额**当前是全局计数**，注释「单 owner MVP 下
  二者等价」正是本接缝要撕开的地方。
- `a2a/a2a.go` 的 `maxConcurrentExecutions=8` 注释自认「自伤，非跨租户互伤」。

### §2.8 已知越权洞（接缝②打开当天即生效，必须同批修）

- ~~`api/schedules.go` `handleDeleteSchedule`~~ —— **2026-07-19 核实已修**：
  handler 传 userID，`Scheduler.DeletePush` 内 `GetSchedule(id, userID)` 归属校验先行。
- ~~`agent/tools.go` `removeScheduleTool`~~ —— **同上已修**，同一条校验路径。

> **2026-07-19 复查记录**：两处洞在多租户改造中已补上校验，但当时**没有回归测试锁住**
> ——而同期的 `ClaimPendingAction`、`EnableSource`、卡片回调 delivery 都有越权用例，
> 唯独契约点名的这两处没有。修好而无守卫只等于「这一版是对的」，不等于「以后也对」。
> 现由 `store/schedule_ownership_test.go` 守住（`GetSchedule` / `DeleteSchedule` 的
> 归属谓词各有一条反向验证：摘掉谓词即变红）。
>
> 同批清理了两处**说假话的注释**：`api/schedules.go` 与 `agent/tools.go` 里
> 「单 owner，故不逐条校验归属」在代码已校验后仍留在原地。代码在校验、注释说不校验，
> 后来的人会据此把 userID 参数当冗余删掉——那一刀下去才是真的越权洞。
>
> 同期排查的其余用户可传 id 的入口（调度更新、信源退订/启用、卡片回调 delivery）
> **均已带归属谓词**，无新增洞。

**范式（做得最好的一处）**:`store/agent.go` 的 `ClaimPendingAction`/`CancelPendingAction`
把归属校验放进 WHERE 谓词内，越权请求完全无副作用。

### §2.9 迁移

**本次（默认，见 §0.1）**:仅 1 个真实用户，走**短停机窗口一次性迁移**。

**未来范式（已有真实租户后）——五步在线迁移**，每步可单独回滚:
1. `ADD COLUMN tenant_id BIGINT`（PG11+ 秒级），应用双写；
2. 分批回填（按主键区间，每批单独提交；单条大 UPDATE 会长持锁 + 撑爆 WAL）；
3. `ADD CONSTRAINT ... CHECK (tenant_id IS NOT NULL) NOT VALID` → `VALIDATE CONSTRAINT`
   （**不要直接 `SET NOT NULL`**，会全表扫 + 持 AccessExclusive）；
4. `SET NOT NULL`；5. RLS 灰度开关。

**goose 两个陷阱（必踩）**:`CREATE INDEX CONCURRENTLY`/`VALIDATE CONSTRAINT` 需文件头
`-- +goose NO TRANSACTION`；plpgsql 函数体分号会被切分器切碎，须
`-- +goose StatementBegin/End` 包住。

### §2.10 RLS 兜不住的三类路径

- **视图**:普通视图以 owner 权限执行 ⇒ 读穿 RLS。PG15+ 必须 `WITH (security_invoker = true)`。
- **物化视图/聚合表**:导出数据完全失去 RLS 保护。`store/observability.go`、`runstats.go`、
  `deliveries_history.go` 三处聚合读若做成物化视图，等于把跨租户数据摊在无保护表上。
  对策:聚合结果表自带 tenant_id + RLS，刷新语句显式 `GROUP BY tenant_id`。
- **外键**:父表被 RLS 挡住时，子表插入报「违反外键约束」而非「权限不足」。
  须把 `users` 主键扩为同时有 `UNIQUE (tenant_id, id)`，子表建复合 FK。**一次做对**。

## §3 接缝③:Channel 抽象（D7 现在就立）

### §3.1 现状判断:物理边界干净，语义边界脏

**物理**（好消息）:除 `feishu` 包外**零处** import lark SDK；
`pusher`/`workflow`/`feedback`/`cardgen` 已用窄接口 + 函数注入把飞书隔在外
（`FeishuSender`/`FeishuManager`/`CardBuilder`），import 环被 M5 契约 §8.2 封死。
⇒ **接缝③不需要拆包，只需给现有窄接口换概念**。

**语义**（三处真渗透）:
1. **收件人模型 = 一个飞书 open_id**:`users.feishu_open_id`（001:24，UNIQUE NOT NULL，
   无 channel 维度）→ `pusher.Push(ownerOpenID)` → `FeishuManager.OwnerOpenID()` →
   `feishu.SendCard(openID)`。
2. **投递回执 = `deliveries.feishu_message_id`**:列名（001:117）、索引名（006:23）、
   追问反查的唯一钥匙（`feedback/question.go:59`）。
3. **最深的一处**:`systemPrompt`（`agent/loop.go:31-37`）用自然语言**向模型承诺了
   「会出确认卡」这个渠道能力**；`RunOnce` 对 Confirm 直接报错——代码已自认
   「没有卡片通道的渠道跑不了这套」。

> **D7 的价值**:这三处渗透全在 `users`/`deliveries` 两张表上，而接缝② 的租户迁移
> **本来就要动这两张表**。一次迁移干完，避免以后再改一次列名 + 索引名 + 反查。

### §3.2 飞书 per-tenant 连接池（D5 的结构性改造）

**现状**:`feishu/manager.go` 是进程内单例，维护**一条** WS 连接，凭证读
`settings` 表 `key='feishu'` 的**单一行**。

**D5 后**:
```go
type Manager struct {
    conns sync.Map // map[TenantID]*tenantConn，各自独立 WS 连接与生命周期
}
```
- 凭证从 `settings` 单行迁入 `tenant_credentials`（加密，§2.7）。
- 租户填/改凭证 → **只重连该租户**，不影响其他租户。
- 入站事件的租户归属**由连接自身携带**（哪条连接收到的就是哪个租户），
  无需从事件内容反推——这是 D5 相对「市场应用」的一大简化。

**三个必须在契约里点名的代价**:
1. **已知 SDK goroutine 泄漏会按租户数放大**:`manager.go:364-383` 记录了
   「每次 Reconfigure 泄漏一个 parked goroutine」的既有取舍。单租户时可忍，
   N 个租户各自 Reconfigure 后是 N 倍。**必须在本次改造中根治或设上限**。
2. **连接数天花板**:N 租户 = N 条 WS 长连接。需要惰性连接（租户无活跃订阅则不连）
   + 连接数上限 + 超限排队策略。
3. **凭证无效的租户不得拖垮进程**:单租户凭证失效/被撤销时只标记该租户
   `channel_status=error` 并通知，不重试风暴、不影响他人。

### §3.3 统一事件信封（依据:Bot Framework Activity）

```go
// package channel
type Event struct {
    Kind        EventKind   // Message | Interaction | System
    ChannelID   ChannelKind // feishu | wecom | slack | email | webhook
    TenantID    TenantID
    Conversation ConvRef
    From, To    IdentityRef     // (channel, external_ref) 二元组
    Text        string          // 人类可读
    Value       json.RawMessage // 中立业务载荷（程序化）
    ChannelData json.RawMessage // 渠道私有逃生舱：永不参与业务判断
    ReplyTo     *ReplyHandle
}
```

**两条硬约束**:
- `Value`（中立）与 `ChannelData`（渠道原样透传）**分家**，是挡住渠道泄漏的主边界。
  **业务逻辑读 `ChannelData` 即违约。**
- **`ReplyHandle` 现在就要留字段**:飞书 WS 不需要，但 Slack 的 `response_url`
  （30 分钟内最多 5 次）、webhook 渠道**每事件自带回信地址**。
  ```go
  type ReplyHandle struct{ Token string; ExpiresAt time.Time; RemainingUses int }
  ```
  飞书实现填「立即失效、次数 1」。不留字段以后是破坏性改动。

### §3.4 能力声明:进代码，且不是 bool

**反面教材（一手）**:Bot Framework 有完整的 per-channel 能力矩阵，
但它**只存在于文档表格里，运行时没有任何 capability 声明或协商 API**。vane 不得重蹈。

```go
type Capabilities struct {
    RenderLevel      RenderLevel // Rich | Partial | Image | Text（抄 BF 四态）
    MaxActions       int         // 0 = 不支持交互
    MaxLabelLen      int
    TitleMaxLen      int         // 0 = 该渠道无标题概念（抄 Apprise，基类据此把 title 折进 body）
    BodyMaxLen       int
    SupportsMarkdown bool
    Confirm          ConfirmLevel // 见 §3.6
}
```

**`Capabilities()` 必须是方法而非结构体常量**。依据:Opsgenie 的双向短信
「除美国和欧盟外的区域不支持」，且用 sender ID 发送时**根本无法回复**——
即**同一渠道的交互能力会随区域/发送方式/凭证在运行时变化**。

### §3.5 可移植 Presentation + 判别联合 Action（依据:OpenClaw）

业务方产出渠道无关的 `Presentation{Blocks []Block}`；飞书渲染器负责
`Presentation → card JSON`，未来邮件渲染器负责 `→ HTML/纯文本`。

**审批必须是独立的 action 类型**，不是塞进 callback value 的字符串:
```go
type Action struct {
    Kind ActionKind // Command | Callback | Approval | Question | URL
    ApprovalID string   // Approval 专用：核心据此保证它永不进入降级文本
    Decision   Decision // AllowOnce | Deny
}
```
vane 现在正是把 approval 语义编码进飞书 callback value 的（`onCardAction` 的
`confirm/cancel/fb/fbr` 四类分流），多渠道后会重复三遍。

> **契约条款（非建议）:降级归核心、原生渲染归渠道。**
> 若让每个 channel 自己决定「不能渲染按钮时怎么办」，安全不变量会在第三个渠道上被悄悄破坏。
> 硬规则:**不支持的控件降级，而非让整条发送失败**；降级后核心须保留标签作为非交互文本
> ——绝不出现空白消息。

### §3.6 确认能力:分级枚举 + fail-closed 兜底（本接缝最关键一条）

> **不变量 I-C1（重述安全承诺，与渠道解绑）**:
> 写操作必须有一个**可归属、可过期、单次消费**的 `approvalID` 被显式 approve。
> 该不变量**不要求渠道具备交互控件**。

```go
type ConfirmLevel uint8
const (
    ConfirmNone        ConfirmLevel = iota // 无任何确认通道
    ConfirmLink                            // 仅能送一次性链接（邮件）
    ConfirmKeyword                         // 可解析回复关键字
    ConfirmInteractive                     // 原生交互控件（飞书/Slack/企微）
)

type AskFallback uint8 // Deny（默认）| Allowlist | Full
```

**`askFallback` 语义（抄 OpenClaw）**:需要确认但**没有任何 UI 可达**（或提示超时）时的裁决。
**省略即 Deny，读配置失败也 Deny**（OpenClaw 的 `createFailClosedExecApprovalsFallback`
直接返回全 deny）。⇒ 这一个枚举把「贫渠道没有交互能力」从架构难题降级成
per-tenant × per-channel 配置项。

**贫渠道确认，业内三条路（无银弹，优先级明确）**:
1. **首选 magic link**:一次性 token + 15–30 分钟过期 + 高熵不可猜 + 执行前二次认证
   （邮箱可能被转发/代收，token ≠ 身份；二次认证走 §1.1 的认证层）。
   **注意冲突**:magic link 绝不能暴露 `ActionID`（内部主键进邮件正文），
   须新增 `token`/`expires_at`/`consumed_at` 三列，token 与 ActionID 不复用。
2. **次选关键字回复**（Salesforce 范式）:`APPROVE/YES/REJECT/NO` **必须在正文第一行**，
   评论第二行。规则越死越好，**别上 LLM 解析**——签名档就足以毁掉解析。
3. **兜底直接禁用**:Bot Framework 官方矩阵里 Email/Twilio SMS 的 card actions 就是 `None`。

### §3.7 回调归一 + 拒绝理由回灌

**中立规则（依据 BF messageBack）**:按钮点击 / 关键字回复 / magic link 回调
→ **产生同一个中立事件**（`Event{Kind: Interaction, Value: {approval_id, decision}}`）。
收益:`agent.Loop.ExecuteAction/CancelAction` **一行不用改**。

**同批补一个已知缺陷**:vane 的 `CancelAction` **不收拒绝理由**。
HumanLayer 的 `FunctionCall.status.comment` 在拒绝时**回灌给 LLM**——vane 现在
丢掉了最有价值的反馈信号。契约要求拒绝路径补 `reason` 并回灌 agent。

### §3.8 身份:一个逻辑 user 挂多条渠道身份

```sql
CREATE TABLE channel_identities (
  tenant_id BIGINT NOT NULL, user_id BIGINT NOT NULL,
  channel   TEXT   NOT NULL, external_ref TEXT NOT NULL,
  credentials JSONB, PRIMARY KEY (tenant_id, channel, external_ref)
);
```
`users.feishu_open_id` 迁入本表；`deliveries.feishu_message_id` 重命名为
`external_message_id`（需改列名 + 索引名 + `feedback/question.go` 反查）。

**必须预留身份合并**（抄 Matrix double puppeting 的迁移动作，非 ghost 概念）:
必然出现「先见到一个未知渠道地址、后来它被认领为某已有 user」。
**该操作必须是特权路径，且必须在租户内进行——跨租户合并一律禁止。**

**provider 无状态（硬约束，抄 Novu）**:Channel 实现内**不许有自己的 DB 访问**，
`pending_actions`/`deliveries` 读写全留上层——否则每个渠道都要自己做租户隔离，必错。

### §3.9 顺手回收:已在飞书包里的渠道中立逻辑

（非重构洁癖，它们是未来渠道的共用件）
- 入站事件按 id 去重 + TTL（`handler.go:163-179`）→ 框架级 inbound dedup
- `humanizeLLMError`（`handler.go:985-1001`）→ `types`/usermsg 包，零飞书成分
- 「goroutine 执行 + select 竞速同步预算 + 超时降级」**同一骨架抄了三遍**
  （`handler.go:519-581`/`645-683`/`699-739`）→ 抽 `syncOrDeferred(...)`
- 卡片视图模型 `buildSubtitle`/`platformEmoji`/`domainFromURL`/`relativeTime`
  （`card.go:234-304`）→ 渠道中立视图模型
- `captureOwnerIfFirst`、owner 白名单判定 → **接缝①的注册流会直接吃掉**

**真·飞书特有（留在 adapter）**:WS 连接生命周期与代数机制、事件 dispatcher 的 8 类订阅、
卡片 2.0 schema 构造、`cardActionSyncBudget=2.5s`（唯一真正由飞书 3s 硬约束推导的取值）。

## §4 接缝④:Source SDK

### §4.1 形态:Steampipe 式 Go 结构体字面量，**不引入外部 DSL**

**依据**:Airbyte/n8n 需要 JSON-path 映射 DSL，是因为目标 schema 未知；
vane 的目标是**固定已知的 7 字段** `types.ContentItem`。
用 DSL 填 7 个固定字段只会把编译期错误变成运行期错误，还丢掉类型检查与跳转。

```go
var XHSUserPosts = &source.Connector{
    Platform: types.PlatformXHS, Capability: types.CapUserPosts, Kind: types.KindArticle,
    Params:   []source.ParamSpec{{Name: "user_id", Required: true, ...}},
    Identity: source.IdentityFromField("note_id"),   // 声明身份，不实现去重
    Request:  ...,
    Paginate: source.CursorFromResponse{Field: "cursor", HasMore: "has_more"},
    Extract:  func(ctx, raw []byte) ([]types.ContentItem, error) { ... }, // 唯一必写的真代码
}
```

**框架承包（测绘实证的重复面）**:HTTP client 构造 + 超时/大小兜底（**5 份逐字重复**）、
config JSONB 解析与必填校验（6 份同构）、鉴权头、HTTP 状态码分类（**6 份且已漂移**）、
body 读取与 LimitReader（5 份 + `x.go` 一处**用错**）、时间戳解析（4 种格式 4 套）、
正文截断、串号防御（2 份独立实现）、限流与付费闸门（**只有 TikHub 一家有**，
同样按次计费的 Exa 与 x/xhs_user 完全没有）、Kind 赋值（**6 处手写字面量**，
而 `sourcecatalog.Entry.Kind` 已声明过一遍）。

> **安全不变量的严重发现**:SSRF 双重防护（抓取前 `LookupIP` 预检 + 传输层校验）
> **只有 RSS fetcher 有**。SDK 化后由框架统一施加于所有连接器。
> **D1（陌生人租户可自由添加信源）下这条从「技术债」升级为「安全漏洞」。**

### §4.2 增量:`is_data_feed` 是默认且唯一模式

**vane 的全部信源都是 data feed**（不可过滤、最新在前）:RSS、X user_posts、
XHS user_posts/search、Exa search。⇒ 这不是边角选项，而应是 SDK 的**默认唯一增量模式**。

**当前最大结构性缺口（测绘实证）**:`tikhub.go` 的 `page=1`、`xhs_user.go` 的 `cursor=""`
——**vane 根本没有游标**，全靠「抓最新一页 + 全局去重当增量」。
`xhs_user` 的 `has_more` 只进日志、不驱动循环。**一旦漏抓一轮（源挂了/调度延迟）
就永久丢内容。**

**state 是黑盒（Singer/Airbyte/Fivetran 三家一致）**:语义归连接器作者，
**存储与生命周期归框架**。给 `sources` 加 `cursor_state JSONB`。

### §4.3 `spec` / `check`:把「实测准入」变成可执行事实

- **`spec`**:连接器自带 `Params []ParamSpec` 成为**单一事实来源**，取代
  `sourcespec.go` 里三个手写 `build*` switch（~150 行 if/else + 手写中文文案），
  并同时供给 `sourcecatalog` 元数据与 agent 工具参数描述——现在这是**三份分开维护**、
  靠测试锁一致性的东西。
- **`check`**:拿真配置真打一次上游。`sourcecatalog` 的 `Status`+`Reason` 现在是
  **人工实测一次写死的**；`check` 让「这个源能不能用」变成运行时可查询、可挂探针的事实。

### §4.4 逃生舱:实现接口并在声明里点名，而非绕过框架

`web/page_watch` 完全不像「抓一列内容」（产出 `KindChange`、需 `SnapshotStore`、
预填 `CanonicalKey`）。**任何 SDK 设计如果不能容纳 page_watch，就是在逼人绕过 SDK。**
抄 n8n 二分法:`DeclarativeConnector` 与 `ProgrammaticConnector`，后者仍走框架的
注册/门禁/记账。

## §5 双机并行推进

### §5.1 约定放哪（三层，不可混用）

| 平面 | 载体 | 内容 |
|---|---|---|
| **协调** | 飞书多维表格（功能清单 + 开发认领）、BOARD.md | 什么/谁/什么状态。高频变、非代码 |
| **契约** | **本文 + Go interface/types（在仓库）** | 模块怎么拼。必须可 diff / PR review / CI 强制 |
| **决策日志** | journal + AGENTS.md 重要更迭 | 为什么这么选 |

> **让并行不撞车的是契约平面，它绝不能放多维表格**——表格不可 diff、不可 review、
> 不可被 CI 强制、不随代码回滚。多维表格答「什么/谁」，仓库答「怎么拼」。

### §5.2 扇出顺序

```
第 0 步（串行，零风险）  §1.1 principal 收敛（过渡期返回固定值，行为不变）
        ↓
第 1 步（串行，地基）    接缝①：邮箱+密码认证 + tenants/memberships/invites + 注册流
        ↓
第 2 步（并行扇出）
   ├─ 机器 A（Mac）：接缝② 租户隔离（表分级/RLS/tenantdb/Temporal propagator/凭证/配额/越权洞）
   └─ 机器 B（Win）：接缝③ Channel（Event 信封 + Capabilities + Presentation/Action
                     + 飞书 adapter 就地重构 + per-tenant 连接池）
        ↓  （二者在 users/deliveries 迁移处交汇 —— 见下方「唯一交汇点」）
第 3 步（并行）          接缝④ Source SDK（弱依赖，可与上并行）
```

**唯一交汇点**:`users` 加 tenant_id（接缝②）与 `users.feishu_open_id` 迁入
`channel_identities`（接缝③）**是同一次迁移**。约定:**该迁移由机器 A 一次写完**
（含两个接缝的列变更），机器 B 只消费其结果。写完立即合 main，B 再动 adapter。

**机器归属按「接口背后的模块」而非文件**:Mac 有生产 VPS/DB 访问（迁移与 RLS 验证
必须在真库上做）；Windows 做 Channel（新包 + feishu 包内重构，与 store 迁移零文件交集）。

### §5.3 纪律（比工具重要）

- **主干开发**:接口优先的小 PR 持续合 main，**禁止长命重构分支**（两机各拉一条必死）。
- **feature flag / dark-ship**:未完成的部分用 config 门控合入 main，main 永远绿、
  永远可部署。先例:TikHub 端点面用 key 门控 dark-ship（#56）。
- **棘轮式 CI 守卫**:白名单只减不增（§2.5）。

## §6 已排除方案（写明防止三个月后重提）

| 方案 | 排除理由 |
|---|---|
| **schema-per-tenant** | 迁移时长随租户线性增长、schema 漂移难查；当事人（Atlas/GopherCon 2025）称「最大遗憾」。vane 已 16 个迁移，扇出不可承受 |
| ~~vane 自建密码体系~~ | **D2′ 后不再排除**——邮箱+密码可自建，也可用托管 IdP 免费档；取舍见 §1.1 表。唯一硬约束是 I-A4：实现细节不得泄漏到 `PrincipalFromContext` 之外 |
| **飞书应用市场应用** | D5 已定租户自建应用：市场应用要过审（周期不可控）、所有租户共享 API 限流（跨租户互伤）、Manager 要做多租户事件分流（改造量最大） |
| **Go 标准库 `plugin` 包** | 官方明列:仅三平台、工具链版本必须完全一致否则运行时崩溃、不可卸载。连接器作者就是 vane 自己，走 PR + CI 是正常路径 |
| **hashicorp/go-plugin 子进程隔离** | 连接器可信时隔离是纯成本；崩溃隔离已由 Temporal Activity 重试提供 |
| **外部 YAML/JSON-path 字段映射 DSL** | 目标 schema 固定已知（7 字段），DSL 只会把编译期错误换成运行期错误 |
| **为限流引入 Redis** | Postgres 行级锁足以做 token bucket，且天然持久化 |

**留边界**:若未来允许**租户上传自定义连接器**，优先 WASM（wazero，纯 Go 无 CGO）
而非子进程。为此现在唯一要做的动作:**把 SDK 接口设计成可序列化/gRPC-able 的形状**。

## §附:一手依据

- **OpenClaw**（openclaw/openclaw @00eb33f, v2026.7.2）:`ChannelPlugin` 约 30 个可选
  adapter 槽位、两层 `presentationCapabilities`（带数值上限）、`MessagePresentationAction`
  判别联合（approval 一等）、7 步出站流水线与成文降级规则表、审批三档阶梯
  （原生 / emoji reaction / `/approve <id>`）、`askFallback` fail-closed。
- **Bot Framework**:Activity 统一信封（`value`/`channelData` 分家、`serviceUrl` 每事件自带）、
  能力矩阵**只在文档不在运行时**的反面教材、四态渲染枚举、`messageBack` 回调归一。
- **Apprise**:类级能力声明 + 基类自动降级（`title_maxlen=0` ⇒ title 折进 body）。
- **Novu**:provider 无状态、`subscriber.channels[]` 跨渠道身份与凭证。
- **HumanLayer**:审批做成一等资源（渠道只是 contact channel）、拒绝 `comment` 回灌 LLM。
- **Postgres RLS**:FORCE / WITH CHECK / RESTRICTIVE / `missing_ok` / `(SELECT ...)` 五细节；
  PlanetScale 实测 19x latency 差异；Bytebase footgun 清单（唯一约束跨租户泄漏、视图绕过、
  物化视图失保护、FK 报错误导）。
- **pgx**:jackc 本人裁定 `AfterConnect` 永久污染连接（#288）；prepared statement 缓存 +
  误标 IMMUTABLE 返回上一租户数据（#2007）。
- **Airbyte / Singer / Fivetran / Steampipe / Benthos**:四动作协议、黑盒 state、
  `is_data_feed`、声明式分页三策略、Go 结构体字面量式连接器。

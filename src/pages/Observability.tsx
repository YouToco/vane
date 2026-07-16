import { Fragment, useEffect, useState } from "react";
import type { ReactNode } from "react";
import { api, ApiError } from "../api";
import type {
  ObservabilityReport,
  PipelineCounts,
  ProbeResult,
  ProbeStatus,
  ScoreBucket,
  SpanDayCost,
} from "../api";

// 可观测看板 —— 纯固化组件（ui-interaction-principles.md：「状态/成本监控 = 图表看板」）。
//
// 全页只读、零模型参与，这是刻意的：让模型去判断"模型有没有静默骗人"是循环论证——
// 出问题时它自己也是坏的（同 probe/probe.go 包注释）。判定逻辑一行都不在前端，
// 红绿灯全部由后端 probe.Run 算好，这里只负责把 7 条结论和支撑指标画出来。
// 前端重算判定 = 探针有了第二份实现，必漂。

// 窗口档位：能枚举就别对话（准则 1）——固化档位，不给自由输入框。
// 24h 是契约 §16.2 红线的口径，故为默认；48h/7d 只用来看趋势。
const WINDOW_OPTIONS: [number, string][] = [
  [24, "24 小时"],
  [48, "48 小时"],
  [168, "7 天"],
];

const DEFAULT_WINDOW_HOURS = 24; // 对齐 probe.DefaultWindow

// 状态呈现表。
//
// yellow 绝不做成绿色系：它是"没验到"不是"验过了"。契约 §16 要求部署后当天与次日
// 复跑，一条 vacuously green 的探针会让人以为验过了（probe.Status 的注释原文）。
// 故黄灯用琥珀色 + 虚线边框 + 「未验到」字样三重区分——虚线是关键：色觉障碍下
// 绿/琥珀可能糊成一片，边框形状不会。
const STATUS_META: Record<ProbeStatus, { label: string; icon: string; cls: string }> = {
  green: { label: "通过", icon: "✓", cls: "probe-green" },
  yellow: { label: "未验到", icon: "?", cls: "probe-yellow" },
  red: { label: "击穿", icon: "!", cls: "probe-red" },
};

// 总体结论文案，与 probe.Report.Worst() 的口径一致（红 > 黄 > 绿）。
const WORST_META: Record<ProbeStatus, string> = {
  green: "7 条探针全部通过",
  yellow: "有探针未验到——不是通过，请按说明补跑",
  red: "有红线被击穿——按契约应回滚排查",
};

const BEIJING_TZ = "Asia/Shanghai";

// 后端全 UTC（红线 6 的三时区陷阱：DB=UTC、VPS 本地=EDT、Boss 读北京时间），
// 换算只在这一层做。显式写 timeZone 而不是靠 toLocaleString 的浏览器默认时区：
// 看板可能在任意机器上打开，落到 EDT 就又踩回同一个坑。
function fmtBeijing(iso?: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString("zh-CN", { timeZone: BEIJING_TZ, hour12: false });
}

// 成本的日界是 **UTC 日**（store/observability.go 的 date_trunc AT TIME ZONE 'UTC'），
// 不是北京日。这里刻意不换算：一个 UTC 日的桶装的是北京 08:00 到次日 08:00 的调用，
// 标成某个北京日期就是撒谎，而且日成本环比红线正是按 UTC 日算的。
// 用 getUTC* 取月日，绕开浏览器本地时区对 Date 显示的影响。
function fmtUTCDay(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${mm}-${dd}`;
}

// cost_usd 是 NUMERIC(10,6) 逐行舍入后求和，6 位小数是它的原生精度，不多不少。
// 别用 toFixed(2)：score 单次可低至 ~$0.0000008，两位小数会把整列显示成 0.00，
// 而"整批舍成 0"本就是正常现象（types.SpanDayCost 注释），再被格式化抹一次就彻底看不见。
function fmtUSD(v: number): string {
  return `$${v.toFixed(6)}`;
}

// trace_id 全串没人读得下来，截前 8 位够肉眼配对；
// 全串挂 title 供复制——探针红灯 Detail 给的排查 SQL 要按 trace_id 查 llm_calls。
function shortTrace(id: string): string {
  return id.length > 12 ? `${id.slice(0, 8)}…` : id;
}

// 闸门中文名（types.BatchExitGate 的五个取值）。
//
// 刻意不直接吐后端枚举：这一列要回答的是"今早为什么没推送"，而 "dedup" / "cardgen"
// 是 pipeline 的内部活动名，把它甩给读者等于让人自己去翻代码。hint 挂 title，
// 补上枚举名本身与一句展开——排查时要按 exit_gate 查库，那个英文串仍需可见。
//
// 闸门名 = 产出该结果的**上一步活动名**，不是"下一步没跑成"：
// dedup 意为"Dedup 跑完后没剩下东西"。
const GATE_META: Record<string, { label: string; hint: string }> = {
  fetch: { label: "无新内容", hint: "fetch：抓取跑完后无候选——压根没抓到新内容" },
  dedup: { label: "去重后空", hint: "dedup：去重跑完后无候选——抓到了，但全是重复" },
  score: { label: "打分后空", hint: "score：打分跑完后无候选" },
  select: { label: "择优后空", hint: "select：择优跑完后无候选" },
  cardgen: { label: "卡片生成后空", hint: "cardgen：卡片生成跑完后无候选" },
};

// 漏斗阶段顺序 = pipeline 顺序，与 types.PipelineCounts 的字段一一对应。
const FUNNEL_STAGES: [keyof PipelineCounts, string][] = [
  ["fetched", "抓取"],
  ["deduped", "去重"],
  ["scored", "打分"],
  ["selected", "择优"],
  ["cards", "卡片"],
];

// ── 漏斗：只渲染**真跑过**的阶段 ──
//
// 用 `v !== undefined` 过滤，绝不用 `?? 0`。后端特意把 PipelineCounts 做成 *int +
// omitempty，就是为了让"这步没跑"（缺席）与"跑了得 0"（0）在库里可区分；一个 `?? 0`
// 就把这份区分抹平，把停在 dedup 闸门的运行画成 "20→0→0→0→0"——读起来是
// "打分打出了 0 条"（LLM 全军覆没的形状），而事实是打分压根没被调用。
// 那正是 PR2 要消灭的那类混淆，在渲染层重造一次等于白改。
function Funnel({ counts }: { counts: PipelineCounts }) {
  const steps = FUNNEL_STAGES.map(([k, label]) => ({ label, v: counts[k] })).filter(
    (s): s is { label: string; v: number } => s.v !== undefined,
  );
  // 全缺席 = 008 之前的历史行（stage_counts 默认 '{}'），或跑到了 Push 的批次
  // （刻意没填漏斗）。显示"—"表示**没有记录**，不拿 0 冒充观测结果。
  if (steps.length === 0) return <span className="muted">—</span>;
  return (
    <span className="funnel" title={steps.map((s) => `${s.label} ${s.v}`).join(" → ")}>
      {steps.map((s, i) => (
        <Fragment key={s.label}>
          {i > 0 && <span className="funnel-arrow" aria-hidden="true">→</span>}
          <span className="funnel-n">{s.v}</span>
        </Fragment>
      ))}
    </span>
  );
}

// ── 单条探针卡片 ──
function ProbeCard({ r }: { r: ProbeResult }) {
  const meta = STATUS_META[r.status];
  // 非 green 默认展开：红灯要人立刻看到怎么查，黄灯要人看到"为什么没验到"——
  // 折起来的黄灯和绿灯长得就一样了，那正是本页要避免的假安全感。
  const [open, setOpen] = useState(r.status !== "green");

  return (
    <div className={"card probe-card " + meta.cls}>
      <div className="probe-head">
        <span className="probe-icon" aria-hidden="true">
          {meta.icon}
        </span>
        <div className="probe-title-wrap">
          <div className="probe-title">
            {r.name}
            <span className="probe-ref">{r.contract_ref}</span>
          </div>
          <div className="probe-summary">{r.summary}</div>
        </div>
        <span className="probe-tag">{meta.label}</span>
      </div>
      {r.detail && (
        <>
          <button
            type="button"
            className="probe-toggle"
            onClick={() => setOpen((v) => !v)}
            aria-expanded={open}
          >
            {open ? "收起说明" : "展开说明"}
          </button>
          {open && <div className="probe-detail">{r.detail}</div>}
        </>
      )}
    </div>
  );
}

// ── 计数格 ──
function Stat({ label, value, tone }: { label: string; value: ReactNode; tone?: string }) {
  return (
    <div className={"stat" + (tone ? " " + tone : "")}>
      <div className="stat-value">{value}</div>
      <div className="stat-label">{label}</div>
    </div>
  );
}

// ── 分数分布直方图：手写 SVG（契约禁止新依赖，图表库一个都不引）──
function ScoreHistogram({ buckets }: { buckets: ScoreBucket[] }) {
  const total = buckets.reduce((s, b) => s + b.count, 0);
  if (total === 0) {
    return <div className="empty-hint">窗口内没有可解析出分数的打分调用。</div>;
  }
  const max = Math.max(...buckets.map((b) => b.count));

  // viewBox 固定 + width:100%：SVG 自己等比缩放，省掉测量容器宽度的 ResizeObserver。
  const W = 700;
  const H = 220;
  const padL = 40;
  const padR = 12;
  const padT = 22;
  const padB = 34;
  const plotW = W - padL - padR;
  const plotH = H - padT - padB;
  const slot = plotW / buckets.length;
  const barW = slot * 0.66;

  return (
    <svg className="chart" viewBox={`0 0 ${W} ${H}`} role="img" aria-label="分数分布直方图">
      {/* 基线 */}
      <line x1={padL} y1={padT + plotH} x2={W - padR} y2={padT + plotH} className="chart-axis" />
      {/* 峰值参考线：没有它就只能看出形状、看不出量级 */}
      <line x1={padL} y1={padT} x2={W - padR} y2={padT} className="chart-grid" />
      <text x={padL - 8} y={padT + 4} className="chart-tick" textAnchor="end">
        {max}
      </text>
      <text x={padL - 8} y={padT + plotH + 4} className="chart-tick" textAnchor="end">
        0
      </text>
      {buckets.map((b, i) => {
        const h = max === 0 ? 0 : (b.count / max) * plotH;
        const x = padL + i * slot + (slot - barW) / 2;
        const y = padT + plotH - h;
        return (
          <g key={b.lo}>
            <rect x={x} y={y} width={barW} height={h} rx={3} className="chart-bar">
              <title>{`${b.lo}–${b.hi}${i === buckets.length - 1 ? "（闭区间）" : ""}：${b.count} 次`}</title>
            </rect>
            {b.count > 0 && (
              <text x={x + barW / 2} y={y - 5} className="chart-value" textAnchor="middle">
                {b.count}
              </text>
            )}
            <text
              x={x + barW / 2}
              y={padT + plotH + 18}
              className="chart-tick"
              textAnchor="middle"
            >
              {b.lo}
            </text>
          </g>
        );
      })}
      <text x={W - padR} y={padT + plotH + 18} className="chart-tick" textAnchor="end">
        100
      </text>
    </svg>
  );
}

interface CostDay {
  day: string;
  rows: SpanDayCost[];
  calls: number;
  cost: number;
}

// 后端已按 (day DESC, span ASC) 排好序，这里只做分组不重排——
// 重排会让前端有第二套排序口径，日成本环比看的就是相邻两个 UTC 日的先后。
function groupCosts(costs: SpanDayCost[]): CostDay[] {
  const out: CostDay[] = [];
  for (const c of costs) {
    let g = out.find((x) => x.day === c.day);
    if (!g) {
      g = { day: c.day, rows: [], calls: 0, cost: 0 };
      out.push(g);
    }
    g.rows.push(c);
    g.calls += c.calls;
    g.cost += c.cost_usd;
  }
  return out;
}

export default function Observability() {
  const [windowHours, setWindowHours] = useState(DEFAULT_WINDOW_HOURS);
  const [report, setReport] = useState<ObservabilityReport | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [nonce, setNonce] = useState(0); // 手动刷新：+1 即重跑 effect

  useEffect(() => {
    let alive = true;
    setLoading(true);
    api
      .observability(windowHours)
      .then((r) => {
        if (!alive) return;
        setReport(r);
        setLoadError("");
      })
      .catch((err) => {
        if (!alive) return;
        // 只取 AppError.Message 那一层人话（红线 3：原始 error 链不进用户文案）
        setLoadError(err instanceof ApiError ? err.message : "加载失败");
        setReport(null);
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [windowHours, nonce]);

  // 总体结论：与 probe.Report.Worst() 同口径（红 > 黄 > 绿）。
  // 这不是"重算判定"——判定在后端，这里只是把已算好的 7 个状态取最严重的那个当标题。
  const worst: ProbeStatus = report
    ? report.results.some((r) => r.status === "red")
      ? "red"
      : report.results.some((r) => r.status === "yellow")
        ? "yellow"
        : "green"
    : "green";

  const q = report?.quality;
  const inj = report?.injection;
  const ev = report?.evolve;
  const costDays = report ? groupCosts(report.costs) : [];

  return (
    <div className="page">
      <div className="page-head">
        <h2 className="page-title">可观测</h2>
        <button
          type="button"
          className="btn btn-ghost btn-mini"
          onClick={() => setNonce((n) => n + 1)}
          disabled={loading}
        >
          {loading ? <span className="spinner spinner-dark" /> : "刷新"}
        </button>
      </div>
      <p className="muted obs-intro">
        M5 契约 §16 的 Gate 服务端探针。判定全部由后端只读聚合算出，本页不参与判断、
        也不调用任何模型。
      </p>

      {/* ── 窗口档位：固化选择器，不是自由输入框 ── */}
      <div className="obs-toolbar">
        <div className="seg" role="tablist" aria-label="统计窗口">
          {WINDOW_OPTIONS.map(([h, label]) => (
            <button
              key={h}
              type="button"
              role="tab"
              aria-selected={windowHours === h}
              className={"seg-item" + (windowHours === h ? " seg-active" : "")}
              onClick={() => setWindowHours(h)}
            >
              {label}
            </button>
          ))}
        </div>
        {report && (
          <span className="obs-generated">
            生成于 {fmtBeijing(report.generated_at)}（北京时间）
          </span>
        )}
      </div>
      {windowHours !== DEFAULT_WINDOW_HOURS && (
        <div className="hint hint-warn">
          当前窗口 {windowHours} 小时：探针红绿灯的口径随之变宽，而契约 §16.2 的回退率红线
          是按 24 小时定的。判 Gate 请切回 24 小时档，其余档位只用来看趋势。
        </div>
      )}

      {loadError && <div className="alert alert-error">{loadError}</div>}
      {loading && !report && (
        <div className="list-loading">
          <span className="spinner spinner-dark" /> 加载中…
        </div>
      )}

      {report && (
        <>
          {/* ── 7 条探针红绿灯 ── */}
          <div className={"obs-verdict " + STATUS_META[worst].cls}>
            <span className="probe-icon" aria-hidden="true">
              {STATUS_META[worst].icon}
            </span>
            {WORST_META[worst]}
          </div>
          <div className="probe-list">
            {report.results.map((r) => (
              // key 带上 status：状态翻转时重挂载，展开态才能按新状态重新取默认值
              <ProbeCard key={`${r.id}:${r.status}`} r={r} />
            ))}
          </div>

          {/* ── 分数分布 ── */}
          <h3 className="section-title">分数分布</h3>
          <section className="card obs-section">
            <ScoreHistogram buckets={report.score_distribution} />
            <p className="muted chart-note">
              横轴为 LLM 原始相关分（区间左闭右开，末桶 90–100 闭合），纵轴为打分次数。
              只统计<strong>解析得出数字</strong>的成功调用：静默回退中位分 50 的那些调用（completion
              里没有数字）根本不在图里——所以"没有 50 尖峰"不等于"没有回退"，
              回退量请看下面的四联计数。
            </p>
          </section>

          {/* ── 每批区分度 ── */}
          <h3 className="section-title">每批区分度（n ≥ 5 的批次）</h3>
          <section className="card obs-section">
            {report.score_traces.length === 0 ? (
              <div className="empty-hint">窗口内没有规模 ≥5 的打分批次。</div>
            ) : (
              <div className="table-wrap">
                <table className="table">
                  <thead>
                    <tr>
                      <th>trace</th>
                      <th>开始（北京时间）</th>
                      <th className="num">打分次数 n</th>
                      <th className="num">不同输出</th>
                    </tr>
                  </thead>
                  <tbody>
                    {report.score_traces.map((t) => (
                      <tr key={t.trace_id} className={t.distinct_completions === 1 ? "row-bad" : ""}>
                        <td className="mono" title={t.trace_id}>
                          {shortTrace(t.trace_id)}
                        </td>
                        <td>{fmtBeijing(t.started_at)}</td>
                        <td className="num">{t.n}</td>
                        <td className="num">
                          {t.distinct_completions}
                          {t.distinct_completions === 1 && (
                            <span className="badge badge-bad badge-inline">整批同分</span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            <p className="muted chart-note">
              "不同输出"数的是模型<strong>原话</strong>去重（"85" 与 "85分" 算两种），不是夹逼后的分数——
              这个方向只会让计数偏高、更不容易误报，而 M3 事故那种整批逐字节相同的 "50"
              依然会掉到 1。
            </p>
          </section>

          {/* ── 打分质量四联计数 ── */}
          <h3 className="section-title">打分质量</h3>
          <section className="card obs-section">
            <div className="stat-row">
              <Stat label="成功调用 ok_total" value={q?.ok_total ?? 0} />
              <Stat
                label="无数字 no_digit（回退 50）"
                value={q?.no_digit ?? 0}
                tone={q && q.no_digit > 0 ? "stat-warn" : undefined}
              />
              <Stat
                label="空输出无报错 empty_no_error"
                value={q?.empty_no_error ?? 0}
                tone={q && q.empty_no_error > 0 ? "stat-bad" : undefined}
              />
              <Stat label="调用失败 errored" value={q?.errored ?? 0} />
            </div>
            <p className="muted chart-note">
              四者关系：empty_no_error ⊂ no_digit ⊂ ok_total，errored 与前三者互斥。
              errored 的条目被 pipeline 直接跳过、<strong>一分未发</strong>，所以不算进回退率的分母——
              否则一次上游 429 抖动就能冲爆 10% 红线。empty_no_error 是 M3 事故的精确形状，
              零容忍。
            </p>
          </section>

          {/* ── 画像注入 ── */}
          <h3 className="section-title">画像注入</h3>
          <section className="card obs-section">
            <div className="stat-row">
              <Stat label="打分总数 total" value={inj?.total ?? 0} />
              <Stat label="注入真实画像 present" value={inj?.present ?? 0} />
              <Stat
                label="拿到「暂无」absent"
                value={inj?.absent ?? 0}
                tone={inj && inj.absent > 0 && ev?.has_profile ? "stat-bad" : undefined}
              />
              <Stat
                label="无法识别 unrecognized"
                value={inj?.unrecognized ?? 0}
                tone={inj && inj.unrecognized > 0 ? "stat-bad" : undefined}
              />
            </div>
            <p className="muted chart-note">
              unrecognized 是探针的自检位，恒应为 0：它 &gt;0 说明 scorer 的 prompt 结构变了
              而探针字面量没跟上——那是探针坏了，不是画像坏了，先修探针再谈判定。
              owner 尚无画像时 absent 全中是<strong>正确行为</strong>，不是故障。
            </p>
            <dl className="kv-grid obs-negtail">
              <div>
                <dt>负面清单保尾</dt>
                <dd>
                  {report.neg_tail.expected_tail === ""
                    ? "当前画像无「不感兴趣：」句，不适用"
                    : `${report.neg_tail.intact} / ${report.neg_tail.total} 条打分完整含负面句`}
                </dd>
              </div>
              {report.neg_tail.expected_tail !== "" && (
                <div>
                  <dt>期望串</dt>
                  <dd className="mono wrap">{report.neg_tail.expected_tail}</dd>
                </div>
              )}
            </dl>
          </section>

          {/* ── 日成本 ── */}
          <h3 className="section-title">日成本（按 UTC 日 × span）</h3>
          <section className="card obs-section">
            {costDays.length === 0 ? (
              <div className="empty-hint">窗口内没有 LLM 调用。</div>
            ) : (
              <div className="table-wrap">
                <table className="table">
                  <thead>
                    <tr>
                      <th>UTC 日</th>
                      <th>span</th>
                      <th className="num">调用</th>
                      <th className="num">成本 USD</th>
                    </tr>
                  </thead>
                  <tbody>
                    {/* 一天一个 Fragment：日分组要出「当日合计」行，
                        用 tbody 分组会让斑马纹和边框在每组重来一遍 */}
                    {costDays.map((g) => (
                      <Fragment key={g.day}>
                        {g.rows.map((c, i) => (
                          <tr
                            key={`${g.day}:${c.span_name}`}
                            className={c.span_name === "score" ? "row-mark" : ""}
                          >
                            <td className="mono">{i === 0 ? fmtUTCDay(g.day) : ""}</td>
                            <td className="mono">
                              {c.span_name}
                              {c.span_name === "score" && (
                                <span className="badge badge-type">红线口径</span>
                              )}
                            </td>
                            <td className="num">{c.calls}</td>
                            <td className="num mono">{fmtUSD(c.cost_usd)}</td>
                          </tr>
                        ))}
                        <tr className="row-total">
                          <td />
                          <td>当日合计</td>
                          <td className="num">{g.calls}</td>
                          <td className="num mono">{fmtUSD(g.cost)}</td>
                        </tr>
                      </Fragment>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            <p className="muted chart-note">
              日界是 UTC（DB 原生），<strong>不是北京日</strong>：一个 UTC 日的桶装的是北京 08:00 到次日
              08:00 的调用，故此列刻意不做时区换算。环比红线只卡 score span——M5 新增的
              profile_evolve / deep_dive 是全新 span，全 span 环比测的是"上了新功能"而非
              "注入变贵"。另：cost_usd 逐行舍入后求和，score 最便宜（MaxTokens=16），
              整批舍成 0 是正常的。
            </p>
          </section>

          {/* ── model 用量（计价漂移探测）── */}
          <h3 className="section-title">model 用量</h3>
          <section className="card obs-section">
            {report.models.length === 0 ? (
              <div className="empty-hint">窗口内没有 LLM 调用。</div>
            ) : (
              <div className="table-wrap">
                <table className="table">
                  <thead>
                    <tr>
                      <th>model</th>
                      <th className="num">调用</th>
                      <th className="num">成本 USD</th>
                    </tr>
                  </thead>
                  <tbody>
                    {report.models.map((m) => (
                      <tr key={m.model}>
                        <td className="mono">{m.model || "（空）"}</td>
                        <td className="num">{m.calls}</td>
                        <td className="num mono">{fmtUSD(m.cost_usd)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            <p className="muted chart-note">
              这里的 model 是<strong>上游报回的名字</strong>。计价按它查价，未知 key 静默回落 v4-pro 价
              （约 flash 的 3 倍），且不产生任何 error——上游一次改名就能无声烧穿预算。
              盯着这张表出现陌生名字，是唯一能提前看见它的角度。
            </p>
          </section>

          {/* ── 演化健康 ── */}
          <h3 className="section-title">演化健康</h3>
          <section className="card obs-section">
            <dl className="kv-grid">
              <div>
                <dt>窗口内演化调用</dt>
                <dd>
                  {ev?.calls ?? 0} 次
                  {ev && ev.errored > 0 && (
                    <span className="badge badge-bad">失败 {ev.errored}</span>
                  )}
                </dd>
              </div>
              <div>
                <dt>最近一次演化调用</dt>
                <dd>{ev?.last_call_at ? fmtBeijing(ev.last_call_at) : "从未演化"}</dd>
              </div>
              <div>
                <dt>画像更新于</dt>
                <dd>{ev?.has_profile ? fmtBeijing(ev.profile_updated_at) : "owner 尚无画像"}</dd>
              </div>
              <div>
                <dt>反馈游标 cursor</dt>
                <dd className="mono">{ev?.cursor ?? 0}</dd>
              </div>
              <div>
                <dt>标签数</dt>
                <dd>{ev?.tag_count ?? 0}</dd>
              </div>
              <div>
                <dt>summary 字数</dt>
                <dd>{ev?.summary_runes ?? 0}</dd>
              </div>
            </dl>
            <p className="muted chart-note">
              "最近一次演化调用"不受窗口约束（要拿它和画像 updated_at 比先后，上次演化可能
              落在窗口外）。画像 updated_at 早于最近一次演化调用 = 这批反馈没写回画像。
              注意 updated_at 无法归因写入者：人工改画像也会刷新它。
            </p>
          </section>

          {/* ── 推送批次历史 ── */}
          <h3 className="section-title">推送批次历史（近 14 天）</h3>
          <div className="alert obs-gap-note">
            <strong>怎么读这张表：空批次现在有行了，跑崩的运行仍然没有。</strong>
            pipeline 五处提前退出（无新内容 / 去重后空 / 打分后空 / 择优后空 / 卡片生成后空）
            现在各留一行 <code>status=empty</code> 的批次，「闸门」列说明从哪一步退的，
            「漏斗」列说明到那一步还剩几条——"今早没新内容"从此在库里查得到，
            且它是<strong>正常终态不是事故</strong>，故给静音灰，不给告警色。
            但 <strong>pipeline 中途报错的运行仍然没有行</strong>：Fetch/Score 等活动重试耗尽后
            workflow 直接失败返回，走不到任何闸门；那类运行在 Temporal 里是 Failed
            （另有 journalctl 错误日志），本表看不见。所以这张表的语义是
            <strong>"推送决策的产物"，不是"每次触发的流水账"</strong>——看不到某天的行，
            仍可能是那天跑崩了。
          </div>
          <section className="card obs-section">
            {report.batches.length === 0 ? (
              <div className="empty-hint">近 14 天没有建成的推送批次。</div>
            ) : (
              <div className="table-wrap">
                <table className="table">
                  <thead>
                    <tr>
                      <th className="num">id</th>
                      <th>状态</th>
                      <th>闸门</th>
                      <th>漏斗</th>
                      <th>创建（北京时间）</th>
                      <th className="num">投递</th>
                      <th className="num">已发</th>
                      <th className="num">原始分 最低–最高</th>
                    </tr>
                  </thead>
                  <tbody>
                    {report.batches.map((b) => {
                      const gate = GATE_META[b.exit_gate];
                      return (
                        <tr key={b.id} className={b.status === "failed" ? "row-bad" : ""}>
                          <td className="num mono">{b.id}</td>
                          <td>
                            <span className={"badge " + batchBadge(b.status)}>{b.status}</span>
                          </td>
                          {/* 空串 = 没有提前退出（跑到了 Push）→ 本列无话可说，显示"—"。
                              未知枚举值（后端加了新闸门而前端没跟上）不吞掉：原样显示
                              英文串，让"没跟上"露出来，而不是静默变成"—"装作没有闸门。 */}
                          <td title={gate?.hint}>
                            {b.exit_gate === "" ? (
                              <span className="muted">—</span>
                            ) : (
                              <span className="badge badge-muted">{gate?.label ?? b.exit_gate}</span>
                            )}
                          </td>
                          <td className="mono">
                            <Funnel counts={b.stage_counts} />
                          </td>
                          <td title={`幂等键 / traceID：${b.idempotency_key}`}>
                            {fmtBeijing(b.created_at)}
                          </td>
                          {/* 投递 0 只有在**该推**的批次上才是异常。empty 批次本就没有候选，
                              0 是它的正确值——标红会把每个"今早没新内容"渲染成一起事故，
                              让人去查一个没出问题的早晨。failed 的 0 投递仍然红。 */}
                          <td
                            className={
                              "num" +
                              (b.status !== "empty" && b.delivery_count === 0 ? " cell-bad" : "")
                            }
                          >
                            {b.delivery_count}
                          </td>
                          <td className="num">{b.sent_count}</td>
                          <td className="num mono">
                            {b.min_score === undefined || b.max_score === undefined
                              ? "—"
                              : `${b.min_score.toFixed(0)}–${b.max_score.toFixed(0)}`}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
            <p className="muted chart-note">
              「闸门」非空 ⇔ 该批在 Push 之前就没候选了（status 恒为 empty）。「漏斗」只画
              <strong>真跑过</strong>的阶段：<code>20→0</code> 是"抓到 20 条、去重后剩 0"，后面的打分/择优
              压根没被调用——所以那里没有数字，而不是 0。两列都显示"—"的行是 008 之前的历史
              批次（那时没有这两个列）或跑到了 Push 的批次，那是<strong>没有记录</strong>，不是"计数为 0"。
              投递数为 0 只在 done/failed 批次上才是异常（批次建了但一条投递都没插成）；
              empty 批次没有投递是正确的。分数是 LLM <strong>原始相关分</strong>，不是排序用的有效分——
              有效分含时新度衰减、只在推送时刻内存中存在、从不落库，所以卡片里"分数高的排在
              后面"是正常的。鼠标悬停"创建"列可看该批的幂等键（= traceID），据此可与上面的
              区分度表对上。
            </p>
          </section>
        </>
      )}
    </div>
  );
}

// 批次状态徽标配色。done 不给绿：批次"跑完了"不等于"推成了"——
// sent_count 才是真的送达数，绿灯留给探针，别在这里制造第二个安全感来源。
//
// empty 给静音灰，与 done 一眼可分但**不给告警色**：它是"跑完了确实没东西可推"的
// 正常终态（008 起的真状态），把它染红等于报一起假事故，让人去查一个没出问题的早晨。
// 与 failed 的区别是根本性的：failed 是"该推却推不出去"（有卡片、推送炸了），
// 混成一个颜色就等于把"飞书挂了"和"今天没新闻"报成同一件事。
//
// pending 几乎不该出现：它只在 Push 活动内部短暂存在，落库即改终态。
// 真看到 pending 说明 Push 中途崩了，是异常，故与 failed 同样不给中性色。
// （"pushing" 这个死枚举已于 008 从 types/enums.go 删除，本函数从来就没为它留过分支。）
function batchBadge(status: string): string {
  if (status === "failed" || status === "pending") return "badge-bad";
  if (status === "empty") return "badge-muted";
  return "badge-type";
}

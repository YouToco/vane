import { useEffect, useState } from "react";
import { Fragment } from "react";
import { api, ApiError } from "../api";
import type { RunstatsResp, SpanDayCost } from "../api";

// 成本与运行监控页（M7 功能 6.5）：llm_calls 的趋势展示视角。
// 与「可观测」页的分工：那边跑 Gate 探针判定（红线），这边纯展示运行指标——
// 本页不参与判断、不调用任何模型，刷新也不会连带跑探针。

const WINDOW_OPTIONS: [number, string][] = [
  [24, "24 小时"],
  [168, "7 天"],
  [720, "30 天"], // 成本要看月度量级；上限对齐后端 maxWindowHours
];
const DEFAULT_WINDOW_HOURS = 24;

// cost_usd 精度纪律同 Observability.tsx 的 fmtUSD：6 位小数是 NUMERIC(10,6)
// 原生精度，toFixed(2) 会把单次 ~$0.0000008 的打分整列抹成 0.00。
function fmtUSD(v: number): string {
  return `$${v.toFixed(6)}`;
}

// token 数直接展示原始整数（千分位），不做 k/M 缩写：
// 预算核对要和上游账单逐位对，缩写反而要人换算。
function fmtInt(v: number): string {
  return v.toLocaleString("en-US");
}

function fmtMs(v: number): string {
  return `${Math.round(v)} ms`;
}

// 日界是 UTC 日（后端 date_trunc AT TIME ZONE 'UTC'），刻意不换算北京——
// 理由同 Observability.tsx 的 fmtUTCDay：标成北京日期就是撒谎。
function fmtUTCDay(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${mm}-${dd}`;
}

interface CostDay {
  day: string;
  rows: SpanDayCost[];
  calls: number;
  cost: number;
}

// 按 UTC 日分组小计（与 Observability.tsx 的 groupCosts 同构；两页各自持有——
// 这 14 行抽共享模块省不了什么，反而把两页的展示演化耦在一起）。
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

export default function Costs() {
  const [report, setReport] = useState<RunstatsResp | null>(null);
  const [windowHours, setWindowHours] = useState(DEFAULT_WINDOW_HOURS);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    api
      .runstats(windowHours)
      .then((r) => {
        if (!alive) return;
        setReport(r);
        setLoadError("");
      })
      .catch((err) => {
        if (!alive) return;
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

  const totals = report
    ? report.spans.reduce(
        (a, s) => ({
          calls: a.calls + s.calls,
          errors: a.errors + s.errors,
          cost: a.cost + s.cost_usd,
          tokens: a.tokens + s.prompt_tokens + s.completion_tokens,
        }),
        { calls: 0, errors: 0, cost: 0, tokens: 0 },
      )
    : null;
  const costDays = report ? groupCosts(report.days) : [];

  return (
    <div className="page">
      <div className="page-head">
        <h2 className="page-title">成本与运行</h2>
        <button
          type="button"
          className="btn btn-ghost btn-mini"
          onClick={() => setNonce((n) => n + 1)}
          disabled={loading}
        >
          {loading ? <span className="spinner spinner-dark" /> : "刷新"}
        </button>
      </div>
      <p className="muted src-intro">
        LLM 调用的成本、token、延迟与缓存命中。数据来自后端只读聚合，本页不参与任何判定。
      </p>

      <div className="obs-toolbar">
        <div className="seg" role="tablist" aria-label="统计窗口">
          {WINDOW_OPTIONS.map(([h, label]) => (
            <button
              key={h}
              type="button"
              role="tab"
              aria-selected={windowHours === h}
              className={windowHours === h ? "seg-item seg-active" : "seg-item"}
              onClick={() => setWindowHours(h)}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      {loadError && <div className="alert alert-error">{loadError}</div>}

      {loading && !report ? (
        <div className="page-loading">
          <span className="spinner spinner-dark" /> 加载中…
        </div>
      ) : report && (
        <>
          {totals && (
            <div className="stat-row" style={{ marginBottom: 16 }}>
              <div className="stat">
                <div className="stat-value">{fmtUSD(totals.cost)}</div>
                <div className="stat-label">窗口总成本</div>
              </div>
              <div className="stat">
                <div className="stat-value">{fmtInt(totals.calls)}</div>
                <div className="stat-label">LLM 调用次数</div>
              </div>
              <div className="stat">
                <div className="stat-value">{fmtInt(totals.tokens)}</div>
                <div className="stat-label">总 token</div>
              </div>
              {/* 错误 0 是常态，非 0 才值得醒目——按值切换告警底色 */}
              <div className={totals.errors > 0 ? "stat stat-bad" : "stat"}>
                <div className="stat-value">{fmtInt(totals.errors)}</div>
                <div className="stat-label">错误调用</div>
              </div>
            </div>
          )}

          <h3 className="section-title">按环节（span）</h3>
          <section className="card obs-section">
            {report.spans.length === 0 ? (
              <div className="empty-hint">窗口内没有 LLM 调用。</div>
            ) : (
              <div className="table-wrap">
                <table className="table">
                  <thead>
                    <tr>
                      <th>span</th>
                      <th className="num">调用</th>
                      <th className="num">错误</th>
                      <th className="num">成本</th>
                      <th className="num">输入 token</th>
                      <th className="num">输出 token</th>
                      <th className="num">延迟 avg</th>
                      <th className="num">p95</th>
                      <th className="num">缓存命中</th>
                    </tr>
                  </thead>
                  <tbody>
                    {report.spans.map((s) => (
                      <tr key={s.span_name} className={s.errors > 0 ? "row-bad" : ""}>
                        <td className="mono">{s.span_name}</td>
                        <td className="num">{fmtInt(s.calls)}</td>
                        <td className="num">{s.errors > 0 ? fmtInt(s.errors) : "—"}</td>
                        <td className="num mono">{fmtUSD(s.cost_usd)}</td>
                        <td className="num">{fmtInt(s.prompt_tokens)}</td>
                        <td className="num">{fmtInt(s.completion_tokens)}</td>
                        <td className="num">{fmtMs(s.avg_latency_ms)}</td>
                        <td className="num">{fmtMs(s.p95_latency_ms)}</td>
                        {/* 分母是 cache_known 不是 calls：NULL 行不支持缓存，
                            算进分母会凭空压低命中率；全 NULL 显示"—" */}
                        <td className="num">
                          {s.cache_known > 0
                            ? `${Math.round((s.cache_hits / s.cache_known) * 100)}% (${s.cache_hits}/${s.cache_known})`
                            : "—"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>

          <h3 className="section-title">按模型</h3>
          <section className="card obs-section">
            {report.models.length === 0 ? (
              <div className="empty-hint">窗口内没有 LLM 调用。</div>
            ) : (
              <div className="table-wrap">
                <table className="table">
                  <thead>
                    <tr>
                      <th>模型（上游报回名）</th>
                      <th className="num">调用</th>
                      <th className="num">成本</th>
                    </tr>
                  </thead>
                  <tbody>
                    {report.models.map((m) => (
                      <tr key={m.model}>
                        <td className="mono">{m.model}</td>
                        <td className="num">{fmtInt(m.calls)}</td>
                        <td className="num mono">{fmtUSD(m.cost_usd)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>

          <h3 className="section-title">按 UTC 日（span 分列）</h3>
          <section className="card obs-section">
            {costDays.length === 0 ? (
              <div className="empty-hint">窗口内没有成本记录。</div>
            ) : (
              <div className="table-wrap">
                <table className="table">
                  <thead>
                    <tr>
                      <th>UTC 日</th>
                      <th>span</th>
                      <th className="num">调用</th>
                      <th className="num">成本</th>
                    </tr>
                  </thead>
                  <tbody>
                    {costDays.map((g) => (
                      <Fragment key={g.day}>
                        {g.rows.map((c, i) => (
                          <tr key={`${g.day}:${c.span_name}`}>
                            <td className="mono">{i === 0 ? fmtUTCDay(g.day) : ""}</td>
                            <td className="mono">{c.span_name}</td>
                            <td className="num">{fmtInt(c.calls)}</td>
                            <td className="num mono">{fmtUSD(c.cost_usd)}</td>
                          </tr>
                        ))}
                        <tr className="row-total">
                          <td />
                          <td>当日合计</td>
                          <td className="num">{fmtInt(g.calls)}</td>
                          <td className="num mono">{fmtUSD(g.cost)}</td>
                        </tr>
                      </Fragment>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        </>
      )}
    </div>
  );
}

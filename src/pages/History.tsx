import { useEffect, useState } from "react";
import { api, ApiError } from "../api";
import type { DeliveryHistoryItem } from "../api";

// 推送历史与反馈页（M7 功能 6.4）：回溯每条推送的打分、状态与用户反馈。
// 只读页面——反馈的产生入口在飞书卡片（准则：能在卡片上一键做的不搬进 Dashboard），
// 这里是回看与对账的地方，不是第二个反馈入口。

const PAGE_SIZE = 20;

const BEIJING_TZ = "Asia/Shanghai";

// 后端全 UTC，换算只在展示层做；显式 timeZone 的理由同 Observability.tsx。
function fmtBeijing(iso?: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString("zh-CN", { timeZone: BEIJING_TZ, hour12: false });
}

// 投递状态徽标。sent 不给绿：绿灯留给探针（batchBadge 同理），
// 这里 sent 是常态、failed/pending 才是要一眼看到的异常。
function statusBadge(status: string): string {
  if (status === "failed" || status === "pending") return "badge-bad";
  return "badge-muted";
}

// 反馈动作 → 中文标签 + 徽标配色。未知枚举原样显示英文串（后端加了新动作
// 而前端没跟上时让"没跟上"露出来，与 Observability 的闸门列同一策略）。
// showDetail：detail 是否内嵌进徽标——只有**用户亲手输入**的短文本才内嵌
// （misjudged 原因 / question 提问）；deep_dive 的 detail 是 AI 生成的解读
// markdown 全文（生产实测数百字符，内嵌直接把徽标撑爆），只进 title 提示。
const FEEDBACK_META: Record<string, { label: string; badge: string; showDetail: boolean }> = {
  interested: { label: "👍 感兴趣", badge: "badge-ok", showDetail: false },
  not_interested: { label: "👎 不感兴趣", badge: "badge-muted", showDetail: false },
  misjudged: { label: "⚠️ 误判", badge: "badge-bad", showDetail: true },
  deep_dive: { label: "🔍 深入", badge: "badge-type", showDetail: false },
  question: { label: "💬 追问", badge: "badge-type", showDetail: true },
};

// title 提示里的 detail 截断：浏览器原生 tooltip 放几百字 markdown 同样不可读。
function clipDetail(s: string, max = 120): string {
  const runes = Array.from(s);
  return runes.length <= max ? s : runes.slice(0, max).join("") + "…";
}

export default function History() {
  const [items, setItems] = useState<DeliveryHistoryItem[]>([]);
  const [total, setTotal] = useState(0);
  const [nextToken, setNextToken] = useState("");
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [loadError, setLoadError] = useState("");
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    api
      .listDeliveries(PAGE_SIZE)
      .then((r) => {
        if (!alive) return;
        setItems(r.items);
        setTotal(r.total);
        setNextToken(r.next_page_token ?? "");
        setLoadError("");
      })
      .catch((err) => {
        if (!alive) return;
        setLoadError(err instanceof ApiError ? err.message : "加载失败");
        setItems([]);
        setTotal(0);
        setNextToken("");
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [nonce]);

  // 追加下一页：游标翻页天然不重不漏，直接 concat。
  // 不做 alive 防护外的并发控制：按钮在 loadingMore 期间禁用，同一时刻至多一个在途请求。
  async function loadMore() {
    if (!nextToken || loadingMore) return;
    setLoadingMore(true);
    try {
      const r = await api.listDeliveries(PAGE_SIZE, nextToken);
      setItems((prev) => prev.concat(r.items));
      setTotal(r.total);
      setNextToken(r.next_page_token ?? "");
      setLoadError("");
    } catch (err) {
      setLoadError(err instanceof ApiError ? err.message : "加载失败");
    } finally {
      setLoadingMore(false);
    }
  }

  return (
    <div className="page">
      <div className="page-head">
        <h2 className="page-title">推送历史</h2>
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
        每条推送的打分、发送状态与你在飞书卡片上的反馈。反馈请在卡片上操作，这里只做回看。
      </p>

      {loadError && <div className="alert alert-error">{loadError}</div>}

      {loading ? (
        <div className="page-loading">
          <span className="spinner spinner-dark" /> 加载中…
        </div>
      ) : items.length === 0 ? (
        !loadError && <div className="empty-hint">还没有推送记录。</div>
      ) : (
        <section className="card">
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>推送时间（北京）</th>
                  <th>内容</th>
                  <th className="num">分数</th>
                  <th>状态</th>
                  <th>反馈</th>
                </tr>
              </thead>
              <tbody>
                {items.map((it) => (
                  <tr key={it.id} className={it.status === "failed" ? "row-bad" : ""}>
                    {/* sent_at 才是真正到达用户的时刻；未发送（pending/failed）回落
                        created_at 并在状态列可见原因，不在时间列伪装成"已推送" */}
                    <td title={`delivery ${it.id} · batch ${it.batch_id}`}>
                      {fmtBeijing(it.sent_at ?? it.created_at)}
                    </td>
                    <td className="hist-title-cell">
                      {it.url ? (
                        <a href={it.url} target="_blank" rel="noreferrer">
                          {it.title || "（无内容）"}
                        </a>
                      ) : (
                        // content_item 被删（ON DELETE SET NULL）时 title/url 均空串
                        <span className="muted">{it.title || "（内容已删除）"}</span>
                      )}
                    </td>
                    <td className="num mono">{it.score}</td>
                    <td>
                      <span className={"badge " + statusBadge(it.status)}>{it.status}</span>
                    </td>
                    <td>
                      {it.feedbacks.length === 0 ? (
                        <span className="muted">—</span>
                      ) : (
                        <div className="hist-feedbacks">
                          {it.feedbacks.map((fb, i) => {
                            const meta = FEEDBACK_META[fb.action];
                            return (
                              <span
                                key={i}
                                className={"badge " + (meta?.badge ?? "badge-muted")}
                                title={fmtBeijing(fb.created_at) + (fb.detail ? ` · ${clipDetail(fb.detail)}` : "")}
                              >
                                {meta?.label ?? fb.action}
                                {meta?.showDetail && fb.detail && (
                                  <span className="hist-fb-detail">{clipDetail(fb.detail, 30)}</span>
                                )}
                              </span>
                            );
                          })}
                        </div>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="hist-pager">
            <span className="muted">
              已显示 {items.length} / {total} 条
            </span>
            {nextToken && (
              <button type="button" className="btn btn-mini" onClick={loadMore} disabled={loadingMore}>
                {loadingMore ? "加载中…" : "加载更多"}
              </button>
            )}
          </div>
        </section>
      )}
    </div>
  );
}

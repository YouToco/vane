import { useEffect, useState } from "react";
import { api, ApiError } from "../api";
import type { Source } from "../api";

// 信源管理页 —— 纯固化组件（遵循 ui-interaction-principles.md）。
// M3 固化范围：填 URL 加订阅、列表增删、抓取状态灯。
// 自然语言加源（「帮我关注 XX」）是 M4 的 AI 预填能力，本页不碰。

// 轻提示：与定时任务页同款瞬态条。
type Toast = { kind: "ok" | "err"; text: string } | null;

// 抓取状态灯：active=绿，disabled/error=红（连续失败被自动禁用），其余=灰。
function statusDot(status: string, failCount: number): { cls: string; label: string } {
  if (status === "active") {
    return failCount > 0
      ? { cls: "dot-warn", label: `抓取异常（连续失败 ${failCount} 次）` }
      : { cls: "dot-ok", label: "正常抓取中" };
  }
  if (status === "disabled") return { cls: "dot-bad", label: "已禁用（多次抓取失败）" };
  if (status === "paused") return { cls: "dot-mute", label: "已暂停" };
  return { cls: "dot-mute", label: status };
}

// URL 基本形态校验：只挡明显不是链接的输入，真正的可达性/私网拦截由后端 fetcher 负责（B4）。
function looksLikeUrl(s: string): boolean {
  const t = s.trim();
  return /^https?:\/\/.+\..+/i.test(t);
}

export default function Sources() {
  const [sources, setSources] = useState<Source[] | null>(null);
  const [loadError, setLoadError] = useState("");
  const [url, setUrl] = useState("");
  const [adding, setAdding] = useState(false);
  const [removingId, setRemovingId] = useState<number | null>(null);
  const [toast, setToast] = useState<Toast>(null);

  function flash(t: Toast) {
    setToast(t);
    if (t) setTimeout(() => setToast(null), 2600);
  }

  async function load() {
    try {
      const list = await api.listSubscriptions();
      setSources(list ?? []);
      setLoadError("");
    } catch (err) {
      setLoadError(err instanceof ApiError ? err.message : "加载失败");
      setSources([]);
    }
  }

  useEffect(() => {
    load();
  }, []);

  async function onAdd() {
    const trimmed = url.trim();
    if (!looksLikeUrl(trimmed) || adding) return;
    setAdding(true);
    try {
      await api.addSubscription(trimmed);
      setUrl("");
      await load();
      flash({ kind: "ok", text: "信源已添加" });
    } catch (err) {
      flash({ kind: "err", text: err instanceof ApiError ? err.message : "添加失败" });
    } finally {
      setAdding(false);
    }
  }

  async function onRemove(id: number) {
    setRemovingId(id);
    try {
      await api.removeSubscription(id);
      await load();
      flash({ kind: "ok", text: "已移除" });
    } catch (err) {
      flash({ kind: "err", text: err instanceof ApiError ? err.message : "移除失败" });
    } finally {
      setRemovingId(null);
    }
  }

  const canAdd = looksLikeUrl(url) && !adding;

  return (
    <div className="page">
      <h2 className="page-title">信源管理</h2>
      <p className="muted src-intro">添加 RSS / 订阅源的 URL，见微 Vane 会定期抓取并纳入推送候选。</p>

      {/* ── 添加行 ── */}
      <section className="card src-add">
        <input
          className="input"
          placeholder="https://example.com/feed.xml"
          value={url}
          onChange={(e) => setUrl(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") onAdd();
          }}
          autoComplete="off"
        />
        <button type="button" className="btn btn-primary" onClick={onAdd} disabled={!canAdd}>
          {adding ? <span className="spinner" /> : "添加"}
        </button>
      </section>
      {url.trim() !== "" && !looksLikeUrl(url) && (
        <div className="hint hint-warn">请输入以 http(s):// 开头的完整链接</div>
      )}

      {/* ── 信源列表 ── */}
      {loadError && <div className="alert alert-error">{loadError}</div>}
      {sources === null && !loadError && (
        <div className="list-loading">
          <span className="spinner spinner-dark" /> 加载中…
        </div>
      )}
      {sources !== null && sources.length === 0 && !loadError && (
        <div className="empty-hint">还没有信源，添加第一个订阅源开始吧。</div>
      )}
      <div className="src-list">
        {sources?.map((s) => {
          const dot = statusDot(s.status, s.fail_count);
          return (
            <div key={s.id} className="card src-card">
              <span className={"dot " + dot.cls} title={dot.label} />
              <div className="src-card-main">
                <div className="src-card-title">{s.title || s.url}</div>
                <div className="src-card-meta">
                  <a className="src-url" href={s.url} target="_blank" rel="noreferrer">
                    {s.url}
                  </a>
                  {s.last_fetched_at && (
                    <span className="src-fetched">
                      上次抓取 {new Date(s.last_fetched_at).toLocaleString("zh-CN")}
                    </span>
                  )}
                </div>
              </div>
              <button
                type="button"
                className="btn btn-mini btn-danger"
                onClick={() => onRemove(s.id)}
                disabled={removingId === s.id}
              >
                {removingId === s.id ? <span className="spinner spinner-dark" /> : "移除"}
              </button>
            </div>
          );
        })}
      </div>

      {toast && (
        <div className={"toast " + (toast.kind === "ok" ? "toast-ok" : "toast-err")}>{toast.text}</div>
      )}
    </div>
  );
}

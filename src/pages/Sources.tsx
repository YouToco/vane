import { useEffect, useRef, useState } from "react";
import type { KeyboardEvent } from "react";
import { api, ApiError } from "../api";
import type { AddSubscriptionReq, Source } from "../api";

// 信源管理页 —— 纯固化组件（遵循 ui-interaction-principles.md）。
// M3 固化范围：三类信源（RSS URL / Exa 搜索 / 小红书关键词）分段切换添加、列表增删、抓取状态灯。
// 自然语言加源（「帮我关注 XX」）是 M4 的 AI 预填能力，本页不碰。

// 轻提示：与定时任务页同款瞬态条。
type Toast = { kind: "ok" | "err"; text: string } | null;

// 可添加的信源类型，与后端 types.SourceType 对齐（buildSource 的三个分支）。
type SrcType = "rss" | "exa" | "tikhub_xhs";

// 后端 buildSource 对 query/keyword 的上限（RuneCountInString）。
// 前端用 code point 计数对齐，避免 emoji 被 .length 按 UTF-16 双计误判。
const MAX_PARAM_RUNES = 256;

function runeCount(s: string): number {
  return [...s].length;
}

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

// Enter 提交必须排除输入法组词：组词确认的 keydown 同样是 key="Enter"
// （isComposing=true；Safari 在 compositionend 后还会补发 keyCode 229 的 Enter），
// 不滤会把组到一半的拼音直接建成信源、被 fetcher 拿去烧配额。
function isSubmitEnter(e: KeyboardEvent<HTMLInputElement>): boolean {
  return e.key === "Enter" && !e.nativeEvent.isComposing && e.nativeEvent.keyCode !== 229;
}

// 从后端合成键（exa://search?q=... / tikhub://xhs/search?keyword=...）取查询参数。
// 不走 new URL()：自定义 scheme 各浏览器解析行为不一，直接切 "?" 后的查询串最稳。
function syntheticParam(url: string, param: string): string | null {
  const qs = url.split("?")[1] ?? "";
  return new URLSearchParams(qs).get(param);
}

// 非 RSS 源在列表里的展示信息：类型徽标 + 人类可读搜索词。
// 合成键不是可打开的链接，渲染成 <a> 就是死链，所以这两类不给 href。
function typeMeta(s: Source): { badge: string; term: string } | null {
  if (s.type === "exa") {
    const cat = syntheticParam(s.url, "category");
    return {
      badge: cat ? `Exa 搜索 · ${categoryLabel(cat)}` : "Exa 搜索",
      term: syntheticParam(s.url, "q") ?? s.url,
    };
  }
  if (s.type === "tikhub_xhs") {
    return { badge: "小红书", term: syntheticParam(s.url, "keyword") ?? s.url };
  }
  return null;
}

// Exa 可选结果类别（能枚举就别自由输入）；值透传后端 config，"" = 不限。
// 档位与官方 Playground（dashboard.exa.ai Search 页）现行下拉对齐（2026-07-14 核实），
// 顺序也照抄。API 实际接受任意字符串作 hint（非严格枚举）；github 已用真实 key
// 实测强生效（返回全为 github.com 仓库）；pdf 实测偏向弱且文档与 Playground 均无、
// 不收录；tweet / linkedin profile 已移除/被 people 取代。
const EXA_CATEGORIES: [string, string][] = [
  ["", "类别不限"],
  ["company", "公司"],
  ["research paper", "研究论文"],
  ["news", "新闻"],
  ["github", "GitHub"],
  ["personal site", "个人网站"],
  ["people", "人物"],
  ["financial report", "财报"],
];

// category 值 → 表单同款中文标签，徽标与下拉保持同一套叫法；
// 未知值（旧数据行/其他客户端写入的任意 hint）原样回退展示。
function categoryLabel(v: string): string {
  const hit = EXA_CATEGORIES.find(([value]) => value === v);
  return hit ? hit[1] : v;
}

export default function Sources() {
  const [sources, setSources] = useState<Source[] | null>(null);
  const [loadError, setLoadError] = useState("");
  const [srcType, setSrcType] = useState<SrcType>("rss");
  const [url, setUrl] = useState("");
  const [query, setQuery] = useState(""); // exa 搜索词
  const [category, setCategory] = useState(""); // exa 可选类别
  const [keyword, setKeyword] = useState(""); // 小红书关键词
  const [adding, setAdding] = useState(false);
  const [removingId, setRemovingId] = useState<number | null>(null);
  const [toast, setToast] = useState<Toast>(null);
  const toastTimer = useRef<number | undefined>(undefined);

  // 每次 flash 先清掉上一个定时器：否则连续操作时旧定时器会提前掐掉新 Toast。
  function flash(t: Toast) {
    setToast(t);
    clearTimeout(toastTimer.current);
    if (t) toastTimer.current = window.setTimeout(() => setToast(null), 2600);
  }

  useEffect(() => () => clearTimeout(toastTimer.current), []);

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

  // 当前类型的输入是否可提交 + 告警文案（空输入只禁用按钮，不告警）。
  function validate(): { ok: boolean; warn: string } {
    if (srcType === "rss") {
      if (url.trim() === "") return { ok: false, warn: "" };
      return looksLikeUrl(url)
        ? { ok: true, warn: "" }
        : { ok: false, warn: "请输入以 http(s):// 开头的完整链接" };
    }
    const term = (srcType === "exa" ? query : keyword).trim();
    if (term === "") return { ok: false, warn: "" };
    if (runeCount(term) > MAX_PARAM_RUNES) {
      return { ok: false, warn: `搜索词过长（上限 ${MAX_PARAM_RUNES} 字符）` };
    }
    return { ok: true, warn: "" };
  }
  const valid = validate();
  const canAdd = valid.ok && !adding;

  async function onAdd() {
    if (!canAdd) return;
    // 按类型组装与后端契约一致的请求体；rss 不带 type 走后端缺省分支。
    let req: AddSubscriptionReq;
    if (srcType === "rss") {
      req = { url: url.trim() };
    } else if (srcType === "exa") {
      req = category
        ? { type: "exa", query: query.trim(), category }
        : { type: "exa", query: query.trim() };
    } else {
      req = { type: "tikhub_xhs", keyword: keyword.trim() };
    }
    setAdding(true);
    try {
      await api.addSubscription(req);
      if (srcType === "rss") setUrl("");
      else if (srcType === "exa") setQuery("");
      else setKeyword("");
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

  return (
    <div className="page">
      <h2 className="page-title">信源管理</h2>
      <p className="muted src-intro">
        添加 RSS 链接、Exa 搜索词或小红书关键词，见微 Vane 会定期抓取并纳入推送候选。
      </p>

      {/* ── 添加卡片：类型分段控件 + 对应输入行 ── */}
      <section className="card src-add">
        <div className="seg" role="tablist" aria-label="信源类型">
          {(
            [
              ["rss", "RSS"],
              ["exa", "Exa 搜索"],
              ["tikhub_xhs", "小红书关键词"],
            ] as [SrcType, string][]
          ).map(([t, label]) => (
            <button
              key={t}
              type="button"
              role="tab"
              aria-selected={srcType === t}
              className={"seg-item" + (srcType === t ? " seg-active" : "")}
              onClick={() => setSrcType(t)}
            >
              {label}
            </button>
          ))}
        </div>
        <div className="src-add-row">
          {srcType === "rss" && (
            <input
              className="input"
              placeholder="https://example.com/feed.xml"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              onKeyDown={(e) => {
                if (isSubmitEnter(e)) onAdd();
              }}
              autoComplete="off"
            />
          )}
          {srcType === "exa" && (
            <>
              <input
                className="input"
                placeholder="输入搜索词，如：AI Agent 落地案例"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                onKeyDown={(e) => {
                  if (isSubmitEnter(e)) onAdd();
                }}
                autoComplete="off"
              />
              <select
                className="input src-category"
                aria-label="结果类别"
                value={category}
                onChange={(e) => setCategory(e.target.value)}
              >
                {EXA_CATEGORIES.map(([v, label]) => (
                  <option key={v} value={v}>
                    {label}
                  </option>
                ))}
              </select>
            </>
          )}
          {srcType === "tikhub_xhs" && (
            <input
              className="input"
              placeholder="输入小红书搜索关键词，如：手冲咖啡"
              value={keyword}
              onChange={(e) => setKeyword(e.target.value)}
              onKeyDown={(e) => {
                if (isSubmitEnter(e)) onAdd();
              }}
              autoComplete="off"
            />
          )}
          <button type="button" className="btn btn-primary" onClick={onAdd} disabled={!canAdd}>
            {adding ? <span className="spinner" /> : "添加"}
          </button>
        </div>
      </section>
      {valid.warn && <div className="hint hint-warn">{valid.warn}</div>}

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
          const meta = typeMeta(s);
          return (
            <div key={s.id} className="card src-card">
              <span className={"dot " + dot.cls} title={dot.label} />
              <div className="src-card-main">
                <div className="src-card-title">{s.title || s.url}</div>
                <div className="src-card-meta">
                  {meta ? (
                    <>
                      <span className="badge badge-type">{meta.badge}</span>
                      <span className="src-term">{meta.term}</span>
                    </>
                  ) : (
                    <a className="src-url" href={s.url} target="_blank" rel="noreferrer">
                      {s.url}
                    </a>
                  )}
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

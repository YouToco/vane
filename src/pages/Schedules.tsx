import { useEffect, useMemo, useState } from "react";
import { api, ApiError } from "../api";
import type { Schedule, ScheduleSpec } from "../api";

// 定时任务页 —— 纯固化组件（遵循 ui-interaction-principles.md）。
// 铁律：cron 绝不让 AI 生成，也不接受用户手输任意 cron；
// 前端把「时间选择器 + 频率档位」这类有限取值编译成结构化 spec，直接命中确定性 API（B8）。
// 后端只接受白名单化的频率档位，最小间隔 1h（B7），这里 UI 层同样挡死。

const TZ = "Asia/Shanghai"; // M3 单 owner，时区固定；spec_json 示例即用此值（B2）

// 频率档位：三选一。每天/每周走 cron，自定义间隔走 every_seconds。
type FreqMode = "daily" | "weekly" | "interval";

// 星期：cron 的 day-of-week 约定 0=周日 … 6=周六。下标即 cron 数值，省去映射表。
const WEEKDAYS = ["日", "一", "二", "三", "四", "五", "六"];

interface ScheduleForm {
  mode: FreqMode;
  time: string; // "HH:MM"，来自 <input type="time">，每天/每周用
  weekdays: number[]; // 0-6，每周用；至少选 1 天
  intervalHours: number; // 自定义间隔小时数，≥1（1h 硬地板，B7）
}

const DEFAULT_FORM: ScheduleForm = {
  mode: "daily",
  time: "08:00",
  weekdays: [1, 2, 3, 4, 5], // 默认工作日
  intervalHours: 6,
};

// ── 纯函数：把 "HH:MM" 拆成 [时, 分]，非法输入兜底 08:00 ──
// 抽出来便于单测：给定字符串必得确定的两个数。
export function parseTime(hhmm: string): [number, number] {
  const m = /^(\d{1,2}):(\d{1,2})$/.exec(hhmm.trim());
  if (!m) return [8, 0];
  const h = Math.min(23, Math.max(0, Number(m[1])));
  const min = Math.min(59, Math.max(0, Number(m[2])));
  return [h, min];
}

// ── 纯函数 buildCron：把「每天/每周 + 时刻」编译成 5 段 cron 表达式 ──
// 格式固定为 "分 时 * * 周"：
//   每天  → "0 8 * * *"        （周位 = *）
//   每周  → "0 8 * * 1,3,5"    （周位 = 升序去重的 day-of-week 列表）
// 只处理这两档；自定义间隔不走 cron（见 buildSpec）。weekdays 传空视为每天。
export function buildCron(hour: number, minute: number, weekdays: number[]): string {
  const dow =
    weekdays.length === 0
      ? "*"
      : [...new Set(weekdays)].sort((a, b) => a - b).join(",");
  return `${minute} ${hour} * * ${dow}`;
}

// ── 纯函数 buildSpec：表单 → 后端中立 spec（cron 或 every_seconds，二选一）──
// 这是「时间选择器编译成结构化 spec」的唯一出口，副作用前的最后一道确定性变换。
export function buildSpec(form: ScheduleForm): ScheduleSpec {
  if (form.mode === "interval") {
    const hours = Math.max(1, Math.floor(form.intervalHours)); // 1h 硬地板，与后端 B7 校验对齐
    return { every_seconds: hours * 3600, tz: TZ };
  }
  const [hour, minute] = parseTime(form.time);
  // 每天 = 周位 *；每周 = 传入选中的星期
  const weekdays = form.mode === "weekly" ? form.weekdays : [];
  return { cron: buildCron(hour, minute, weekdays), tz: TZ };
}

// ── 纯函数 describeSpec：把已存的 spec 反解成中文，供列表卡片展示「下次触发」旁的频率说明 ──
// 后端 GET 只回镜像（B8），无 next_run 时用它兜底成人话，用户仍看得懂这条调度多久推一次。
export function describeSpec(spec: ScheduleSpec): string {
  if (typeof spec.every_seconds === "number" && spec.every_seconds > 0) {
    const h = Math.round(spec.every_seconds / 3600);
    return h % 24 === 0 && h >= 24 ? `每 ${h / 24} 天` : `每 ${h} 小时`;
  }
  if (spec.cron) {
    const parts = spec.cron.trim().split(/\s+/);
    if (parts.length === 5) {
      const [mm, hh, , , dow] = parts;
      const clock = `${hh.padStart(2, "0")}:${mm.padStart(2, "0")}`;
      if (dow === "*") return `每天 ${clock}`;
      const days = dow
        .split(",")
        .map((d) => WEEKDAYS[Number(d)] ?? d)
        .join("、");
      return `每周${days} ${clock}`;
    }
  }
  return "自定义频率";
}

// 轻提示：短暂浮现的成功/信息条，纯前端瞬态，不入 URL、不落库。
type Toast = { kind: "ok" | "err"; text: string } | null;

export default function Schedules() {
  const [schedules, setSchedules] = useState<Schedule[] | null>(null);
  const [loadError, setLoadError] = useState("");

  const [form, setForm] = useState<ScheduleForm>(DEFAULT_FORM);
  const [nlDesc, setNlDesc] = useState("");
  const [creating, setCreating] = useState(false);
  const [pushing, setPushing] = useState(false);
  const [deletingId, setDeletingId] = useState<string | null>(null);
  const [toast, setToast] = useState<Toast>(null);

  function flash(t: Toast) {
    setToast(t);
    if (t) setTimeout(() => setToast(null), 2600);
  }

  async function load() {
    try {
      const list = await api.listSchedules();
      setSchedules(list);
      setLoadError("");
    } catch (err) {
      setLoadError(err instanceof ApiError ? err.message : "加载失败");
      setSchedules([]);
    }
  }

  useEffect(() => {
    load();
  }, []);

  // 表单能否提交：每周档位必须至少选一天，间隔档位小时数 ≥1
  const canCreate = useMemo(() => {
    if (creating) return false;
    if (form.mode === "weekly") return form.weekdays.length > 0;
    if (form.mode === "interval") return form.intervalHours >= 1;
    return true;
  }, [form, creating]);

  // 实时预览：把当前表单编译成 spec 再反解成人话，让用户点「创建」前就看清将要生成什么
  const previewText = useMemo(() => describeSpec(buildSpec(form)), [form]);

  function toggleWeekday(d: number) {
    setForm((f) => ({
      ...f,
      weekdays: f.weekdays.includes(d)
        ? f.weekdays.filter((x) => x !== d)
        : [...f.weekdays, d],
    }));
  }

  async function onCreate() {
    if (!canCreate) return;
    setCreating(true);
    try {
      const spec = buildSpec(form);
      // nl_description 缺省用预览文案兜底，列表卡片才有可读标题
      const desc = nlDesc.trim() || previewText;
      await api.createSchedule(spec, {}, desc);
      setNlDesc("");
      await load();
      flash({ kind: "ok", text: "定时任务已创建" });
    } catch (err) {
      flash({ kind: "err", text: err instanceof ApiError ? err.message : "创建失败" });
    } finally {
      setCreating(false);
    }
  }

  async function onPushNow() {
    setPushing(true);
    try {
      await api.pushNow();
      flash({ kind: "ok", text: "已触发一次推送，去飞书查收" });
    } catch (err) {
      flash({ kind: "err", text: err instanceof ApiError ? err.message : "触发失败" });
    } finally {
      setPushing(false);
    }
  }

  async function onDelete(id: string) {
    setDeletingId(id);
    try {
      await api.deleteSchedule(id);
      await load();
      flash({ kind: "ok", text: "已删除" });
    } catch (err) {
      flash({ kind: "err", text: err instanceof ApiError ? err.message : "删除失败" });
    } finally {
      setDeletingId(null);
    }
  }

  return (
    <div className="page">
      <div className="page-head">
        <h2 className="page-title">定时任务</h2>
        <button type="button" className="btn btn-ghost btn-push-now" onClick={onPushNow} disabled={pushing}>
          {pushing ? <span className="spinner spinner-dark" /> : "⚡ 现在推一次"}
        </button>
      </div>

      {/* ── 创建卡片：时间选择器 + 频率分段控件 ── */}
      <section className="card sched-builder">
        <div className="seg" role="tablist" aria-label="频率">
          {(
            [
              ["daily", "每天"],
              ["weekly", "每周"],
              ["interval", "自定义间隔"],
            ] as [FreqMode, string][]
          ).map(([mode, label]) => (
            <button
              key={mode}
              type="button"
              role="tab"
              aria-selected={form.mode === mode}
              className={"seg-item" + (form.mode === mode ? " seg-active" : "")}
              onClick={() => setForm((f) => ({ ...f, mode }))}
            >
              {label}
            </button>
          ))}
        </div>

        <div className="sched-controls">
          {form.mode !== "interval" && (
            <label className="field">
              <span className="field-label">推送时刻</span>
              <input
                className="input input-time"
                type="time"
                value={form.time}
                onChange={(e) => setForm((f) => ({ ...f, time: e.target.value }))}
              />
            </label>
          )}

          {form.mode === "weekly" && (
            <label className="field">
              <span className="field-label">星期</span>
              <div className="weekday-row">
                {WEEKDAYS.map((w, d) => (
                  <button
                    key={d}
                    type="button"
                    className={"weekday" + (form.weekdays.includes(d) ? " weekday-on" : "")}
                    onClick={() => toggleWeekday(d)}
                    aria-pressed={form.weekdays.includes(d)}
                  >
                    {w}
                  </button>
                ))}
              </div>
            </label>
          )}

          {form.mode === "interval" && (
            <label className="field">
              <span className="field-label">每隔（小时）</span>
              <input
                className="input input-num"
                type="number"
                min={1}
                max={168}
                value={form.intervalHours}
                onChange={(e) =>
                  setForm((f) => ({ ...f, intervalHours: Math.max(1, Number(e.target.value) || 1) }))
                }
              />
            </label>
          )}
        </div>

        <label className="field">
          <span className="field-label">备注（可选）</span>
          <input
            className="input"
            placeholder={`如「${previewText}推科技」，留空则用「${previewText}」`}
            value={nlDesc}
            onChange={(e) => setNlDesc(e.target.value)}
            maxLength={60}
          />
        </label>

        <div className="sched-foot">
          <div className="sched-preview">
            <span className="preview-chip">{previewText}</span>
            <span className="preview-tz">时区 {TZ}</span>
          </div>
          <button type="button" className="btn btn-primary" onClick={onCreate} disabled={!canCreate}>
            {creating ? <span className="spinner" /> : "创建"}
          </button>
        </div>
        {form.mode === "weekly" && form.weekdays.length === 0 && (
          <div className="hint hint-warn">请至少选择一天</div>
        )}
      </section>

      {/* ── 现有调度列表 ── */}
      <h3 className="section-title">已创建的调度</h3>
      {loadError && <div className="alert alert-error">{loadError}</div>}
      {schedules === null && !loadError && (
        <div className="list-loading">
          <span className="spinner spinner-dark" /> 加载中…
        </div>
      )}
      {schedules !== null && schedules.length === 0 && !loadError && (
        <div className="empty-hint">还没有定时任务，用上面的选择器创建第一个吧。</div>
      )}
      <div className="sched-list">
        {schedules?.map((s) => (
          <div key={s.id} className={"card sched-card" + (s.status === "paused" ? " is-paused" : "")}>
            <div className="sched-card-main">
              <div className="sched-card-title">{s.nl_description || describeSpec(s.spec)}</div>
              <div className="sched-card-meta">
                <span className="meta-freq">{describeSpec(s.spec)}</span>
                {s.next_run && (
                  <span className="meta-next">
                    下次 {new Date(s.next_run).toLocaleString("zh-CN")}
                  </span>
                )}
                {s.status === "paused" && <span className="badge badge-paused">已暂停</span>}
              </div>
            </div>
            <button
              type="button"
              className="btn btn-mini btn-danger"
              onClick={() => onDelete(s.id)}
              disabled={deletingId === s.id}
            >
              {deletingId === s.id ? <span className="spinner spinner-dark" /> : "删除"}
            </button>
          </div>
        ))}
      </div>

      {toast && (
        <div className={"toast " + (toast.kind === "ok" ? "toast-ok" : "toast-err")}>{toast.text}</div>
      )}
    </div>
  );
}

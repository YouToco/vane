import { useEffect, useMemo, useState } from "react";
import { toast } from "sonner";
import { Calendar, Clock, Loader2, Plus, Timer, Trash2, Zap } from "lucide-react";
import { api, ApiError } from "../api";
import type { Schedule, ScheduleSpec } from "../api";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";

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

export default function Schedules() {
  const [schedules, setSchedules] = useState<Schedule[] | null>(null);
  const [loadError, setLoadError] = useState("");

  const [form, setForm] = useState<ScheduleForm>(DEFAULT_FORM);
  const [nlDesc, setNlDesc] = useState("");
  const [creating, setCreating] = useState(false);
  const [pushing, setPushing] = useState(false);
  const [deletingId, setDeletingId] = useState<string | null>(null);

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
      toast.success("定时任务已创建");
    } catch (err) {
      toast.error(err instanceof ApiError ? err.message : "创建失败");
    } finally {
      setCreating(false);
    }
  }

  async function onPushNow() {
    setPushing(true);
    try {
      await api.pushNow();
      toast.success("已触发一次推送，去飞书查收");
    } catch (err) {
      toast.error(err instanceof ApiError ? err.message : "触发失败");
    } finally {
      setPushing(false);
    }
  }

  async function onDelete(id: string) {
    setDeletingId(id);
    try {
      await api.deleteSchedule(id);
      await load();
      toast.success("已删除");
    } catch (err) {
      toast.error(err instanceof ApiError ? err.message : "删除失败");
    } finally {
      setDeletingId(null);
    }
  }

  return (
    <div className="max-w-2xl mx-auto px-4 py-6 space-y-6">
      {/* ── 页头 ── */}
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold tracking-tight">定时任务</h1>
        <Button variant="outline" onClick={onPushNow} disabled={pushing}>
          {pushing ? (
            <Loader2 className="size-4 animate-spin" />
          ) : (
            <Zap className="size-4" />
          )}
          {pushing ? "推送中…" : "现在推一次"}
        </Button>
      </div>

      {/* ── 创建卡片：频率分段 + 时间选择器 ── */}
      <Card>
        <CardHeader>
          <CardTitle>新建调度</CardTitle>
        </CardHeader>
        <CardContent className="space-y-5">
          {/* 频率模式 Tabs */}
          <Tabs
            value={form.mode}
            onValueChange={(v) => {
              if (v != null) setForm((f) => ({ ...f, mode: v as FreqMode }));
            }}
          >
            <TabsList className="w-full">
              <TabsTrigger value="daily" className="flex-1">
                <Calendar className="size-3.5" />
                每天
              </TabsTrigger>
              <TabsTrigger value="weekly" className="flex-1">
                <Clock className="size-3.5" />
                每周
              </TabsTrigger>
              <TabsTrigger value="interval" className="flex-1">
                <Timer className="size-3.5" />
                自定义间隔
              </TabsTrigger>
            </TabsList>

            {/* 每天：只选时刻 */}
            <TabsContent value="daily" className="mt-4">
              <div className="space-y-1.5">
                <Label>推送时刻</Label>
                <Input
                  type="time"
                  value={form.time}
                  onChange={(e) => setForm((f) => ({ ...f, time: e.target.value }))}
                  className="w-36"
                />
              </div>
            </TabsContent>

            {/* 每周：时刻 + 星期多选 */}
            <TabsContent value="weekly" className="mt-4 space-y-4">
              <div className="space-y-1.5">
                <Label>推送时刻</Label>
                <Input
                  type="time"
                  value={form.time}
                  onChange={(e) => setForm((f) => ({ ...f, time: e.target.value }))}
                  className="w-36"
                />
              </div>
              <div className="space-y-1.5">
                <Label>推送星期</Label>
                <ToggleGroup variant="outline" spacing={0}>
                  {WEEKDAYS.map((w, d) => (
                    <ToggleGroupItem
                      key={d}
                      pressed={form.weekdays.includes(d)}
                      onPressedChange={() => toggleWeekday(d)}
                      size="sm"
                      aria-label={`周${w}`}
                    >
                      {w}
                    </ToggleGroupItem>
                  ))}
                </ToggleGroup>
                {form.weekdays.length === 0 && (
                  <p className="text-xs text-destructive">请至少选择一天</p>
                )}
              </div>
            </TabsContent>

            {/* 自定义间隔：小时数 */}
            <TabsContent value="interval" className="mt-4">
              <div className="space-y-1.5">
                <Label>每隔（小时）</Label>
                <Input
                  type="number"
                  min={1}
                  max={168}
                  value={form.intervalHours}
                  onChange={(e) =>
                    setForm((f) => ({
                      ...f,
                      intervalHours: Math.max(1, Number(e.target.value) || 1),
                    }))
                  }
                  className="w-36"
                />
              </div>
            </TabsContent>
          </Tabs>

          {/* 备注 */}
          <div className="space-y-1.5">
            <Label>备注（可选）</Label>
            <Input
              placeholder={`如「${previewText}推科技」，留空则用「${previewText}」`}
              value={nlDesc}
              onChange={(e) => setNlDesc(e.target.value)}
              maxLength={60}
            />
          </div>

          {/* 预览 + 创建按钮 */}
          <div className="flex items-center justify-between pt-1">
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <span>预览</span>
              <Badge variant="secondary">{previewText}</Badge>
              <span className="text-xs">{TZ}</span>
            </div>
            <Button onClick={onCreate} disabled={!canCreate}>
              {creating ? (
                <Loader2 className="size-4 animate-spin" />
              ) : (
                <Plus className="size-4" />
              )}
              {creating ? "创建中…" : "创建"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* ── 现有调度列表 ── */}
      <div className="space-y-3">
        <h2 className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
          已创建的调度
        </h2>

        {loadError && (
          <Alert variant="destructive">
            <AlertDescription>{loadError}</AlertDescription>
          </Alert>
        )}

        {/* 加载骨架 */}
        {schedules === null && !loadError && (
          <div className="space-y-3">
            <Skeleton className="h-16 w-full rounded-xl" />
            <Skeleton className="h-16 w-full rounded-xl" />
          </div>
        )}

        {/* 空状态 */}
        {schedules !== null && schedules.length === 0 && !loadError && (
          <div className="flex flex-col items-center justify-center py-12 text-muted-foreground">
            <Clock className="size-8 mb-2 opacity-30" />
            <p className="text-sm">还没有定时任务，用上面的选择器创建第一个吧。</p>
          </div>
        )}

        {/* 调度列表 */}
        {schedules?.map((s) => (
          <Card
            key={s.id}
            className={
              "transition-shadow hover:ring-foreground/20" +
              (s.status === "paused" ? " opacity-60" : "")
            }
          >
            <CardContent className="flex items-center justify-between py-4">
              <div className="min-w-0 flex-1">
                <p className="font-medium text-sm truncate">
                  {s.nl_description || describeSpec(s.spec)}
                </p>
                <div className="flex items-center gap-2 mt-0.5 flex-wrap">
                  <span className="text-xs text-muted-foreground">
                    {describeSpec(s.spec)}
                  </span>
                  {s.next_run && (
                    <span className="text-xs text-muted-foreground">
                      · 下次 {new Date(s.next_run).toLocaleString("zh-CN")}
                    </span>
                  )}
                  {s.status === "paused" && (
                    <Badge variant="outline" className="text-xs">
                      已暂停
                    </Badge>
                  )}
                </div>
              </div>
              <Button
                variant="ghost"
                size="icon-sm"
                onClick={() => onDelete(s.id)}
                disabled={deletingId === s.id}
                className="ml-3 shrink-0 text-muted-foreground hover:text-destructive"
                aria-label="删除"
              >
                {deletingId === s.id ? (
                  <Loader2 className="size-4 animate-spin" />
                ) : (
                  <Trash2 className="size-4" />
                )}
              </Button>
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
}

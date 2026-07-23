import { useEffect, useState } from "react";
import { Plus, Clock, Loader2, Activity, Send, PlayCircle } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { api, ApiError } from "../api";
import type { Schedule, ScheduleSpec, ScheduleRunSummary } from "../api";
import { fmt, useI18n, type Dict } from "@/i18n";
import { fmtBeijing } from "@/lib/time";

// describeSpec 的 i18n 版：用户面所有任务文案都必须随语言走。
function describeSpecI18n(spec: ScheduleSpec, s: Dict["app"]["schedule"]): string {
  if (typeof spec.every_seconds === "number" && spec.every_seconds > 0) {
    const h = Math.round(spec.every_seconds / 3600);
    return h % 24 === 0 && h >= 24 ? fmt(s.everyDays, { n: h / 24 }) : fmt(s.everyHours, { n: h });
  }
  if (spec.cron) {
    const parts = spec.cron.trim().split(/\s+/);
    if (parts.length === 5) {
      const [mm, hh, , , dow] = parts;
      const clock = `${(hh ?? "0").padStart(2, "0")}:${(mm ?? "0").padStart(2, "0")}`;
      if (dow === "*") return fmt(s.dailyAt, { time: clock });
      const days = (dow ?? "")
        .split(",")
        .map((d) => s.weekdays[Number(d)] ?? d)
        .join(s.daySep);
      return fmt(s.weeklyAt, { days, time: clock });
    }
  }
  return s.custom;
}

// 批次结局的用户话术：done/failed/empty + 空批的提前退出闸门。
// 任务列表「上次运行」与详情页运行历史共用同一词表（t.app.batch）。
export function batchOutcomeLabel(
  status: string,
  exitGate: string,
  b: Dict["app"]["batch"],
): string {
  if (status === "done") return b.done;
  if (status === "failed") return b.failed;
  if (status === "empty") {
    const gate = (b.gate as Record<string, string>)[exitGate];
    return gate ?? b.empty;
  }
  return status;
}

export function batchOutcomeVariant(
  status: string,
): "default" | "secondary" | "outline" | "destructive" {
  if (status === "failed") return "destructive";
  if (status === "done") return "default";
  return "secondary"; // empty：正常终态，不渲染成事故
}

// 任务卡读的是真实 schedules：schedules.nl_description 本来就是用户当初
// 用自然语言描述的意图，schedule 就是「任务」在当前后端的载体。
// 不另造 mock 任务——首页的「运行中任务」数也来自同一份数据，两处口径必须一致。
function StatusBadge({ status }: { status: string }) {
  const { t } = useI18n();
  if (status === "active") {
    return (
      <Badge
        variant="outline"
        className="text-emerald-600 border-emerald-200 bg-emerald-50 dark:bg-emerald-950/30 dark:border-emerald-800"
      >
        {t.app.tasks.running}
      </Badge>
    );
  }
  return (
    <Badge
      variant="outline"
      className="text-amber-600 border-amber-200 bg-amber-50 dark:bg-amber-950/30 dark:border-amber-800"
    >
      {t.app.tasks.paused}
    </Badge>
  );
}

// 顶部统计条：三格口径全部来自本页已取的数据（任务列表 + summary 接口），
// 不另造前端推导值。summary 拉取失败时整条隐藏（下方卡片同样会降级），不显示 0 冒充真值。
function StatsBar({
  tasks,
  summaries,
}: {
  tasks: Schedule[];
  summaries: Map<string, ScheduleRunSummary>;
}) {
  const { t } = useI18n();
  const T = t.app.tasks;
  const active = tasks.filter((x) => x.status === "active").length;
  let sent7d = 0;
  let runs7d = 0;
  for (const s of summaries.values()) {
    sent7d += s.sent_pushes_7d;
    runs7d += s.batches_7d;
  }
  const cells = [
    { icon: Activity, label: T.statActive, value: active },
    { icon: Send, label: T.statSent7d, value: sent7d },
    { icon: PlayCircle, label: T.statRuns7d, value: runs7d },
  ];
  return (
    <div className="grid grid-cols-3 gap-3">
      {cells.map((c) => (
        <Card key={c.label}>
          <CardContent className="p-4 flex items-center gap-3">
            <c.icon className="size-4 text-muted-foreground shrink-0" />
            <div className="min-w-0">
              <div className="text-lg font-semibold leading-tight">{c.value}</div>
              <div className="text-xs text-muted-foreground truncate">{c.label}</div>
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

function TaskCard({ task, summary }: { task: Schedule; summary?: ScheduleRunSummary }) {
  const { t } = useI18n();
  const T = t.app.tasks;
  const specText = describeSpecI18n(task.spec, t.app.schedule);
  return (
    <a href={`#/tasks/${encodeURIComponent(task.id)}`} className="block">
      <Card className="hover:border-ring/40 transition-colors cursor-pointer">
        <CardContent className="p-5">
          <div className="flex items-start justify-between gap-3 mb-2">
            <div className="min-w-0 flex-1">
              <h3 className="font-medium text-sm">{task.nl_description || specText}</h3>
              <p className="text-xs text-muted-foreground mt-0.5">{specText}</p>
            </div>
            <StatusBadge status={task.status} />
          </div>
          {/* 密度行：上次运行 + 结局、近 7 天推送、信源数。summary 接口失败/缺行时
              整行省略（老数据没有假装成 0 的必要），任务本体信息不受影响。 */}
          {summary && (
            <div className="flex flex-wrap items-center gap-x-4 gap-y-1 mt-3 text-xs text-muted-foreground">
              <span className="inline-flex items-center gap-1.5">
                {T.lastRun}{" "}
                {summary.last_run_at ? (
                  <>
                    {fmtBeijing(summary.last_run_at)}
                    <Badge
                      variant={batchOutcomeVariant(summary.last_status)}
                      className="px-1.5 py-0 text-[10px]"
                    >
                      {batchOutcomeLabel(summary.last_status, summary.last_exit_gate, t.app.batch)}
                    </Badge>
                  </>
                ) : (
                  T.neverRan
                )}
              </span>
              <span>{fmt(T.sent7d, { n: summary.sent_pushes_7d })}</span>
              <span>{fmt(T.sourceCount, { n: summary.source_count })}</span>
            </div>
          )}
          {task.next_run && (
            <div className="flex items-center gap-1 mt-2 text-xs text-muted-foreground">
              <Clock className="size-3" />
              {T.nextRun} {fmtBeijing(task.next_run)}
            </div>
          )}
        </CardContent>
      </Card>
    </a>
  );
}

// 新建任务对话框目前只有输入框，「发送」尚未接后端：
// 自然语言 → 任务手册的编译能力在后端已存在（agent/playbook_translate.go），
// 但入口是飞书 agent 工具（create_schedule），没有 HTTP 出口。
// 接线属于 P2，这里先不给假的成功反馈。
function CreateTaskDialog({ open, onClose }: { open: boolean; onClose: () => void }) {
  const { t } = useI18n();
  const T = t.app.tasks;
  const [input, setInput] = useState("");

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>{T.newTask}</DialogTitle>
        </DialogHeader>
        <div className="space-y-4 pt-2">
          <p className="text-sm text-muted-foreground">{T.dialogDesc}</p>
          <div className="flex gap-2">
            <Input
              placeholder={T.dialogPlaceholder}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              className="flex-1"
              autoFocus
            />
            <Button disabled size="sm" title={T.sendPendingTitle}>
              {T.send}
            </Button>
          </div>
          <p className="text-xs text-muted-foreground">{T.dialogNote}</p>
        </div>
      </DialogContent>
    </Dialog>
  );
}

export default function TaskDashboard() {
  const { t } = useI18n();
  const T = t.app.tasks;
  const [showCreate, setShowCreate] = useState(false);
  const [tasks, setTasks] = useState<Schedule[]>([]);
  const [summaries, setSummaries] = useState<Map<string, ScheduleRunSummary>>(new Map());
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");

  useEffect(() => {
    let alive = true;
    // 任务列表是主数据（失败要报错，vane-web#18：静默吞错会让故障与空数据同形）；
    // summary 是增强数据，失败只降级为不显示密度行/统计条，不拦整页。
    Promise.allSettled([api.listSchedules(), api.scheduleSummaries()])
      .then(([schedRes, sumRes]) => {
        if (!alive) return;
        if (schedRes.status === "fulfilled") {
          setTasks(schedRes.value);
          setLoadError("");
        } else {
          const err = schedRes.reason;
          setLoadError(err instanceof ApiError ? err.message : t.app.common.loadFailed);
          setTasks([]);
        }
        if (sumRes.status === "fulfilled") {
          setSummaries(new Map(sumRes.value.map((s) => [s.schedule_id, s])));
        }
      })
      .finally(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold">{T.title}</h1>
        <Button size="sm" onClick={() => setShowCreate(true)}>
          <Plus className="size-4 mr-1" />
          {T.newTask}
        </Button>
      </div>

      {loading ? (
        <div className="flex items-center justify-center py-12 text-muted-foreground">
          <Loader2 className="size-4 animate-spin mr-2" />
          <span className="text-sm">{t.app.common.loading}</span>
        </div>
      ) : loadError ? (
        <Alert variant="destructive">
          <AlertDescription>{loadError}</AlertDescription>
        </Alert>
      ) : tasks.length === 0 ? (
        <div className="text-center py-16">
          <Clock className="size-10 mx-auto text-muted-foreground/50 mb-3" />
          <p className="text-sm text-muted-foreground mb-4">{T.empty}</p>
          <Button onClick={() => setShowCreate(true)}>
            <Plus className="size-4 mr-1" />
            {T.createFirst}
          </Button>
        </div>
      ) : (
        <>
          {summaries.size > 0 && <StatsBar tasks={tasks} summaries={summaries} />}
          <div className="space-y-3">
            {tasks.map((task) => (
              <TaskCard key={task.id} task={task} summary={summaries.get(task.id)} />
            ))}
          </div>
        </>
      )}

      <CreateTaskDialog open={showCreate} onClose={() => setShowCreate(false)} />
    </div>
  );
}

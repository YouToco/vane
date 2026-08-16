import { useEffect, useState } from "react";
import { Plus, Clock, Loader2, Activity, Newspaper, BellRing } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription } from "@/components/ui/alert";
import TaskActionDialog from "@/features/task/TaskActionDialog";
import { api, ApiError } from "@/shared/api/client";
import type {
  Schedule,
  ScheduleSpec,
  ScheduleRunSummary,
} from "@/shared/api/client";
import { fmt, useI18n, type Dict } from "@/i18n";
import { fmtBeijing } from "@/shared/utils/time";
import { taskRunOutcome } from "@/shared/utils/task-detail-presentation";
import { deliveryChannelLabel } from "@/features/task/TaskDeliveryChannel";

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

// 顶部概览只保留用户关心的任务数和情报数；运行批次等运维指标不进入普通界面。
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
  let insights7d = 0;
  for (const s of summaries.values()) {
    insights7d += s.sent_pushes_7d;
  }
  const cells = [
    { icon: Activity, label: T.statActive, value: active },
    { icon: Newspaper, label: T.statInsights7d, value: insights7d },
  ];
  return (
    <div className="grid grid-cols-2 gap-3">
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
  const D = t.app.taskDetail;
  const specText = describeSpecI18n(task.spec, t.app.schedule);
  const outcome = taskRunOutcome(summary?.last_status ?? "");
  const outcomeLabel =
    outcome === "completed"
      ? D.checkCompleted
      : outcome === "no_important_change"
        ? D.checkNoChange
        : D.checkIncomplete;
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
          {/* 密度行：最近检查 + 用户结论、近 7 天情报。summary 接口失败/缺行时
              整行省略（老数据没有假装成 0 的必要），任务本体信息不受影响。 */}
          {summary && (
            <div className="flex flex-wrap items-center gap-x-4 gap-y-1 mt-3 text-xs text-muted-foreground">
              <span className="inline-flex items-center gap-1.5">
                {T.lastCheck}{" "}
                {summary.last_run_at ? (
                  <>
                    {fmtBeijing(summary.last_run_at)}
                    <Badge
                      variant={outcome === "incomplete" ? "destructive" : "outline"}
                      className="px-1.5 py-0 text-[10px]"
                    >
                      {outcomeLabel}
                    </Badge>
                  </>
                ) : (
                  T.neverRan
                )}
              </span>
              <span>{fmt(T.insights7d, { n: summary.sent_pushes_7d })}</span>
            </div>
          )}
          {task.next_run && (
            <div className="flex items-center gap-1 mt-2 text-xs text-muted-foreground">
              <Clock className="size-3" />
              {T.nextRun} {fmtBeijing(task.next_run)}
            </div>
          )}
          <div className="mt-2 flex items-center gap-1 text-xs text-muted-foreground">
            <BellRing className="size-3" />
            定时推送：{deliveryChannelLabel(task.delivery_channel)}
          </div>
        </CardContent>
      </Card>
    </a>
  );
}

export default function TaskDashboard({ actorScope }: { actorScope: string }) {
  const { t } = useI18n();
  const T = t.app.tasks;
  const [showCreate, setShowCreate] = useState(false);
  const [tasks, setTasks] = useState<Schedule[]>([]);
  const [summaries, setSummaries] = useState<Map<string, ScheduleRunSummary>>(new Map());
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");

  function handleCreateClose() {
    setShowCreate(false);
  }

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

      <TaskActionDialog
        open={showCreate}
        actorScope={actorScope}
        onClose={handleCreateClose}
        onComplete={() => {
          void api.listSchedules().then(setTasks);
          void api.scheduleSummaries().then((values) => {
            setSummaries(new Map(values.map((item) => [item.schedule_id, item])));
          });
        }}
        labels={{
          title: T.newTask,
          description: T.dialogDesc,
          placeholder: T.dialogPlaceholder,
          inputLabel: T.dialogInputLabel,
          execute: T.generate,
          executing: T.generating,
          close: T.close,
          requestFailed: T.requestFailed,
        }}
      />
    </div>
  );
}

import { useEffect, useRef, useState } from "react";
import {
  ArrowLeft,
  Loader2,
  ExternalLink,
  Newspaper,
  Clock,
  Pencil,
  Pause,
  Play,
  Trash2,
  MoreHorizontal,
} from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import TaskActionDialog from "@/components/TaskActionDialog";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from "@/components/ui/table";
import { api, ApiError } from "../api";
import type {
  ScheduleDetail,
  ScheduleBatchItem,
  ScheduleSourceInfo,
  ObservationPolicy,
  TaskActionStatus,
} from "../api";
import { fmt, useI18n, type Dict } from "@/i18n";
import { fmtBeijing } from "@/lib/time";
import DeliveriesTable from "@/components/DeliveriesTable";
import { SCHEDULE_COMMAND_STORAGE_PREFIX } from "@/lib/task-action-session";
import { taskRunOutcome, type TaskRunOutcome } from "@/lib/task-detail-presentation";
import {
  nextRunPresentation,
  taskDefinitionEditEnabled,
} from "@/lib/task-detail-contract";

const PAGE_SIZE = 20;
type ScheduleCommand = "run" | "pause" | "resume" | "delete";

function commandStorageKey(
  actorScope: string,
  scheduleID: string,
  kind: ScheduleCommand,
): string {
  return `${SCHEDULE_COMMAND_STORAGE_PREFIX}:${encodeURIComponent(actorScope)}:${encodeURIComponent(scheduleID)}:${kind}`;
}

function commandIdempotencyKey(
  actorScope: string,
  scheduleID: string,
  kind: ScheduleCommand,
): string {
  const key = commandStorageKey(actorScope, scheduleID, kind);
  try {
    const existing = window.sessionStorage.getItem(key);
    if (existing) return existing;
    const created = globalThis.crypto.randomUUID();
    window.sessionStorage.setItem(key, created);
    return created;
  } catch {
    // Session storage may be unavailable. The synchronous commandRef below
    // still prevents ordinary double clicks from creating two live requests.
    return globalThis.crypto.randomUUID();
  }
}

function clearCommandIdempotencyKey(
  actorScope: string,
  scheduleID: string,
  kind: ScheduleCommand,
): void {
  try {
    window.sessionStorage.removeItem(
      commandStorageKey(actorScope, scheduleID, kind),
    );
  } catch {
    // A completed command does not need storage cleanup to remain correct.
  }
}

function commandMayHaveReachedServer(error: unknown): boolean {
  if (!(error instanceof ApiError)) return true;
  return (
    error.status === 0 ||
    error.status >= 500 ||
    error.status === 408 ||
    error.status === 409 ||
    error.status === 425 ||
    error.status === 429
  );
}

function runOutcomeLabel(
  outcome: TaskRunOutcome,
  d: Dict["app"]["taskDetail"],
): string {
  if (outcome === "completed") return d.checkCompleted;
  if (outcome === "no_important_change") return d.checkNoChange;
  if (outcome === "not_run") return d.neverRan;
  return d.checkIncomplete;
}

function runOutcomeVariant(
  outcome: TaskRunOutcome,
): "secondary" | "outline" | "destructive" {
  if (outcome === "incomplete") return "destructive";
  return outcome === "completed" ? "secondary" : "outline";
}

// 运行与诊断：保留检查时间、用户可理解的结果和情报条数；batch id、发送状态、
// 阶段漏斗等运维对象不进入普通任务页。
function RunsTab({ scheduleID }: { scheduleID: string }) {
  const { t } = useI18n();
  const D = t.app.taskDetail;
  const [items, setItems] = useState<ScheduleBatchItem[]>([]);
  const [total, setTotal] = useState(0);
  const [nextToken, setNextToken] = useState("");
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [loadError, setLoadError] = useState("");

  useEffect(() => {
    let alive = true;
    api
      .scheduleBatches(scheduleID, PAGE_SIZE)
      .then((r) => {
        if (!alive) return;
        setItems(r.items);
        setTotal(r.total);
        setNextToken(r.next_page_token ?? "");
        setLoadError("");
      })
      .catch((err) => {
        if (!alive) return;
        setLoadError(err instanceof ApiError ? err.message : t.app.common.loadFailed);
      })
      .finally(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scheduleID]);

  async function loadMore() {
    if (!nextToken || loadingMore) return;
    setLoadingMore(true);
    try {
      const r = await api.scheduleBatches(scheduleID, PAGE_SIZE, nextToken);
      setItems((prev) => prev.concat(r.items));
      setTotal(r.total);
      setNextToken(r.next_page_token ?? "");
      setLoadError("");
    } catch (err) {
      setLoadError(err instanceof ApiError ? err.message : t.app.common.loadFailed);
    } finally {
      setLoadingMore(false);
    }
  }

  if (loading) {
    return (
      <Card>
        <CardContent className="py-6 space-y-3">
          {Array.from({ length: 4 }).map((_, i) => (
            <Skeleton key={i} className="h-4 w-full" />
          ))}
        </CardContent>
      </Card>
    );
  }
  if (loadError) {
    return (
      <Alert variant="destructive">
        <AlertDescription>{loadError}</AlertDescription>
      </Alert>
    );
  }
  if (items.length === 0) {
    return (
      <Card>
        <CardContent className="py-12 text-center text-muted-foreground">
          {D.runsEmpty}
        </CardContent>
      </Card>
    );
  }
  return (
    <Card>
      <div className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>{D.runsColTime}</TableHead>
              <TableHead>{D.runsColOutcome}</TableHead>
              <TableHead className="text-right">{D.runsColInsights}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {items.map((b) => (
              <TableRow key={b.id} className={b.status === "failed" ? "bg-destructive/5" : ""}>
                <TableCell className="text-sm whitespace-nowrap">
                  {fmtBeijing(b.created_at)}
                </TableCell>
                <TableCell>
                  <Badge variant={runOutcomeVariant(taskRunOutcome(b.status))}>
                    {runOutcomeLabel(taskRunOutcome(b.status), D)}
                  </Badge>
                </TableCell>
                <TableCell className="text-right font-mono text-sm whitespace-nowrap">
                  {fmt(D.insightCount, { n: b.deliveries })}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
      <div className="flex items-center justify-between border-t px-4 py-3">
        <span className="text-sm text-muted-foreground">
          {fmt(D.shownRuns, { shown: items.length, total })}
        </span>
        {nextToken && (
          <Button variant="outline" size="sm" onClick={loadMore} disabled={loadingMore}>
            {loadingMore ? (
              <>
                <Loader2 className="mr-2 size-4 animate-spin" />
                {t.app.common.loading}
              </>
            ) : (
              t.app.common.loadMore
            )}
          </Button>
        )}
      </div>
    </Card>
  );
}

function sourceStatusVariant(status: string): "default" | "secondary" | "outline" | "destructive" {
  if (status === "disabled") return "destructive";
  if (status === "paused") return "secondary";
  return "outline";
}

function durationText(seconds: number, d: Dict["app"]["taskDetail"]): string {
  if (seconds % 86400 === 0) return `${seconds / 86400} ${d.observationDays}`;
  if (seconds % 3600 === 0) return `${seconds / 3600} ${d.observationHours}`;
  return `${seconds / 60} ${d.observationMinutes}`;
}

function policyValue(value: string | undefined, labels: Record<string, string>): string {
  return value ? labels[value] ?? value : "—";
}

function ObservationTab({ policy }: { policy: ObservationPolicy }) {
  const { t } = useI18n();
  const D = t.app.taskDetail;
  const window = policy.window;
  const evidence = policy.evidence;
  const eventDefinition = [
    policy.event?.subject,
    policy.event?.event_kind,
    policy.event?.qualification,
  ].filter(Boolean).join(" · ");
  const rows: [string, string][] = [
    [
      D.observationMode,
      `${policyValue(policy.mode, {
        content: D.observationContent,
        event: D.observationEvent,
      })}${eventDefinition ? ` · ${eventDefinition}` : ""}`,
    ],
    [
      D.observationWindow,
      window?.kind === "schedule_interval"
        ? D.observationScheduleInterval
        : window?.kind === "rolling_duration" && typeof window.rolling_duration_seconds === "number"
          ? `${D.observationRolling} · ${durationText(window.rolling_duration_seconds, D)}`
          : window?.kind === "calendar_period"
            ? `${D.observationCalendar} · ${policyValue(window.calendar_period, {
                day: D.observationDay,
                week: D.observationWeek,
                month: D.observationMonth,
              })}`
            : "—",
    ],
    [
      D.observationEvidence,
      evidence?.requirement === "official_required"
        ? D.observationOfficialOnly
        : evidence?.requirement === "trusted_allowed"
          ? D.observationTrustedAllowed
          : evidence?.requirement ?? "—",
    ],
    [
      D.observationLate,
      policy.late_policy === "strict"
        ? D.observationStrict
        : policy.late_policy === "bounded" && typeof policy.allowed_lateness_seconds === "number"
          ? `${D.observationBounded} · ${durationText(policy.allowed_lateness_seconds, D)}`
          : policy.late_policy ?? "—",
    ],
    [
      D.observationUnknownTime,
      policyValue(policy.unknown_time, {
        reject: D.observationReject,
        deprioritize: D.observationDeprioritize,
        allow: D.observationAllow,
      }),
    ],
  ];
  if (policy.effective_at) rows.push([D.observationEffectiveAt, fmtBeijing(policy.effective_at)]);
  if (evidence?.official_domains?.length) rows.push([D.observationOfficialDomains, evidence.official_domains.join(", ")]);

  return (
    <Card>
      <CardContent className="py-5 space-y-3">
        <p className="text-xs text-muted-foreground">{D.observationDesc}</p>
        <dl className="grid gap-x-6 gap-y-3 sm:grid-cols-2 text-sm">
          {rows.map(([label, value]) => (
            <div key={label} className="min-w-0">
              <dt className="text-xs text-muted-foreground">{label}</dt>
              <dd className="mt-0.5 break-words">{value}</dd>
            </div>
          ))}
        </dl>
      </CardContent>
    </Card>
  );
}

// 绑定信源 Tab：详情接口已随首屏取回，这里纯渲染。空 ≠ 坏：老任务走账号级
// 订阅、没有 schedule_sources 行，空态文案要说真话而不是「没数据」。
function SourcesTab({ sources }: { sources: ScheduleSourceInfo[] }) {
  const { t } = useI18n();
  const D = t.app.taskDetail;
  if (sources.length === 0) {
    return (
      <Card>
        <CardContent className="py-12 text-center text-sm text-muted-foreground">
          {D.sourcesEmpty}
        </CardContent>
      </Card>
    );
  }
  return (
    <Card>
      <div className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>{D.srcColSource}</TableHead>
              <TableHead>{D.srcColStatus}</TableHead>
              <TableHead>{D.srcColLastFetch}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {sources.map((s) => (
              <TableRow key={s.id}>
                <TableCell className="max-w-[280px]">
                  <div className="truncate">
                    {s.url ? (
                      <a
                        href={s.url}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center gap-1 text-primary hover:underline"
                      >
                        <span className="truncate">{s.title || s.url}</span>
                        <ExternalLink className="size-3 shrink-0" />
                      </a>
                    ) : (
                      <span>{s.title}</span>
                    )}
                  </div>
                  <div className="text-xs text-muted-foreground mt-0.5">
                    {s.platform}
                    {s.capability ? ` · ${s.capability}` : ""}
                  </div>
                </TableCell>
                <TableCell>
                  <div className="flex items-center gap-2">
                    <Badge variant={sourceStatusVariant(s.status)}>
                      {(D.srcStatus as Record<string, string>)[s.status] ?? s.status}
                    </Badge>
                    {s.fail_count > 0 && (
                      <span className="text-xs text-destructive">
                        {fmt(D.srcFailTimes, { n: s.fail_count })}
                      </span>
                    )}
                  </div>
                </TableCell>
                <TableCell className="text-sm whitespace-nowrap text-muted-foreground">
                  {fmtBeijing(s.last_fetched_at)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </Card>
  );
}

export default function TaskDetail({
  scheduleID,
  actorScope,
}: {
  scheduleID: string;
  actorScope: string;
}) {
  const { t } = useI18n();
  const D = t.app.taskDetail;
  const A = t.app.tasks;
  const [detail, setDetail] = useState<ScheduleDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [showEdit, setShowEdit] = useState(false);
  const [command, setCommand] = useState("");
  const commandRef = useRef<ScheduleCommand | "">("");
  const [commandMessage, setCommandMessage] = useState("");
  const [commandError, setCommandError] = useState("");

  async function reloadDetail() {
    const next = await api.scheduleDetail(scheduleID);
    setDetail(next);
    setLoadError("");
  }

  useEffect(() => {
    let alive = true;
    setLoading(true);
    setDetail(null);
    api
      .scheduleDetail(scheduleID)
      .then((d) => {
        if (!alive) return;
        setDetail(d);
        setLoadError("");
      })
      .catch((err) => {
        if (!alive) return;
        setLoadError(err instanceof ApiError ? err.message : t.app.common.loadFailed);
      })
      .finally(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scheduleID]);

  async function runCommand(kind: "run" | "pause" | "resume") {
    if (commandRef.current) return;
    commandRef.current = kind;
    setCommand(kind);
    setCommandError("");
    setCommandMessage("");
    const idempotencyKey = commandIdempotencyKey(
      actorScope,
      scheduleID,
      kind,
    );
    try {
      if (kind === "run") {
        await api.runScheduleNow(scheduleID, idempotencyKey);
        setCommandMessage(D.runAccepted);
      } else if (kind === "pause") {
        await api.pauseSchedule(scheduleID, idempotencyKey);
        await reloadDetail();
        setCommandMessage(D.pauseDone);
      } else {
        await api.resumeSchedule(scheduleID, idempotencyKey);
        await reloadDetail();
        setCommandMessage(D.resumeDone);
      }
      clearCommandIdempotencyKey(actorScope, scheduleID, kind);
    } catch (err) {
      setCommandError(err instanceof ApiError ? err.message : t.app.common.loadFailed);
      if (!commandMayHaveReachedServer(err)) {
        clearCommandIdempotencyKey(actorScope, scheduleID, kind);
      }
    } finally {
      commandRef.current = "";
      setCommand("");
    }
  }

  async function deleteTask() {
    if (commandRef.current || !window.confirm(D.deleteConfirm)) return;
    commandRef.current = "delete";
    setCommand("delete");
    setCommandError("");
    const idempotencyKey = commandIdempotencyKey(
      actorScope,
      scheduleID,
      "delete",
    );
    try {
      await api.deleteSchedule(scheduleID, idempotencyKey);
      clearCommandIdempotencyKey(actorScope, scheduleID, "delete");
      location.hash = "#/tasks";
    } catch (err) {
      setCommandError(err instanceof ApiError ? err.message : t.app.common.loadFailed);
      if (!commandMayHaveReachedServer(err)) {
        clearCommandIdempotencyKey(actorScope, scheduleID, "delete");
      }
      commandRef.current = "";
      setCommand("");
    }
  }

  function handleEditComplete(status: TaskActionStatus) {
    if (status.kind === "edit" && status.status === "completed") {
      void reloadDetail().catch((err) => {
        setCommandError(err instanceof ApiError ? err.message : t.app.common.loadFailed);
      });
    }
  }

  if (loading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-6 w-64" />
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          {Array.from({ length: 4 }).map((_, i) => (
            <Skeleton key={i} className="h-20" />
          ))}
        </div>
        <Skeleton className="h-48 w-full" />
      </div>
    );
  }
  if (loadError || !detail) {
    return (
      <div className="space-y-4">
        <a
          href="#/tasks"
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="size-4" />
          {D.back}
        </a>
        <Alert variant="destructive">
          <AlertDescription>{loadError || t.app.common.loadFailed}</AlertDescription>
        </Alert>
      </div>
    );
  }

  const { schedule, summary, sources, playbook } = detail;
  const editEnabled = taskDefinitionEditEnabled(detail);
  const nextRun = nextRunPresentation(schedule);
  // `observation` is the immutable runtime-policy projection; the alias keeps
  // this first read-only UI useful while older API deployments expose the
  // create-command spelling instead.
  const observation = schedule.scope.observation ?? schedule.scope.observation_policy;
  const lastOutcome = taskRunOutcome(summary.last_status);

  return (
    <div className="space-y-6">
      <div>
        <a
          href="#/tasks"
          className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="size-4" />
          {D.back}
        </a>
        <div className="flex items-start justify-between gap-3 mt-2">
          <h1 className="text-xl font-semibold min-w-0">{schedule.nl_description || schedule.id}</h1>
          <div className="flex shrink-0 items-center gap-1">
            <Badge
              variant="outline"
              className={
                schedule.status === "active"
                  ? "text-emerald-600 border-emerald-200 bg-emerald-50 dark:bg-emerald-950/30 dark:border-emerald-800"
                  : "text-amber-600 border-amber-200 bg-amber-50 dark:bg-amber-950/30 dark:border-amber-800"
              }
            >
              {schedule.status === "active" ? t.app.tasks.running : t.app.tasks.paused}
            </Badge>
            <DropdownMenu>
              <DropdownMenuTrigger
                render={
                  <Button
                    variant="ghost"
                    size="icon-sm"
                    aria-label={D.taskMenu}
                    disabled={command !== ""}
                  />
                }
              >
                {command ? (
                  <Loader2 className="size-4 animate-spin" />
                ) : (
                  <MoreHorizontal className="size-4" />
                )}
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                <DropdownMenuItem
                  onClick={() => void runCommand("run")}
                  disabled={schedule.status !== "active"}
                >
                  <Play className="size-4" />
                  {D.runNow}
                </DropdownMenuItem>
                <DropdownMenuItem
                  onClick={() =>
                    void runCommand(schedule.status === "active" ? "pause" : "resume")
                  }
                >
                  {schedule.status === "active" ? (
                    <Pause className="size-4" />
                  ) : (
                    <Play className="size-4" />
                  )}
                  {schedule.status === "active" ? D.pauseTask : D.resumeTask}
                </DropdownMenuItem>
                {editEnabled && (
                  <DropdownMenuItem onClick={() => setShowEdit(true)}>
                    <Pencil className="size-4" />
                    {D.editTask}
                  </DropdownMenuItem>
                )}
                <DropdownMenuItem
                  variant="destructive"
                  onClick={() => void deleteTask()}
                >
                  <Trash2 className="size-4" />
                  {D.deleteTask}
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
        </div>
        <p className="text-sm text-muted-foreground mt-1">
          {D.lastCheck}{" "}
          {summary.last_run_at ? (
            <>
              {fmtBeijing(summary.last_run_at)}
              <Badge
                variant={runOutcomeVariant(lastOutcome)}
                className="ml-2 px-1.5 py-0 text-[10px]"
              >
                {runOutcomeLabel(lastOutcome, D)}
              </Badge>
            </>
          ) : (
            D.neverRan
          )}
        </p>
        <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 text-sm text-muted-foreground">
          <span className="inline-flex items-center gap-1">
            <Clock className="size-4" />
            {D.nextRun}{" "}
            {nextRun.kind === "scheduled"
              ? fmtBeijing(nextRun.at)
              : nextRun.kind === "paused"
                ? D.nextRunPaused
                : nextRun.kind === "none"
                  ? D.noNextRun
                  : D.nextRunUnavailable}
          </span>
        </div>
        {commandMessage && (
          <Alert className="mt-4">
            <AlertDescription>{commandMessage}</AlertDescription>
          </Alert>
        )}
        {commandError && (
          <Alert variant="destructive" className="mt-4">
            <AlertDescription>{commandError}</AlertDescription>
          </Alert>
        )}
      </div>

      <Card>
        <CardContent className="flex items-center gap-3 p-4">
          <Newspaper className="size-4 shrink-0 text-muted-foreground" />
          <div>
            <div className="font-medium">
              {fmt(D.insights7d, { n: summary.sent_pushes_7d })}
            </div>
            <div className="text-xs text-muted-foreground">{D.insights7dDesc}</div>
          </div>
        </CardContent>
      </Card>

      <Tabs defaultValue="pushes">
        <TabsList>
          <TabsTrigger value="pushes">{D.tabPushes}</TabsTrigger>
          <TabsTrigger value="runs">{D.tabRuns}</TabsTrigger>
          <TabsTrigger value="sources">{D.tabSources}</TabsTrigger>
          <TabsTrigger value="playbook">{D.tabPlaybook}</TabsTrigger>
          {observation && <TabsTrigger value="observation">{D.tabObservation}</TabsTrigger>}
        </TabsList>
        <TabsContent value="pushes">
          <DeliveriesTable
            presentation="task-content"
            fetchPage={(pageSize, pageToken) =>
              api.scheduleDeliveries(scheduleID, pageSize, pageToken)
            }
            emptyText={D.pushesEmpty}
          />
        </TabsContent>
        <TabsContent value="runs">
          <RunsTab scheduleID={scheduleID} />
        </TabsContent>
        <TabsContent value="sources">
          <SourcesTab sources={sources} />
        </TabsContent>
        <TabsContent value="playbook">
          {playbook ? (
            <Card>
              <CardContent className="py-5 space-y-3">
                <p className="text-xs text-muted-foreground">
                  {D.playbookDesc} · {fmt(D.playbookUpdated, { time: fmtBeijing(playbook.updated_at) })}
                </p>
                <pre className="whitespace-pre-wrap break-words font-sans text-sm leading-relaxed">
                  {playbook.content}
                </pre>
              </CardContent>
            </Card>
          ) : (
            <Card>
              <CardContent className="py-12 text-center text-sm text-muted-foreground">
                {D.playbookEmpty}
              </CardContent>
            </Card>
          )}
        </TabsContent>
        {observation && (
          <TabsContent value="observation">
            <ObservationTab policy={observation} />
          </TabsContent>
        )}
      </Tabs>
      {editEnabled && (
        <TaskActionDialog
          open={showEdit}
          actorScope={actorScope}
          taskID={scheduleID}
          onClose={() => setShowEdit(false)}
          onComplete={handleEditComplete}
          labels={{
            title: D.editTask,
            description: D.editDesc,
            placeholder: D.editPlaceholder,
            inputLabel: D.editInputLabel,
            draft: D.generateEdit,
            drafting: D.generatingEdit,
            preview: D.editPreview,
            confirm: D.confirmEdit,
            confirming: D.confirmingEdit,
            cancel: D.cancelEdit,
            close: D.closeEdit,
            waiting: D.editWaiting,
            checkAgain: A.checkAgain,
            requestFailed: A.requestFailed,
            resultStatus: A.resultStatus,
            invalidProposal: A.invalidProposal,
            status: A.actionStatus,
          }}
        />
      )}
    </div>
  );
}

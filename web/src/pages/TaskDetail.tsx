import { lazy, Suspense, useEffect, useRef, useState } from "react";
import {
  ArrowLeft,
  Loader2,
  Newspaper,
  Clock,
  Pencil,
  Pause,
  Play,
  Trash2,
  MoreHorizontal,
  Settings2,
} from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import TaskActionDialog from "@/components/TaskActionDialog";
import TaskHealthPanel from "@/components/TaskHealthPanel";
import type { TaskHealthAction } from "@/components/TaskHealthPanel";
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
  ObservationPolicy,
  TaskLatestCheck,
  TaskHealthProjection,
} from "../api";
import { fmt, useI18n, type Dict, type Locale } from "@/i18n";
import {
  taskHealthCopy,
  taskHealthLoadingCopy,
  taskHealthUnavailableCopy,
} from "@/i18n/task-health";
import { fmtBeijing } from "@/lib/time";
import { SCHEDULE_COMMAND_STORAGE_PREFIX } from "@/lib/task-action-session";
import {
  canonicalCheckOutcome,
  taskRunOutcome,
  type TaskRunOutcome,
} from "@/lib/task-detail-presentation";
import { nextRunPresentation } from "@/lib/task-detail-contract";

const PAGE_SIZE = 20;
const TaskBriefFeed = lazy(() => import("@/components/TaskBriefFeed"));
type ScheduleCommand = "run" | "pause" | "resume" | "delete";
type TaskSection = "brief" | "manage";

function taskSurfaceCopy(
  locale: Locale,
  d: Dict["app"]["taskDetail"],
): {
  tabs: Record<TaskSection, string>;
  health: ReturnType<typeof taskHealthCopy>;
  healthLoading: string;
  healthUnavailable: string;
  manageDescription: string;
  playbookTitle: string;
  observationTitle: string;
  runsTitle: string;
} {
  return {
    tabs: {
      brief: d.tabPushes,
      manage: d.tabPlaybook,
    },
    health: taskHealthCopy(locale),
    healthLoading: taskHealthLoadingCopy(locale),
    healthUnavailable: taskHealthUnavailableCopy(locale),
    manageDescription: d.playbookDesc,
    playbookTitle: d.playbookTitle,
    observationTitle: d.tabObservation,
    runsTitle: d.tabRuns,
  };
}

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

export default function TaskDetail({
  scheduleID,
  actorScope,
}: {
  scheduleID: string;
  actorScope: string;
}) {
  const { t, locale } = useI18n();
  const D = t.app.taskDetail;
  const A = t.app.tasks;
  const surface = taskSurfaceCopy(locale, D);
  const [detail, setDetail] = useState<ScheduleDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [showEdit, setShowEdit] = useState(false);
  const [showCreate, setShowCreate] = useState(false);
  const [command, setCommand] = useState("");
  const commandRef = useRef<ScheduleCommand | "">("");
  const scheduleIDRef = useRef(scheduleID);
  scheduleIDRef.current = scheduleID;
  const [commandMessage, setCommandMessage] = useState("");
  const [commandError, setCommandError] = useState("");
  const [latestCheckState, setLatestCheckState] = useState<{
    scheduleID: string;
    check?: TaskLatestCheck;
  }>();
  const [healthState, setHealthState] = useState<{
    scheduleID: string;
    loaded: boolean;
    health?: TaskHealthProjection;
  }>();
  const [section, setSection] = useState<TaskSection>("brief");
  const health =
    healthState?.scheduleID === scheduleID
      ? healthState.health
      : undefined;
  const healthLoaded =
    healthState?.scheduleID === scheduleID && healthState.loaded;
  const permissions = health?.permissions;
  const editEnabled = permissions?.can_edit === true;
  const hasTaskActions = Boolean(
    permissions?.can_run ||
      permissions?.can_pause ||
      permissions?.can_edit ||
      permissions?.can_delete,
  );

  async function reloadDetail(requestedScheduleID = scheduleID) {
    const next = await api.scheduleDetail(requestedScheduleID);
    if (scheduleIDRef.current !== requestedScheduleID) return false;
    setDetail(next);
    setLoadError("");
    return true;
  }

  useEffect(() => {
    let alive = true;
    setLoading(true);
    setDetail(null);
    setHealthState({ scheduleID, loaded: false, health: undefined });
    setSection("brief");
    setShowEdit(false);
    setCommandMessage("");
    setCommandError("");
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

  useEffect(() => {
    const query = window.location.hash.split("?", 2)[1] ?? "";
    if (
      editEnabled &&
      new URLSearchParams(query).get("brief_action") === "edit_task"
    ) {
      setShowEdit(true);
    }
  }, [editEnabled, scheduleID]);

  async function runCommand(kind: "run" | "pause" | "resume") {
    if (
      (kind === "run" && !permissions?.can_run) ||
      ((kind === "pause" || kind === "resume") &&
        !permissions?.can_pause)
    ) {
      return;
    }
    if (commandRef.current) return;
    const requestedScheduleID = scheduleID;
    commandRef.current = kind;
    setCommand(kind);
    setCommandError("");
    setCommandMessage("");
    const idempotencyKey = commandIdempotencyKey(
      actorScope,
      requestedScheduleID,
      kind,
    );
    try {
      if (kind === "run") {
        await api.runScheduleNow(requestedScheduleID, idempotencyKey);
        if (scheduleIDRef.current === requestedScheduleID) {
          setCommandMessage(D.runAccepted);
        }
      } else if (kind === "pause") {
        await api.pauseSchedule(requestedScheduleID, idempotencyKey);
        if (await reloadDetail(requestedScheduleID)) {
          setCommandMessage(D.pauseDone);
        }
      } else {
        await api.resumeSchedule(requestedScheduleID, idempotencyKey);
        if (await reloadDetail(requestedScheduleID)) {
          setCommandMessage(D.resumeDone);
        }
      }
      clearCommandIdempotencyKey(actorScope, requestedScheduleID, kind);
    } catch (err) {
      if (scheduleIDRef.current === requestedScheduleID) {
        setCommandError(
          err instanceof ApiError ? err.message : t.app.common.loadFailed,
        );
      }
      if (!commandMayHaveReachedServer(err)) {
        clearCommandIdempotencyKey(actorScope, requestedScheduleID, kind);
      }
    } finally {
      commandRef.current = "";
      setCommand("");
    }
  }

  async function deleteTask() {
    if (!permissions?.can_delete) return;
    if (commandRef.current) return;
    if (!window.confirm(D.deleteConfirm)) return;
    const requestedScheduleID = scheduleID;
    commandRef.current = "delete";
    setCommand("delete");
    setCommandError("");
    const idempotencyKey = commandIdempotencyKey(
      actorScope,
      requestedScheduleID,
      "delete",
    );
    try {
      await api.deleteSchedule(requestedScheduleID, idempotencyKey);
      clearCommandIdempotencyKey(actorScope, requestedScheduleID, "delete");
      if (scheduleIDRef.current === requestedScheduleID) {
        location.hash = "#/tasks";
      }
    } catch (err) {
      if (scheduleIDRef.current === requestedScheduleID) {
        setCommandError(
          err instanceof ApiError ? err.message : t.app.common.loadFailed,
        );
      }
      if (!commandMayHaveReachedServer(err)) {
        clearCommandIdempotencyKey(actorScope, requestedScheduleID, "delete");
      }
    } finally {
      if (commandRef.current === "delete") {
        commandRef.current = "";
        setCommand("");
      }
    }
  }

  function handleEditComplete() {
    const requestedScheduleID = scheduleID;
    void reloadDetail(requestedScheduleID).catch((err) => {
      if (scheduleIDRef.current !== requestedScheduleID) return;
      setCommandError(
        err instanceof ApiError ? err.message : t.app.common.loadFailed,
      );
    });
  }

  function handleHealthAction(action: TaskHealthAction) {
    if (action === "run_again" && permissions?.can_run) {
      void runCommand("run");
      return;
    }
    if (action === "review_task" && permissions?.can_edit) {
      setShowEdit(true);
      return;
    }
    if (action === "review_usage" && permissions?.can_view_usage) {
      setSection("manage");
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

  const { schedule, summary, playbook } = detail;
  const nextRun = nextRunPresentation(schedule);
  // `observation` is the immutable runtime-policy projection; the alias keeps
  // this first read-only UI useful while older API deployments expose the
  // create-command spelling instead.
  const observation = schedule.scope.observation ?? schedule.scope.observation_policy;
  const latestCheck =
    latestCheckState?.scheduleID === scheduleID
      ? latestCheckState.check
      : undefined;
  const lastOutcome = latestCheck
    ? canonicalCheckOutcome(latestCheck)
    : taskRunOutcome(summary.last_status);
  const lastCheckAt = latestCheck?.finalized_at ?? summary.last_run_at;

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
            {hasTaskActions && (
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
                {permissions?.can_run && (
                  <DropdownMenuItem
                    onClick={() => void runCommand("run")}
                    disabled={schedule.status !== "active"}
                  >
                    <Play className="size-4" />
                    {D.runNow}
                  </DropdownMenuItem>
                )}
                {permissions?.can_pause && (
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
                )}
                {editEnabled && (
                  <DropdownMenuItem onClick={() => setShowEdit(true)}>
                    <Pencil className="size-4" />
                    {D.editTask}
                  </DropdownMenuItem>
                )}
                {permissions?.can_delete && (
                  <DropdownMenuItem
                    variant="destructive"
                    onClick={() => void deleteTask()}
                  >
                    <Trash2 className="size-4" />
                    {D.deleteTask}
                  </DropdownMenuItem>
                )}
              </DropdownMenuContent>
            </DropdownMenu>
            )}
          </div>
        </div>
        <p className="text-sm text-muted-foreground mt-1">
          {D.lastCheck}{" "}
          {lastCheckAt ? (
            <>
              {fmtBeijing(lastCheckAt)}
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

      <Tabs
        value={section}
        onValueChange={(value) => setSection(value as TaskSection)}
      >
        <TabsList className="w-full justify-start overflow-x-auto">
          <TabsTrigger value="brief">
            <Newspaper className="size-4" />
            {surface.tabs.brief}
          </TabsTrigger>
          <TabsTrigger value="manage">
            <Settings2 className="size-4" />
            {surface.tabs.manage}
          </TabsTrigger>
        </TabsList>
        <TabsContent value="brief">
          <Suspense
            fallback={
              <Card>
                <CardContent className="py-8">
                  <Skeleton className="h-24 w-full" />
                </CardContent>
              </Card>
            }
          >
            <TaskBriefFeed
              key={scheduleID}
              scheduleID={scheduleID}
              onLatestCheck={(check) =>
                setLatestCheckState({ scheduleID, check })
              }
              onHealth={(nextHealth) =>
                setHealthState({
                  scheduleID,
                  loaded: true,
                  health: nextHealth,
                })
              }
              onAdjustTask={
                editEnabled ? () => setShowEdit(true) : undefined
              }
              onCreateTask={() => setShowCreate(true)}
            />
          </Suspense>
        </TabsContent>
        <TabsContent value="manage">
          <div className="space-y-6">
            <p className="text-sm text-muted-foreground">
              {surface.manageDescription}
            </p>
            {health ? (
              <TaskHealthPanel
                health={health}
                copy={surface.health}
                locale={locale}
                onAction={handleHealthAction}
              />
            ) : healthLoaded ? (
              <Card>
                <CardContent className="py-5 text-sm text-muted-foreground">
                  {surface.healthUnavailable}
                </CardContent>
              </Card>
            ) : (
              <Card>
                <CardContent className="flex items-center gap-2 py-5 text-sm text-muted-foreground">
                  <Loader2 className="size-4 animate-spin" />
                  {surface.healthLoading}
                </CardContent>
              </Card>
            )}
            <section className="space-y-3">
              <h2 className="text-base font-semibold">
                {surface.playbookTitle}
              </h2>
              {playbook ? (
                <Card>
                  <CardContent className="space-y-3 py-5">
                    <p className="text-xs text-muted-foreground">
                      {D.playbookDesc} ·{" "}
                      {fmt(D.playbookUpdated, {
                        time: fmtBeijing(playbook.updated_at),
                      })}
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
            </section>
            {observation && (
              <section className="space-y-3">
                <h2 className="text-base font-semibold">
                  {surface.observationTitle}
                </h2>
                <ObservationTab policy={observation} />
              </section>
            )}
            <section className="space-y-3">
              <h2 className="text-base font-semibold">
                {surface.runsTitle}
              </h2>
              <RunsTab scheduleID={scheduleID} />
            </section>
          </div>
        </TabsContent>
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
            execute: D.generateEdit,
            executing: D.generatingEdit,
            close: D.closeEdit,
            requestFailed: A.requestFailed,
          }}
        />
      )}
      <TaskActionDialog
        open={showCreate}
        actorScope={actorScope}
        onClose={() => setShowCreate(false)}
        onComplete={() => {}}
        labels={{
          title: A.newTask,
          description: A.dialogDesc,
          placeholder: A.dialogPlaceholder,
          inputLabel: A.dialogInputLabel,
          execute: A.generate,
          executing: A.generating,
          close: A.close,
          requestFailed: A.requestFailed,
        }}
      />
    </div>
  );
}

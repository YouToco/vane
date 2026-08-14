import { useEffect, useMemo, useState } from "react";
import {
  AlertTriangle,
  Bot,
  Check,
  ChevronDown,
  Clock3,
  Copy,
  Database,
  Loader2,
  Search,
  TerminalSquare,
  UserRound,
  Wrench,
} from "lucide-react";
import { toast } from "sonner";

import { api, ApiError } from "@/shared/api/client";
import type {
  AdminExecutionTrace,
  AdminTraceEvent,
  AdminTraceRun,
  AdminTraceTask,
  AdminTraceUser,
} from "@/shared/api/client";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { cn } from "@/shared/utils/class-names";

function formatTime(value?: string): string {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleString("zh-CN", { hour12: false });
}

function formatDuration(ms?: number): string {
  if (ms === undefined) return "—";
  return ms < 1000 ? `${ms} ms` : `${(ms / 1000).toFixed(2)} s`;
}

function formatCost(event: AdminTraceEvent): string {
  if (event.cost_amount === undefined) return "未计价";
  const symbol = event.cost_currency === "CNY" ? "¥" : "$";
  return `${symbol}${event.cost_amount.toLocaleString("zh-CN", {
    maximumFractionDigits: 8,
  })}`;
}

function prettyJSON(value: unknown): string {
  if (value === undefined || value === null) return "{}";
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

async function copyText(value: string, label: string) {
  try {
    await navigator.clipboard.writeText(value);
    toast.success(`${label}已复制`);
  } catch {
    toast.error("复制失败，请手动选择文本");
  }
}

function CodePanel({ value, label }: { value: string; label: string }) {
  return (
    <div className="relative rounded-lg border bg-muted/35">
      <Button
        type="button"
        variant="ghost"
        size="sm"
        className="absolute right-2 top-2 z-10 h-8 gap-1.5 bg-background/80"
        onClick={() => void copyText(value, label)}
      >
        <Copy className="size-3.5" />
        复制
      </Button>
      <pre className="max-h-[32rem] overflow-auto whitespace-pre-wrap break-words p-4 pr-20 font-mono text-xs leading-6 text-foreground">
        {value || "（空）"}
      </pre>
    </div>
  );
}

function ModelEvent({ event, index }: { event: AdminTraceEvent; index: number }) {
  const failed = Boolean(event.error);
  return (
    <Collapsible defaultOpen>
      <Card className={cn("overflow-hidden", failed && "border-destructive/40")}>
        <CollapsibleTrigger className="flex w-full items-start gap-3 px-5 py-4 text-left hover:bg-muted/25">
          <span className="mt-0.5 flex size-9 shrink-0 items-center justify-center rounded-lg bg-violet-500/10 text-violet-700 dark:text-violet-300">
            <Bot className="size-4.5" />
          </span>
          <span className="min-w-0 flex-1">
            <span className="flex flex-wrap items-center gap-2">
              <span className="font-medium">
                {event.span_name || `模型调用 ${index + 1}`}
              </span>
              <Badge variant={failed ? "destructive" : "secondary"}>
                {failed ? "失败" : "完成"}
              </Badge>
              <Badge variant="outline">
                {[event.provider, event.model].filter(Boolean).join(" · ") ||
                  "模型未知"}
              </Badge>
            </span>
            <span className="mt-1.5 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
              <span>{formatTime(event.created_at)}</span>
              <span>{formatDuration(event.latency_ms)}</span>
              <span>
                {event.prompt_tokens ?? 0} 输入 /{" "}
                {event.completion_tokens ?? 0} 输出 token
              </span>
              <span>{formatCost(event)}</span>
              <span>{event.pricing_status || "计价状态未知"}</span>
            </span>
          </span>
          <ChevronDown className="mt-2 size-4 shrink-0 text-muted-foreground" />
        </CollapsibleTrigger>
        <CollapsibleContent>
          <CardContent className="border-t pt-4">
            {event.error ? (
              <Alert variant="destructive" className="mb-4">
                <AlertTriangle className="size-4" />
                <AlertDescription>{event.error}</AlertDescription>
              </Alert>
            ) : null}
            <Tabs defaultValue="system">
              <TabsList className="max-w-full justify-start overflow-x-auto">
                <TabsTrigger value="system">系统提示词</TabsTrigger>
                <TabsTrigger value="user">用户提示词</TabsTrigger>
                <TabsTrigger value="output">模型输出</TabsTrigger>
                <TabsTrigger value="settings">调用设置</TabsTrigger>
              </TabsList>
              <TabsContent value="system" className="mt-3">
                <CodePanel
                  value={event.system_prompt || ""}
                  label="系统提示词"
                />
              </TabsContent>
              <TabsContent value="user" className="mt-3">
                <CodePanel value={event.user_prompt || ""} label="用户提示词" />
              </TabsContent>
              <TabsContent value="output" className="mt-3">
                <CodePanel value={event.completion || ""} label="模型输出" />
              </TabsContent>
              <TabsContent value="settings" className="mt-3">
                <dl className="grid gap-3 rounded-lg border bg-muted/20 p-4 text-sm sm:grid-cols-2 lg:grid-cols-4">
                  <div>
                    <dt className="text-xs text-muted-foreground">温度</dt>
                    <dd className="mt-1 font-medium">
                      {event.temperature ?? "未显式设置"}
                    </dd>
                  </div>
                  <div>
                    <dt className="text-xs text-muted-foreground">最大 token</dt>
                    <dd className="mt-1 font-medium">
                      {event.max_tokens ?? "未显式设置"}
                    </dd>
                  </div>
                  <div>
                    <dt className="text-xs text-muted-foreground">阶段</dt>
                    <dd className="mt-1 font-medium">{event.span_name || "—"}</dd>
                  </div>
                  <div>
                    <dt className="text-xs text-muted-foreground">计价</dt>
                    <dd className="mt-1 font-medium">
                      {formatCost(event)} ·{" "}
                      {event.pricing_status || "状态未知"}
                    </dd>
                  </div>
                </dl>
              </TabsContent>
            </Tabs>
          </CardContent>
        </CollapsibleContent>
      </Card>
    </Collapsible>
  );
}

function ToolEvent({ event, index }: { event: AdminTraceEvent; index: number }) {
  const failed = Boolean(event.error || event.error_type);
  const args = prettyJSON(event.arguments);
  return (
    <Collapsible defaultOpen>
      <Card className={cn("overflow-hidden", failed && "border-destructive/40")}>
        <CollapsibleTrigger className="flex w-full items-start gap-3 px-5 py-4 text-left hover:bg-muted/25">
          <span className="mt-0.5 flex size-9 shrink-0 items-center justify-center rounded-lg bg-cyan-500/10 text-cyan-700 dark:text-cyan-300">
            <Wrench className="size-4.5" />
          </span>
          <span className="min-w-0 flex-1">
            <span className="flex flex-wrap items-center gap-2">
              <span className="font-medium">
                {event.tool_name || `工具调用 ${index + 1}`}
              </span>
              <Badge variant={failed ? "destructive" : "secondary"}>
                {failed ? "失败" : "完成"}
              </Badge>
              <Badge variant="outline">{event.tool_kind || "工具"}</Badge>
              {event.result_truncated ? (
                <Badge variant="outline" className="border-amber-500/50 text-amber-700 dark:text-amber-300">
                  历史结果已截断
                </Badge>
              ) : null}
            </span>
            <span className="mt-1.5 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
              <span>{formatTime(event.created_at)}</span>
              <span>{formatDuration(event.duration_ms)}</span>
              <span>{event.provider || "内部工具"}</span>
              <span>{formatCost(event)}</span>
              {event.http_status ? <span>HTTP {event.http_status}</span> : null}
            </span>
          </span>
          <ChevronDown className="mt-2 size-4 shrink-0 text-muted-foreground" />
        </CollapsibleTrigger>
        <CollapsibleContent>
          <CardContent className="space-y-4 border-t pt-4">
            {failed ? (
              <Alert variant="destructive">
                <AlertTriangle className="size-4" />
                <AlertDescription>
                  {[event.error_type, event.error].filter(Boolean).join(" · ")}
                </AlertDescription>
              </Alert>
            ) : null}
            {event.endpoint_path ? (
              <div className="rounded-md border bg-muted/20 px-3 py-2 font-mono text-xs">
                {event.endpoint_path}
              </div>
            ) : null}
            <div>
              <div className="mb-2 text-xs font-medium text-muted-foreground">
                实际调用参数
              </div>
              <CodePanel value={args} label="工具调用参数" />
            </div>
            <div>
              <div className="mb-2 flex flex-wrap items-center gap-2 text-xs font-medium text-muted-foreground">
                <span>结果{event.result_truncated ? "预览" : ""}</span>
                <span>· 持久化前大小 {event.result_size ?? 0} bytes</span>
              </div>
              {event.result_truncated ? (
                <Alert className="mb-2 border-amber-500/40 bg-amber-500/5">
                  <Database className="size-4 text-amber-700 dark:text-amber-300" />
                  <AlertDescription>
                    历史记录仅保存 8K 预览；以下不是完整上游响应。参数、状态和原始大小仍为完整记录。
                  </AlertDescription>
                </Alert>
              ) : null}
              <CodePanel
                value={event.result_preview || ""}
                label={event.result_truncated ? "工具结果预览" : "工具结果"}
              />
            </div>
          </CardContent>
        </CollapsibleContent>
      </Card>
    </Collapsible>
  );
}

function SelectionPanel({
  title,
  icon,
  children,
}: {
  title: string;
  icon: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <Card className="min-w-0 gap-0 overflow-hidden py-0">
      <CardHeader className="border-b px-4 py-3">
        <CardTitle className="flex items-center gap-2 text-sm">
          {icon}
          {title}
        </CardTitle>
      </CardHeader>
      <ScrollArea className="h-64">
        <CardContent className="space-y-1 p-2">{children}</CardContent>
      </ScrollArea>
    </Card>
  );
}

export default function ExecutionTraces() {
  const [users, setUsers] = useState<AdminTraceUser[]>([]);
  const [tasks, setTasks] = useState<AdminTraceTask[]>([]);
  const [runs, setRuns] = useState<AdminTraceRun[]>([]);
  const [selectedUser, setSelectedUser] = useState<AdminTraceUser>();
  const [selectedTask, setSelectedTask] = useState<AdminTraceTask>();
  const [selectedRun, setSelectedRun] = useState<AdminTraceRun>();
  const [trace, setTrace] = useState<AdminExecutionTrace>();
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const [tasksLoading, setTasksLoading] = useState(false);
  const [runsLoading, setRunsLoading] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    let alive = true;
    setLoading(true);
    api
      .adminTraceUsers()
      .then((items) => {
        if (!alive) return;
        setUsers(items);
        setSelectedUser((current) => current ?? items[0]);
        setError("");
      })
      .catch((cause) => {
        if (alive) {
          setError(
            cause instanceof ApiError ? cause.message : "用户列表加载失败",
          );
        }
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, []);

  useEffect(() => {
    if (!selectedUser) {
      setTasks([]);
      setTasksLoading(false);
      return;
    }
    let alive = true;
    setTasksLoading(true);
    setTasks([]);
    setRuns([]);
    setSelectedTask(undefined);
    setSelectedRun(undefined);
    setTrace(undefined);
    api
      .adminTraceTasks(selectedUser.tenant_id, selectedUser.user_id)
      .then((items) => {
        if (!alive) return;
        setTasks(items);
        setSelectedTask(items[0]);
      })
      .catch((cause) => {
        if (alive)
          setError(cause instanceof ApiError ? cause.message : "任务加载失败");
      })
      .finally(() => {
        if (alive) setTasksLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [selectedUser]);

  useEffect(() => {
    if (!selectedUser || !selectedTask) {
      setRuns([]);
      setRunsLoading(false);
      return;
    }
    let alive = true;
    setRunsLoading(true);
    setRuns([]);
    setSelectedRun(undefined);
    setTrace(undefined);
    api
      .adminTraceRuns(
        selectedUser.tenant_id,
        selectedUser.user_id,
        selectedTask.task_id,
      )
      .then((items) => {
        if (!alive) return;
        setRuns(items);
        setSelectedRun(items[0]);
      })
      .catch((cause) => {
        if (alive)
          setError(cause instanceof ApiError ? cause.message : "运行加载失败");
      })
      .finally(() => {
        if (alive) setRunsLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [selectedTask, selectedUser]);

  useEffect(() => {
    if (!selectedUser || !selectedTask || !selectedRun) {
      setTrace(undefined);
      return;
    }
    let alive = true;
    setDetailLoading(true);
    setTrace(undefined);
    api
      .adminExecutionTrace(
        selectedUser.tenant_id,
        selectedUser.user_id,
        selectedTask.task_id,
        selectedRun.snapshot_id,
      )
      .then((value) => {
        if (alive) {
          setTrace(value);
          setError("");
        }
      })
      .catch((cause) => {
        if (alive)
          setError(
            cause instanceof ApiError ? cause.message : "执行轨迹加载失败",
          );
      })
      .finally(() => {
        if (alive) setDetailLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [selectedRun, selectedTask, selectedUser]);

  const filteredUsers = useMemo(() => {
    const needle = query.trim().toLocaleLowerCase();
    if (!needle) return users;
    return users.filter((user) =>
      [user.name, user.email, String(user.user_id)]
        .join(" ")
        .toLocaleLowerCase()
        .includes(needle),
    );
  }, [query, users]);

  return (
    <div className="space-y-5">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <h2 className="text-lg font-semibold">执行轨迹</h2>
          <p className="mt-1 max-w-3xl text-sm text-muted-foreground">
            按用户、任务和单次运行查看当时真正发送给模型的提示词与工具调用。每次打开详情都会写入管理员访问审计。
          </p>
        </div>
        <div className="relative w-full lg:w-72">
          <Search className="absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="搜索姓名或邮箱"
            className="pl-9"
          />
        </div>
      </div>

      {error ? (
        <Alert variant="destructive">
          <AlertTriangle className="size-4" />
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      <div className="grid gap-3 lg:grid-cols-3">
        <SelectionPanel
          title={`用户 · ${users.length}`}
          icon={<UserRound className="size-4" />}
        >
          {loading ? (
            <div className="flex h-32 items-center justify-center">
              <Loader2 className="size-5 animate-spin text-muted-foreground" />
            </div>
          ) : filteredUsers.length === 0 ? (
            <div className="p-6 text-center text-sm text-muted-foreground">
              没有匹配用户
            </div>
          ) : (
            filteredUsers.map((user) => (
              <button
                key={`${user.tenant_id}:${user.user_id}`}
                type="button"
                onClick={() => setSelectedUser(user)}
                className={cn(
                  "w-full rounded-lg border border-transparent px-3 py-2.5 text-left transition-colors hover:bg-muted",
                  selectedUser?.tenant_id === user.tenant_id &&
                    selectedUser.user_id === user.user_id &&
                    "border-brand/30 bg-brand/10",
                )}
              >
                <div className="truncate text-sm font-medium">
                  {user.name || user.email || `用户 ${user.user_id}`}
                </div>
                <div className="mt-1 flex items-center justify-between gap-2 text-xs text-muted-foreground">
                  <span className="truncate">{user.email || "飞书用户"}</span>
                  <span className="shrink-0">{user.task_count} 个任务</span>
                </div>
              </button>
            ))
          )}
        </SelectionPanel>

        <SelectionPanel
          title={`任务 · ${tasks.length}`}
          icon={<TerminalSquare className="size-4" />}
        >
          {tasksLoading ? (
            <div className="flex h-32 items-center justify-center">
              <Loader2 className="size-5 animate-spin text-muted-foreground" />
            </div>
          ) : selectedUser && tasks.length === 0 ? (
            <div className="p-6 text-center text-sm text-muted-foreground">
              该用户暂无任务
            </div>
          ) : (
            tasks.map((task) => (
              <button
                key={task.task_id}
                type="button"
                onClick={() => setSelectedTask(task)}
                className={cn(
                  "w-full rounded-lg border border-transparent px-3 py-2.5 text-left transition-colors hover:bg-muted",
                  selectedTask?.task_id === task.task_id &&
                    "border-brand/30 bg-brand/10",
                )}
              >
                <div className="line-clamp-2 text-sm font-medium">
                  {task.title || "未命名任务"}
                </div>
                <div className="mt-1.5 flex items-center justify-between text-xs text-muted-foreground">
                  <span>{task.status}</span>
                  <span>{task.run_count} 次运行</span>
                </div>
              </button>
            ))
          )}
        </SelectionPanel>

        <SelectionPanel
          title={`运行 · 最近 ${runs.length} 次`}
          icon={<Clock3 className="size-4" />}
        >
          {runsLoading ? (
            <div className="flex h-32 items-center justify-center">
              <Loader2 className="size-5 animate-spin text-muted-foreground" />
            </div>
          ) : selectedTask && runs.length === 0 ? (
            <div className="p-6 text-center text-sm text-muted-foreground">
              该任务暂无运行快照
            </div>
          ) : (
            runs.map((run) => (
              <button
                key={run.snapshot_id}
                type="button"
                onClick={() => setSelectedRun(run)}
                className={cn(
                  "w-full rounded-lg border border-transparent px-3 py-2.5 text-left transition-colors hover:bg-muted",
                  selectedRun?.snapshot_id === run.snapshot_id &&
                    "border-brand/30 bg-brand/10",
                )}
              >
                <div className="flex items-center justify-between gap-2">
                  <span className="text-sm font-medium">
                    {formatTime(run.created_at)}
                  </span>
                  <Badge
                    variant={
                      run.result === "failed" ? "destructive" : "secondary"
                    }
                  >
                    {run.result || run.status}
                  </Badge>
                </div>
                <div className="mt-1.5 text-xs text-muted-foreground">
                  {run.model_calls} 次模型 · {run.tool_calls} 次工具
                </div>
              </button>
            ))
          )}
        </SelectionPanel>
      </div>

      {detailLoading ? (
        <Card>
          <CardContent className="flex h-48 items-center justify-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="size-5 animate-spin" />
            正在读取并记录访问审计…
          </CardContent>
        </Card>
      ) : trace ? (
        <div className="space-y-4">
          <Card>
            <CardContent className="grid gap-4 p-5 sm:grid-cols-2 xl:grid-cols-5">
              <div className="xl:col-span-2">
                <div className="text-xs text-muted-foreground">当前定位</div>
                <div className="mt-1 line-clamp-2 font-medium">
                  {selectedUser?.name || selectedUser?.email} /{" "}
                  {selectedTask?.title}
                </div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">运行结局</div>
                <div className="mt-1 flex items-center gap-1.5 font-medium">
                  {trace.run.result &&
                  !["failed", "interrupted"].includes(trace.run.result) ? (
                    <Check className="size-4 text-emerald-600" />
                  ) : null}
                  {trace.run.result || trace.run.status}
                </div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">证据覆盖 / 处理</div>
                <div className="mt-1 font-medium">
                  {trace.run.source_coverage || "待定"} /{" "}
                  {trace.run.processing || "待定"}
                </div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">调用规模</div>
                <div className="mt-1 font-medium">
                  {trace.run.model_calls} 模型 · {trace.run.tool_calls} 工具
                </div>
              </div>
            </CardContent>
          </Card>

          {trace.run.failure_message ? (
            <Alert variant="destructive">
              <AlertTriangle className="size-4" />
              <AlertDescription>
                {trace.run.failure_code} · {trace.run.failure_message}
              </AlertDescription>
            </Alert>
          ) : null}

          <div className="relative space-y-3 before:absolute before:bottom-5 before:left-[1.45rem] before:top-5 before:w-px before:bg-border">
            {trace.events.length === 0 ? (
              <Card>
                <CardContent className="flex min-h-36 items-center justify-center text-sm text-muted-foreground">
                  此运行没有可归因的模型或工具调用。系统不会按时间窗口猜测归属。
                </CardContent>
              </Card>
            ) : (
              trace.events.map((event, index) => (
                <div
                  key={`${event.kind}:${event.created_at}:${index}`}
                  className="relative pl-12"
                >
                  <span className="absolute left-[1.16rem] top-6 z-10 size-2.5 rounded-full border-2 border-background bg-brand" />
                  {event.kind === "model" ? (
                    <ModelEvent event={event} index={index} />
                  ) : (
                    <ToolEvent event={event} index={index} />
                  )}
                </div>
              ))
            )}
          </div>
        </div>
      ) : (
        <Card>
          <CardContent className="flex min-h-40 items-center justify-center gap-2 text-sm text-muted-foreground">
            <Bot className="size-5" />
            选择一名用户、任务和运行查看完整轨迹
          </CardContent>
        </Card>
      )}
    </div>
  );
}

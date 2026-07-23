import { useEffect, useState } from "react";
import {
  ArrowLeft,
  Loader2,
  ExternalLink,
  Send,
  PlayCircle,
  CircleSlash,
  Coins,
} from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
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
  PipelineCounts,
  ScheduleSourceInfo,
} from "../api";
import { fmt, useI18n, type Dict } from "@/i18n";
import { fmtBeijing } from "@/lib/time";
import DeliveriesTable from "@/components/DeliveriesTable";
import { batchOutcomeLabel, batchOutcomeVariant } from "./TaskDashboard";

const PAGE_SIZE = 20;

// 漏斗文本：只列**有记录**的阶段（缺席 = 那一步没跑，api.ts PipelineCounts 注释；
// 补零会把「没跑」编成「跑了得 0」）。全部缺席 → "—"。
function funnelText(c: PipelineCounts, d: Dict["app"]["taskDetail"]): string {
  const stages: [string, number | undefined][] = [
    [d.stageFetched, c.fetched],
    [d.stageDeduped, c.deduped],
    [d.stageScored, c.scored],
    [d.stageSelected, c.selected],
    [d.stageCards, c.cards],
  ];
  const parts = stages
    .filter(([, v]) => typeof v === "number")
    .map(([label, v]) => `${label} ${v}`);
  return parts.length > 0 ? parts.join(" → ") : "—";
}

// 运行历史 Tab：单任务 push_batches 倒序 + 键集分页。三态齐全，错误≠空。
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
              <TableHead className="text-right">{D.runsColPushed}</TableHead>
              <TableHead>{D.runsColFunnel}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {items.map((b) => (
              <TableRow key={b.id} className={b.status === "failed" ? "bg-destructive/5" : ""}>
                <TableCell className="text-sm whitespace-nowrap" title={`batch ${b.id}`}>
                  {fmtBeijing(b.created_at)}
                </TableCell>
                <TableCell>
                  <Badge variant={batchOutcomeVariant(b.status)}>
                    {batchOutcomeLabel(b.status, b.exit_gate, t.app.batch)}
                  </Badge>
                </TableCell>
                <TableCell className="text-right font-mono text-sm whitespace-nowrap">
                  {b.deliveries > 0
                    ? fmt(D.sentOfDeliveries, { sent: b.sent, total: b.deliveries })
                    : "—"}
                </TableCell>
                <TableCell className="text-xs text-muted-foreground whitespace-nowrap">
                  {funnelText(b.stage_counts, D)}
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

export default function TaskDetail({ scheduleID }: { scheduleID: string }) {
  const { t } = useI18n();
  const D = t.app.taskDetail;
  const [detail, setDetail] = useState<ScheduleDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");

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

  const { schedule, summary, sources, playbook, cost } = detail;
  const stats = [
    { icon: Send, label: D.statSent7d, value: String(summary.sent_pushes_7d) },
    { icon: PlayCircle, label: D.statRuns7d, value: String(summary.batches_7d) },
    { icon: CircleSlash, label: D.statEmpty7d, value: String(summary.empty_batches_7d) },
    {
      icon: Coins,
      label: D.statLLMCost,
      value: `$${cost.llm_cost_usd.toFixed(4)}`,
      sub: fmt(D.llmCalls, { n: cost.llm_calls }),
      title: D.costNote,
    },
  ];

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
        </div>
        <p className="text-sm text-muted-foreground mt-1">
          {D.lastRun}{" "}
          {summary.last_run_at ? (
            <>
              {fmtBeijing(summary.last_run_at)}
              <Badge
                variant={batchOutcomeVariant(summary.last_status)}
                className="ml-2 px-1.5 py-0 text-[10px]"
              >
                {batchOutcomeLabel(summary.last_status, summary.last_exit_gate, t.app.batch)}
              </Badge>
            </>
          ) : (
            D.neverRan
          )}
        </p>
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        {stats.map((s) => (
          <Card key={s.label} title={s.title}>
            <CardContent className="p-4 flex items-center gap-3">
              <s.icon className="size-4 text-muted-foreground shrink-0" />
              <div className="min-w-0">
                <div className="text-lg font-semibold leading-tight">{s.value}</div>
                <div className="text-xs text-muted-foreground truncate">
                  {s.label}
                  {s.sub ? ` · ${s.sub}` : ""}
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      <Tabs defaultValue="pushes">
        <TabsList>
          <TabsTrigger value="pushes">{D.tabPushes}</TabsTrigger>
          <TabsTrigger value="runs">{D.tabRuns}</TabsTrigger>
          <TabsTrigger value="sources">{D.tabSources}</TabsTrigger>
          <TabsTrigger value="playbook">{D.tabPlaybook}</TabsTrigger>
        </TabsList>
        <TabsContent value="pushes">
          <DeliveriesTable
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
      </Tabs>
    </div>
  );
}

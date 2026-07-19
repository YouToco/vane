import { Fragment, useEffect, useState } from "react";
import type { ReactNode } from "react";
import { api, ApiError } from "../api";
import type {
  ObservabilityReport,
  PipelineCounts,
  ProbeResult,
  ProbeStatus,
  ScoreBucket,
  SpanDayCost,
} from "../api";
import {
  Card,
  CardContent,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from "@/components/ui/table";
import {
  Collapsible,
  CollapsibleTrigger,
  CollapsibleContent,
} from "@/components/ui/collapsible";
import { Skeleton } from "@/components/ui/skeleton";
import {
  RefreshCw,
  Loader2,
  CheckCircle2,
  AlertTriangle,
  HelpCircle,
  ChevronDown,
  ChevronUp,
  Info,
} from "lucide-react";

const WINDOW_OPTIONS: [number, string][] = [
  [24, "24 小时"],
  [48, "48 小时"],
  [168, "7 天"],
];

const DEFAULT_WINDOW_HOURS = 24;

const STATUS_META: Record<ProbeStatus, { label: string; icon: ReactNode; cls: string; border: string; bg: string }> = {
  green: {
    label: "通过",
    icon: <CheckCircle2 className="size-5" />,
    cls: "text-emerald-700 dark:text-emerald-400",
    border: "border-l-emerald-500",
    bg: "bg-emerald-50 dark:bg-emerald-950/30",
  },
  yellow: {
    label: "未验到",
    icon: <HelpCircle className="size-5" />,
    cls: "text-amber-700 dark:text-amber-400",
    border: "border-l-amber-500 border-dashed",
    bg: "bg-amber-50 dark:bg-amber-950/30",
  },
  red: {
    label: "击穿",
    icon: <AlertTriangle className="size-5" />,
    cls: "text-red-700 dark:text-red-400",
    border: "border-l-red-500",
    bg: "bg-red-50 dark:bg-red-950/30",
  },
};

const WORST_META: Record<ProbeStatus, string> = {
  green: "7 条探针全部通过",
  yellow: "有探针未验到——不是通过，请按说明补跑",
  red: "有红线被击穿——按契约应回滚排查",
};

const BEIJING_TZ = "Asia/Shanghai";

function fmtBeijing(iso?: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString("zh-CN", { timeZone: BEIJING_TZ, hour12: false });
}

function fmtUTCDay(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${mm}-${dd}`;
}

function fmtUSD(v: number): string {
  return `$${v.toFixed(6)}`;
}

function shortTrace(id: string): string {
  return id.length > 12 ? `${id.slice(0, 8)}…` : id;
}

const GATE_META: Record<string, { label: string; hint: string }> = {
  fetch: { label: "无新内容", hint: "fetch：抓取跑完后无候选——压根没抓到新内容" },
  dedup: { label: "去重后空", hint: "dedup：去重跑完后无候选——抓到了，但全是重复" },
  score: { label: "打分后空", hint: "score：打分跑完后无候选" },
  select: { label: "择优后空", hint: "select：择优跑完后无候选" },
  cardgen: { label: "卡片生成后空", hint: "cardgen：卡片生成跑完后无候选" },
};

const FUNNEL_STAGES: [keyof PipelineCounts, string][] = [
  ["fetched", "抓取"],
  ["deduped", "去重"],
  ["scored", "打分"],
  ["selected", "择优"],
  ["cards", "卡片"],
];

function Funnel({ counts }: { counts: PipelineCounts }) {
  const steps = FUNNEL_STAGES.map(([k, label]) => ({ label, v: counts[k] })).filter(
    (s): s is { label: string; v: number } => s.v !== undefined,
  );
  if (steps.length === 0) return <span className="text-muted-foreground">—</span>;
  return (
    <span
      className="inline-flex items-center gap-1 font-mono text-sm"
      title={steps.map((s) => `${s.label} ${s.v}`).join(" → ")}
    >
      {steps.map((s, i) => (
        <Fragment key={s.label}>
          {i > 0 && <span className="text-muted-foreground" aria-hidden="true">→</span>}
          <span className="font-semibold">{s.v}</span>
        </Fragment>
      ))}
    </span>
  );
}

function ProbeCard({ r }: { r: ProbeResult }) {
  const meta = STATUS_META[r.status];
  const [open, setOpen] = useState(r.status !== "green");

  return (
    <Card className={`border-l-4 ${meta.border} ${meta.bg}`}>
      <div className="flex items-start gap-3 p-4">
        <span className={meta.cls}>{meta.icon}</span>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="font-medium text-sm">{r.name}</span>
            <Badge variant="outline" className="text-xs font-mono">
              {r.contract_ref}
            </Badge>
          </div>
          <p className="text-sm text-muted-foreground mt-0.5">{r.summary}</p>
        </div>
        <Badge variant="secondary" className={meta.cls}>
          {meta.label}
        </Badge>
      </div>
      {r.detail && (
        <Collapsible open={open} onOpenChange={setOpen}>
          <div className="px-4 pb-1">
            <CollapsibleTrigger render={<Button variant="ghost" size="sm" className="text-xs text-muted-foreground h-7" />}>
              {open ? (
                <>
                  <ChevronUp className="size-3 mr-1" />
                  收起说明
                </>
              ) : (
                <>
                  <ChevronDown className="size-3 mr-1" />
                  展开说明
                </>
              )}
            </CollapsibleTrigger>
          </div>
          <CollapsibleContent>
            <div className="px-4 pb-4 text-sm text-muted-foreground whitespace-pre-wrap">
              {r.detail}
            </div>
          </CollapsibleContent>
        </Collapsible>
      )}
    </Card>
  );
}

function Stat({ label, value, tone }: { label: string; value: ReactNode; tone?: string }) {
  return (
    <div className={`rounded-lg border p-3 ${tone ?? ""}`}>
      <div className="text-xl font-bold">{value}</div>
      <div className="text-xs text-muted-foreground mt-0.5">{label}</div>
    </div>
  );
}

function ScoreHistogram({ buckets }: { buckets: ScoreBucket[] }) {
  const total = buckets.reduce((s, b) => s + b.count, 0);
  if (total === 0) {
    return (
      <div className="py-8 text-center text-muted-foreground">
        窗口内没有可解析出分数的打分调用。
      </div>
    );
  }
  const max = Math.max(...buckets.map((b) => b.count));

  const W = 700;
  const H = 220;
  const padL = 40;
  const padR = 12;
  const padT = 22;
  const padB = 34;
  const plotW = W - padL - padR;
  const plotH = H - padT - padB;
  const slot = plotW / buckets.length;
  const barW = slot * 0.66;

  return (
    <svg
      className="w-full"
      viewBox={`0 0 ${W} ${H}`}
      role="img"
      aria-label="分数分布直方图"
    >
      <line
        x1={padL}
        y1={padT + plotH}
        x2={W - padR}
        y2={padT + plotH}
        stroke="hsl(var(--border))"
        strokeWidth="1"
      />
      <line
        x1={padL}
        y1={padT}
        x2={W - padR}
        y2={padT}
        stroke="hsl(var(--border))"
        strokeWidth="0.5"
        strokeDasharray="4 2"
      />
      <text
        x={padL - 8}
        y={padT + 4}
        fill="hsl(var(--muted-foreground))"
        fontSize="11"
        textAnchor="end"
      >
        {max}
      </text>
      <text
        x={padL - 8}
        y={padT + plotH + 4}
        fill="hsl(var(--muted-foreground))"
        fontSize="11"
        textAnchor="end"
      >
        0
      </text>
      {buckets.map((b, i) => {
        const h = max === 0 ? 0 : (b.count / max) * plotH;
        const x = padL + i * slot + (slot - barW) / 2;
        const y = padT + plotH - h;
        return (
          <g key={b.lo}>
            <rect
              x={x}
              y={y}
              width={barW}
              height={h}
              rx={3}
              fill="hsl(var(--primary))"
              opacity={0.8}
            >
              <title>{`${b.lo}–${b.hi}${i === buckets.length - 1 ? "（闭区间）" : ""}：${b.count} 次`}</title>
            </rect>
            {b.count > 0 && (
              <text
                x={x + barW / 2}
                y={y - 5}
                fill="hsl(var(--foreground))"
                fontSize="11"
                fontWeight="600"
                textAnchor="middle"
              >
                {b.count}
              </text>
            )}
            <text
              x={x + barW / 2}
              y={padT + plotH + 18}
              fill="hsl(var(--muted-foreground))"
              fontSize="11"
              textAnchor="middle"
            >
              {b.lo}
            </text>
          </g>
        );
      })}
      <text
        x={W - padR}
        y={padT + plotH + 18}
        fill="hsl(var(--muted-foreground))"
        fontSize="11"
        textAnchor="end"
      >
        100
      </text>
    </svg>
  );
}

interface CostDay {
  day: string;
  rows: SpanDayCost[];
  calls: number;
  cost: number;
}

function groupCosts(costs: SpanDayCost[]): CostDay[] {
  const out: CostDay[] = [];
  for (const c of costs) {
    let g = out.find((x) => x.day === c.day);
    if (!g) {
      g = { day: c.day, rows: [], calls: 0, cost: 0 };
      out.push(g);
    }
    g.rows.push(c);
    g.calls += c.calls;
    g.cost += c.cost_usd;
  }
  return out;
}

function batchBadge(status: string): "destructive" | "secondary" | "outline" {
  if (status === "failed" || status === "pending") return "destructive";
  if (status === "empty") return "secondary";
  return "outline";
}

export default function Observability() {
  const [windowHours, setWindowHours] = useState(DEFAULT_WINDOW_HOURS);
  const [report, setReport] = useState<ObservabilityReport | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    api
      .observability(windowHours)
      .then((r) => {
        if (!alive) return;
        setReport(r);
        setLoadError("");
      })
      .catch((err) => {
        if (!alive) return;
        setLoadError(err instanceof ApiError ? err.message : "加载失败");
        setReport(null);
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [windowHours, nonce]);

  const worst: ProbeStatus = report
    ? report.results.some((r) => r.status === "red")
      ? "red"
      : report.results.some((r) => r.status === "yellow")
        ? "yellow"
        : "green"
    : "green";

  const q = report?.quality;
  const inj = report?.injection;
  const ev = report?.evolve;
  const costDays = report ? groupCosts(report.costs) : [];

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <p className="text-sm text-muted-foreground">
          M5 契约 §16 的 Gate 服务端探针。判定全部由后端只读聚合算出，本页不参与判断、也不调用任何模型。
        </p>
        <Button
          variant="outline"
          size="sm"
          onClick={() => setNonce((n) => n + 1)}
          disabled={loading}
        >
          {loading ? (
            <Loader2 className="size-4 animate-spin" />
          ) : (
            <RefreshCw className="size-4" />
          )}
        </Button>
      </div>

      <div className="flex items-center justify-between flex-wrap gap-2">
        <Tabs
          value={String(windowHours)}
          onValueChange={(v) => setWindowHours(Number(v))}
        >
          <TabsList>
            {WINDOW_OPTIONS.map(([h, label]) => (
              <TabsTrigger key={h} value={String(h)}>
                {label}
              </TabsTrigger>
            ))}
          </TabsList>
        </Tabs>
        {report && (
          <span className="text-xs text-muted-foreground">
            生成于 {fmtBeijing(report.generated_at)}（北京时间）
          </span>
        )}
      </div>

      {windowHours !== DEFAULT_WINDOW_HOURS && (
        <Alert>
          <Info className="size-4" />
          <AlertDescription className="text-sm">
            当前窗口 {windowHours} 小时：探针红绿灯的口径随之变宽，而契约 §16.2 的回退率红线
            是按 24 小时定的。判 Gate 请切回 24 小时档，其余档位只用来看趋势。
          </AlertDescription>
        </Alert>
      )}

      {loadError && (
        <Alert variant="destructive">
          <AlertDescription>{loadError}</AlertDescription>
        </Alert>
      )}
      {loading && !report && (
        <div className="space-y-4">
          <Skeleton className="h-16 w-full" />
          <div className="space-y-3">
            {Array.from({ length: 4 }).map((_, i) => (
              <Skeleton key={i} className="h-20 w-full" />
            ))}
          </div>
        </div>
      )}

      {report && (
        <>
          {/* Verdict banner */}
          <div
            className={`flex items-center gap-3 rounded-lg border p-4 ${
              worst === "green"
                ? "border-emerald-200 bg-emerald-50 dark:border-emerald-800 dark:bg-emerald-950/50"
                : worst === "yellow"
                  ? "border-amber-200 border-dashed bg-amber-50 dark:border-amber-800 dark:bg-amber-950/50"
                  : "border-red-200 bg-red-50 dark:border-red-800 dark:bg-red-950/50"
            }`}
          >
            <span className={STATUS_META[worst].cls}>
              {STATUS_META[worst].icon}
            </span>
            <span className={`font-medium ${STATUS_META[worst].cls}`}>
              {WORST_META[worst]}
            </span>
          </div>

          {/* Probe cards */}
          <div className="space-y-2">
            {report.results.map((r) => (
              <ProbeCard key={`${r.id}:${r.status}`} r={r} />
            ))}
          </div>

          {/* Score distribution */}
          <div>
            <h3 className="text-lg font-semibold mb-3">分数分布</h3>
            <Card>
              <CardContent className="pt-6">
                <ScoreHistogram buckets={report.score_distribution} />
                <p className="text-xs text-muted-foreground mt-3">
                  横轴为 LLM 原始相关分（区间左闭右开，末桶 90–100 闭合），纵轴为打分次数。
                  只统计<strong>解析得出数字</strong>的成功调用：静默回退中位分 50 的那些调用（completion
                  里没有数字）根本不在图里——所以"没有 50 尖峰"不等于"没有回退"，
                  回退量请看下面的四联计数。
                </p>
              </CardContent>
            </Card>
          </div>

          {/* Score traces */}
          <div>
            <h3 className="text-lg font-semibold mb-3">每批区分度（n ≥ 5 的批次）</h3>
            <Card>
              {report.score_traces.length === 0 ? (
                <CardContent className="py-8 text-center text-muted-foreground">
                  窗口内没有规模 ≥5 的打分批次。
                </CardContent>
              ) : (
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>trace</TableHead>
                        <TableHead>开始（北京时间）</TableHead>
                        <TableHead className="text-right">打分次数 n</TableHead>
                        <TableHead className="text-right">不同输出</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {report.score_traces.map((t) => (
                        <TableRow
                          key={t.trace_id}
                          className={t.distinct_completions === 1 ? "bg-destructive/5" : ""}
                        >
                          <TableCell className="font-mono text-sm" title={t.trace_id}>
                            {shortTrace(t.trace_id)}
                          </TableCell>
                          <TableCell className="text-sm">{fmtBeijing(t.started_at)}</TableCell>
                          <TableCell className="text-right">{t.n}</TableCell>
                          <TableCell className="text-right">
                            <span className="inline-flex items-center gap-1.5">
                              {t.distinct_completions}
                              {t.distinct_completions === 1 && (
                                <Badge variant="destructive" className="text-xs">整批同分</Badge>
                              )}
                            </span>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              )}
              <div className="px-4 pb-4">
                <p className="text-xs text-muted-foreground mt-3">
                  "不同输出"数的是模型<strong>原话</strong>去重（"85" 与 "85分" 算两种），不是夹逼后的分数——
                  这个方向只会让计数偏高、更不容易误报，而 M3 事故那种整批逐字节相同的 "50"
                  依然会掉到 1。
                </p>
              </div>
            </Card>
          </div>

          {/* Scoring quality */}
          <div>
            <h3 className="text-lg font-semibold mb-3">打分质量</h3>
            <Card>
              <CardContent className="pt-6 space-y-3">
                <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                  <Stat label="成功调用 ok_total" value={q?.ok_total ?? 0} />
                  <Stat
                    label="无数字 no_digit（回退 50）"
                    value={q?.no_digit ?? 0}
                    tone={q && q.no_digit > 0 ? "border-amber-300 bg-amber-50 dark:border-amber-700 dark:bg-amber-950/30" : undefined}
                  />
                  <Stat
                    label="空输出无报错 empty_no_error"
                    value={q?.empty_no_error ?? 0}
                    tone={q && q.empty_no_error > 0 ? "border-red-300 bg-red-50 dark:border-red-700 dark:bg-red-950/30" : undefined}
                  />
                  <Stat label="调用失败 errored" value={q?.errored ?? 0} />
                </div>
                <p className="text-xs text-muted-foreground">
                  四者关系：empty_no_error ⊂ no_digit ⊂ ok_total，errored 与前三者互斥。
                  errored 的条目被 pipeline 直接跳过、<strong>一分未发</strong>，所以不算进回退率的分母——
                  否则一次上游 429 抖动就能冲爆 10% 红线。empty_no_error 是 M3 事故的精确形状，零容忍。
                </p>
              </CardContent>
            </Card>
          </div>

          {/* Profile injection */}
          <div>
            <h3 className="text-lg font-semibold mb-3">画像注入</h3>
            <Card>
              <CardContent className="pt-6 space-y-3">
                <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                  <Stat label="打分总数 total" value={inj?.total ?? 0} />
                  <Stat label="注入真实画像 present" value={inj?.present ?? 0} />
                  <Stat
                    label="拿到「暂无」absent"
                    value={inj?.absent ?? 0}
                    tone={inj && inj.absent > 0 && ev?.has_profile ? "border-red-300 bg-red-50 dark:border-red-700 dark:bg-red-950/30" : undefined}
                  />
                  <Stat
                    label="无法识别 unrecognized"
                    value={inj?.unrecognized ?? 0}
                    tone={inj && inj.unrecognized > 0 ? "border-red-300 bg-red-50 dark:border-red-700 dark:bg-red-950/30" : undefined}
                  />
                </div>
                <p className="text-xs text-muted-foreground">
                  unrecognized 是探针的自检位，恒应为 0：它 &gt;0 说明 scorer 的 prompt 结构变了
                  而探针字面量没跟上——那是探针坏了，不是画像坏了，先修探针再谈判定。
                  owner 尚无画像时 absent 全中是<strong>正确行为</strong>，不是故障。
                </p>
                <div className="rounded-lg border p-3 space-y-2">
                  <div className="flex justify-between text-sm">
                    <span className="text-muted-foreground">负面清单保尾</span>
                    <span>
                      {report.neg_tail.expected_tail === ""
                        ? "当前画像无「不感兴趣：」句，不适用"
                        : `${report.neg_tail.intact} / ${report.neg_tail.total} 条打分完整含负面句`}
                    </span>
                  </div>
                  {report.neg_tail.expected_tail !== "" && (
                    <div className="flex justify-between text-sm">
                      <span className="text-muted-foreground">期望串</span>
                      <span className="font-mono text-xs break-all text-right max-w-[60%]">
                        {report.neg_tail.expected_tail}
                      </span>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Daily cost */}
          <div>
            <h3 className="text-lg font-semibold mb-3">日成本（按 UTC 日 × span）</h3>
            <Card>
              {costDays.length === 0 ? (
                <CardContent className="py-8 text-center text-muted-foreground">
                  窗口内没有 LLM 调用。
                </CardContent>
              ) : (
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>UTC 日</TableHead>
                        <TableHead>span</TableHead>
                        <TableHead className="text-right">调用</TableHead>
                        <TableHead className="text-right">成本 USD</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {costDays.map((g) => (
                        <Fragment key={g.day}>
                          {g.rows.map((c, i) => (
                            <TableRow key={`${g.day}:${c.span_name}`}>
                              <TableCell className="font-mono text-sm">
                                {i === 0 ? fmtUTCDay(g.day) : ""}
                              </TableCell>
                              <TableCell className="font-mono text-sm">
                                <span className="inline-flex items-center gap-1.5">
                                  {c.span_name}
                                  {c.span_name === "score" && (
                                    <Badge variant="outline" className="text-xs">红线口径</Badge>
                                  )}
                                </span>
                              </TableCell>
                              <TableCell className="text-right">{c.calls}</TableCell>
                              <TableCell className="text-right font-mono">{fmtUSD(c.cost_usd)}</TableCell>
                            </TableRow>
                          ))}
                          <TableRow className="bg-muted/50 font-medium">
                            <TableCell />
                            <TableCell>当日合计</TableCell>
                            <TableCell className="text-right">{g.calls}</TableCell>
                            <TableCell className="text-right font-mono">{fmtUSD(g.cost)}</TableCell>
                          </TableRow>
                        </Fragment>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              )}
              <div className="px-4 pb-4">
                <p className="text-xs text-muted-foreground mt-3">
                  日界是 UTC（DB 原生），<strong>不是北京日</strong>：一个 UTC 日的桶装的是北京 08:00 到次日
                  08:00 的调用，故此列刻意不做时区换算。环比红线只卡 score span——M5 新增的
                  profile_evolve / deep_dive 是全新 span，全 span 环比测的是"上了新功能"而非
                  "注入变贵"。另：cost_usd 逐行舍入后求和，score 最便宜（MaxTokens=16），
                  整批舍成 0 是正常的。
                </p>
              </div>
            </Card>
          </div>

          {/* Model usage */}
          <div>
            <h3 className="text-lg font-semibold mb-3">model 用量</h3>
            <Card>
              {report.models.length === 0 ? (
                <CardContent className="py-8 text-center text-muted-foreground">
                  窗口内没有 LLM 调用。
                </CardContent>
              ) : (
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>model</TableHead>
                        <TableHead className="text-right">调用</TableHead>
                        <TableHead className="text-right">成本 USD</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {report.models.map((m) => (
                        <TableRow key={m.model}>
                          <TableCell className="font-mono text-sm">{m.model || "（空）"}</TableCell>
                          <TableCell className="text-right">{m.calls}</TableCell>
                          <TableCell className="text-right font-mono">{fmtUSD(m.cost_usd)}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              )}
              <div className="px-4 pb-4">
                <p className="text-xs text-muted-foreground mt-3">
                  这里的 model 是<strong>上游报回的名字</strong>。计价按它查价，未知 key 静默回落 v4-pro 价
                  （约 flash 的 3 倍），且不产生任何 error——上游一次改名就能无声烧穿预算。
                  盯着这张表出现陌生名字，是唯一能提前看见它的角度。
                </p>
              </div>
            </Card>
          </div>

          {/* Evolution health */}
          <div>
            <h3 className="text-lg font-semibold mb-3">演化健康</h3>
            <Card>
              <CardContent className="pt-6 space-y-3">
                <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
                  <div className="rounded-lg border p-3">
                    <p className="text-xs text-muted-foreground">窗口内演化调用</p>
                    <p className="text-lg font-bold mt-0.5">
                      {ev?.calls ?? 0} 次
                      {ev && ev.errored > 0 && (
                        <Badge variant="destructive" className="ml-2 text-xs">失败 {ev.errored}</Badge>
                      )}
                    </p>
                  </div>
                  <div className="rounded-lg border p-3">
                    <p className="text-xs text-muted-foreground">最近一次演化调用</p>
                    <p className="text-sm font-medium mt-0.5">
                      {ev?.last_call_at ? fmtBeijing(ev.last_call_at) : "从未演化"}
                    </p>
                  </div>
                  <div className="rounded-lg border p-3">
                    <p className="text-xs text-muted-foreground">画像更新于</p>
                    <p className="text-sm font-medium mt-0.5">
                      {ev?.has_profile ? fmtBeijing(ev.profile_updated_at) : "owner 尚无画像"}
                    </p>
                  </div>
                  <div className="rounded-lg border p-3">
                    <p className="text-xs text-muted-foreground">反馈游标 cursor</p>
                    <p className="text-lg font-bold font-mono mt-0.5">{ev?.cursor ?? 0}</p>
                  </div>
                  <div className="rounded-lg border p-3">
                    <p className="text-xs text-muted-foreground">标签数</p>
                    <p className="text-lg font-bold mt-0.5">{ev?.tag_count ?? 0}</p>
                  </div>
                  <div className="rounded-lg border p-3">
                    <p className="text-xs text-muted-foreground">summary 字数</p>
                    <p className="text-lg font-bold mt-0.5">{ev?.summary_runes ?? 0}</p>
                  </div>
                </div>
                <p className="text-xs text-muted-foreground">
                  "最近一次演化调用"不受窗口约束（要拿它和画像 updated_at 比先后，上次演化可能
                  落在窗口外）。画像 updated_at 早于最近一次演化调用 = 这批反馈没写回画像。
                  注意 updated_at 无法归因写入者：人工改画像也会刷新它。
                </p>
              </CardContent>
            </Card>
          </div>

          {/* Batch history */}
          <div>
            <h3 className="text-lg font-semibold mb-3">推送批次历史（近 14 天）</h3>
            <Alert className="mb-3">
              <AlertTitle className="text-sm font-semibold">怎么读这张表：空批次现在有行了，跑崩的运行仍然没有。</AlertTitle>
              <AlertDescription className="text-xs mt-1">
                pipeline 五处提前退出（无新内容 / 去重后空 / 打分后空 / 择优后空 / 卡片生成后空）
                现在各留一行 <code className="bg-muted px-1 rounded">status=empty</code> 的批次，「闸门」列说明从哪一步退的，
                「漏斗」列说明到那一步还剩几条——"今早没新内容"从此在库里查得到，
                且它是<strong>正常终态不是事故</strong>，故给静音灰，不给告警色。
                但 <strong>pipeline 中途报错的运行仍然没有行</strong>：Fetch/Score 等活动重试耗尽后
                workflow 直接失败返回，走不到任何闸门；那类运行在 Temporal 里是 Failed，
                本表看不见。所以这张表的语义是<strong>"推送决策的产物"，不是"每次触发的流水账"</strong>。
              </AlertDescription>
            </Alert>
            <Card>
              {report.batches.length === 0 ? (
                <CardContent className="py-8 text-center text-muted-foreground">
                  近 14 天没有建成的推送批次。
                </CardContent>
              ) : (
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="text-right">id</TableHead>
                        <TableHead>状态</TableHead>
                        <TableHead>闸门</TableHead>
                        <TableHead>漏斗</TableHead>
                        <TableHead>创建（北京时间）</TableHead>
                        <TableHead className="text-right">投递</TableHead>
                        <TableHead className="text-right">已发</TableHead>
                        <TableHead className="text-right">原始分 最低–最高</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {report.batches.map((b) => {
                        const gate = GATE_META[b.exit_gate];
                        return (
                          <TableRow
                            key={b.id}
                            className={b.status === "failed" ? "bg-destructive/5" : ""}
                          >
                            <TableCell className="text-right font-mono text-sm">{b.id}</TableCell>
                            <TableCell>
                              <Badge variant={batchBadge(b.status)}>{b.status}</Badge>
                            </TableCell>
                            <TableCell title={gate?.hint}>
                              {b.exit_gate === "" ? (
                                <span className="text-muted-foreground">—</span>
                              ) : (
                                <Badge variant="secondary">{gate?.label ?? b.exit_gate}</Badge>
                              )}
                            </TableCell>
                            <TableCell>
                              <Funnel counts={b.stage_counts} />
                            </TableCell>
                            <TableCell
                              className="text-sm whitespace-nowrap"
                              title={`幂等键 / traceID：${b.idempotency_key}`}
                            >
                              {fmtBeijing(b.created_at)}
                            </TableCell>
                            <TableCell
                              className={`text-right ${
                                b.status !== "empty" && b.delivery_count === 0
                                  ? "text-destructive font-semibold"
                                  : ""
                              }`}
                            >
                              {b.delivery_count}
                            </TableCell>
                            <TableCell className="text-right">{b.sent_count}</TableCell>
                            <TableCell className="text-right font-mono text-sm">
                              {b.min_score === undefined || b.max_score === undefined
                                ? "—"
                                : `${b.min_score.toFixed(0)}–${b.max_score.toFixed(0)}`}
                            </TableCell>
                          </TableRow>
                        );
                      })}
                    </TableBody>
                  </Table>
                </div>
              )}
              <div className="px-4 pb-4">
                <p className="text-xs text-muted-foreground mt-3">
                  「闸门」非空 ⇔ 该批在 Push 之前就没候选了（status 恒为 empty）。「漏斗」只画
                  <strong>真跑过</strong>的阶段：<code className="bg-muted px-1 rounded">20→0</code> 是"抓到 20 条、去重后剩 0"，后面的打分/择优
                  压根没被调用——所以那里没有数字，而不是 0。投递数为 0 只在 done/failed 批次上才是异常；
                  empty 批次没有投递是正确的。分数是 LLM <strong>原始相关分</strong>，不是排序用的有效分。
                </p>
              </div>
            </Card>
          </div>
        </>
      )}
    </div>
  );
}

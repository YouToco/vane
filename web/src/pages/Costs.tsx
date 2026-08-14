import { useEffect, useState } from "react";
import { Fragment } from "react";
import { api, ApiError } from "../api";
import type { RunstatsResp, SpanDayCost } from "../api";
import {
  Card,
  CardContent,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
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
  RefreshCw,
  Loader2,
  DollarSign,
  Cpu,
  Zap,
  AlertTriangle,
} from "lucide-react";

const WINDOW_OPTIONS: [number, string][] = [
  [24, "24 小时"],
  [168, "7 天"],
  [720, "30 天"],
];
const DEFAULT_WINDOW_HOURS = 24;

function fmtUSD(v: number): string {
  return `$${v.toFixed(6)}`;
}

function fmtInt(v: number): string {
  return v.toLocaleString("en-US");
}

function fmtMs(v: number): string {
  return `${Math.round(v)} ms`;
}

function fmtUTCDay(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${mm}-${dd}`;
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

export default function Costs() {
  const [report, setReport] = useState<RunstatsResp | null>(null);
  const [windowHours, setWindowHours] = useState(DEFAULT_WINDOW_HOURS);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    api
      .runstats(windowHours)
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

  const totals = report
    ? report.spans.reduce(
        (a, s) => ({
          calls: a.calls + s.calls,
          errors: a.errors + s.errors,
          cost: a.cost + s.cost_usd,
          tokens: a.tokens + s.prompt_tokens + s.completion_tokens,
        }),
        { calls: 0, errors: 0, cost: 0, tokens: 0 },
      )
    : null;
  const costDays = report ? groupCosts(report.days) : [];

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <p className="text-sm text-muted-foreground">
          LLM 调用的成本、token、延迟与缓存命中。数据来自后端只读聚合，本页不参与任何判定。
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

      {loadError && (
        <Alert variant="destructive">
          <AlertDescription>{loadError}</AlertDescription>
        </Alert>
      )}

      {loading && !report ? (
        <div className="space-y-4">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {Array.from({ length: 4 }).map((_, i) => (
              <Card key={i}>
                <CardContent className="pt-6">
                  <Skeleton className="h-8 w-24 mb-2" />
                  <Skeleton className="h-4 w-16" />
                </CardContent>
              </Card>
            ))}
          </div>
        </div>
      ) : (
        report && (
          <>
            {totals && (
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <Card>
                  <CardContent className="pt-6">
                    <div className="flex items-center gap-2 mb-2">
                      <DollarSign className="size-4 text-muted-foreground" />
                      <span className="text-2xl font-bold">{fmtUSD(totals.cost)}</span>
                    </div>
                    <p className="text-xs text-muted-foreground">窗口总成本</p>
                  </CardContent>
                </Card>
                <Card>
                  <CardContent className="pt-6">
                    <div className="flex items-center gap-2 mb-2">
                      <Cpu className="size-4 text-muted-foreground" />
                      <span className="text-2xl font-bold">{fmtInt(totals.calls)}</span>
                    </div>
                    <p className="text-xs text-muted-foreground">LLM 调用次数</p>
                  </CardContent>
                </Card>
                <Card>
                  <CardContent className="pt-6">
                    <div className="flex items-center gap-2 mb-2">
                      <Zap className="size-4 text-muted-foreground" />
                      <span className="text-2xl font-bold">{fmtInt(totals.tokens)}</span>
                    </div>
                    <p className="text-xs text-muted-foreground">总 token</p>
                  </CardContent>
                </Card>
                <Card className={totals.errors > 0 ? "border-destructive/50 bg-destructive/5" : ""}>
                  <CardContent className="pt-6">
                    <div className="flex items-center gap-2 mb-2">
                      <AlertTriangle className={`size-4 ${totals.errors > 0 ? "text-destructive" : "text-muted-foreground"}`} />
                      <span className="text-2xl font-bold">{fmtInt(totals.errors)}</span>
                    </div>
                    <p className="text-xs text-muted-foreground">错误调用</p>
                  </CardContent>
                </Card>
              </div>
            )}

            <div>
              <h3 className="text-lg font-semibold mb-3">按环节（span）</h3>
              <Card>
                {report.spans.length === 0 ? (
                  <CardContent className="py-8 text-center text-muted-foreground">
                    窗口内没有 LLM 调用。
                  </CardContent>
                ) : (
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>span</TableHead>
                          <TableHead className="text-right">调用</TableHead>
                          <TableHead className="text-right">错误</TableHead>
                          <TableHead className="text-right">成本</TableHead>
                          <TableHead className="text-right">输入 token</TableHead>
                          <TableHead className="text-right">输出 token</TableHead>
                          <TableHead className="text-right">延迟 avg</TableHead>
                          <TableHead className="text-right">p95</TableHead>
                          <TableHead className="text-right">缓存命中</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {report.spans.map((s) => (
                          <TableRow
                            key={s.span_name}
                            className={s.errors > 0 ? "bg-destructive/5" : ""}
                          >
                            <TableCell className="font-mono text-sm">{s.span_name}</TableCell>
                            <TableCell className="text-right">{fmtInt(s.calls)}</TableCell>
                            <TableCell className="text-right">
                              {s.errors > 0 ? fmtInt(s.errors) : "—"}
                            </TableCell>
                            <TableCell className="text-right font-mono">{fmtUSD(s.cost_usd)}</TableCell>
                            <TableCell className="text-right">{fmtInt(s.prompt_tokens)}</TableCell>
                            <TableCell className="text-right">{fmtInt(s.completion_tokens)}</TableCell>
                            <TableCell className="text-right">{fmtMs(s.avg_latency_ms)}</TableCell>
                            <TableCell className="text-right">{fmtMs(s.p95_latency_ms)}</TableCell>
                            <TableCell className="text-right">
                              {s.cache_known > 0
                                ? `${Math.round((s.cache_hits / s.cache_known) * 100)}% (${s.cache_hits}/${s.cache_known})`
                                : "—"}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                )}
              </Card>
            </div>

            <div>
              <h3 className="text-lg font-semibold mb-3">按模型</h3>
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
                          <TableHead>模型（上游报回名）</TableHead>
                          <TableHead className="text-right">调用</TableHead>
                          <TableHead className="text-right">成本</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {report.models.map((m) => (
                          <TableRow key={m.model}>
                            <TableCell className="font-mono text-sm">{m.model}</TableCell>
                            <TableCell className="text-right">{fmtInt(m.calls)}</TableCell>
                            <TableCell className="text-right font-mono">{fmtUSD(m.cost_usd)}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                )}
              </Card>
            </div>

            <div>
              <h3 className="text-lg font-semibold mb-3">按 UTC 日（span 分列）</h3>
              <Card>
                {costDays.length === 0 ? (
                  <CardContent className="py-8 text-center text-muted-foreground">
                    窗口内没有成本记录。
                  </CardContent>
                ) : (
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>UTC 日</TableHead>
                          <TableHead>span</TableHead>
                          <TableHead className="text-right">调用</TableHead>
                          <TableHead className="text-right">成本</TableHead>
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
                                <TableCell className="font-mono text-sm">{c.span_name}</TableCell>
                                <TableCell className="text-right">{fmtInt(c.calls)}</TableCell>
                                <TableCell className="text-right font-mono">{fmtUSD(c.cost_usd)}</TableCell>
                              </TableRow>
                            ))}
                            <TableRow className="bg-muted/50 font-medium">
                              <TableCell />
                              <TableCell>当日合计</TableCell>
                              <TableCell className="text-right">{fmtInt(g.calls)}</TableCell>
                              <TableCell className="text-right font-mono">{fmtUSD(g.cost)}</TableCell>
                            </TableRow>
                          </Fragment>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                )}
              </Card>
            </div>
          </>
        )
      )}
    </div>
  );
}

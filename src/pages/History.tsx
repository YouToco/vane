import { useEffect, useState } from "react";
import { api, ApiError } from "../api";
import type { DeliveryHistoryItem } from "../api";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from "@/components/ui/table";
import {
  Tooltip,
  TooltipTrigger,
  TooltipContent,
} from "@/components/ui/tooltip";
import { RefreshCw, Loader2, ExternalLink } from "lucide-react";

const PAGE_SIZE = 20;

const BEIJING_TZ = "Asia/Shanghai";

function fmtBeijing(iso?: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString("zh-CN", { timeZone: BEIJING_TZ, hour12: false });
}

function statusBadge(status: string): "destructive" | "secondary" | "outline" {
  if (status === "failed" || status === "pending") return "destructive";
  return "secondary";
}

const FEEDBACK_META: Record<string, { label: string; variant: "default" | "secondary" | "outline" | "destructive"; showDetail: boolean }> = {
  interested: { label: "👍 感兴趣", variant: "default", showDetail: false },
  not_interested: { label: "👎 不感兴趣", variant: "secondary", showDetail: false },
  misjudged: { label: "⚠️ 误判", variant: "destructive", showDetail: true },
  deep_dive: { label: "🔍 深入", variant: "outline", showDetail: false },
  question: { label: "💬 追问", variant: "outline", showDetail: true },
};

function clipDetail(s: string, max = 120): string {
  const runes = Array.from(s);
  return runes.length <= max ? s : runes.slice(0, max).join("") + "…";
}

export default function History() {
  const [items, setItems] = useState<DeliveryHistoryItem[]>([]);
  const [total, setTotal] = useState(0);
  const [nextToken, setNextToken] = useState("");
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [loadError, setLoadError] = useState("");
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    api
      .listDeliveries(PAGE_SIZE)
      .then((r) => {
        if (!alive) return;
        setItems(r.items);
        setTotal(r.total);
        setNextToken(r.next_page_token ?? "");
        setLoadError("");
      })
      .catch((err) => {
        if (!alive) return;
        setLoadError(err instanceof ApiError ? err.message : "加载失败");
        setItems([]);
        setTotal(0);
        setNextToken("");
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [nonce]);

  async function loadMore() {
    if (!nextToken || loadingMore) return;
    setLoadingMore(true);
    try {
      const r = await api.listDeliveries(PAGE_SIZE, nextToken);
      setItems((prev) => prev.concat(r.items));
      setTotal(r.total);
      setNextToken(r.next_page_token ?? "");
      setLoadError("");
    } catch (err) {
      setLoadError(err instanceof ApiError ? err.message : "加载失败");
    } finally {
      setLoadingMore(false);
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm text-muted-foreground">
            每条推送的打分、发送状态与你在飞书卡片上的反馈。反馈请在卡片上操作，这里只做回看。
          </p>
        </div>
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

      {loadError && (
        <Alert variant="destructive">
          <AlertDescription>{loadError}</AlertDescription>
        </Alert>
      )}

      {loading ? (
        <Card>
          <CardContent className="py-6 space-y-3">
            {Array.from({ length: 5 }).map((_, i) => (
              <div key={i} className="flex gap-4">
                <Skeleton className="h-4 w-32" />
                <Skeleton className="h-4 w-48" />
                <Skeleton className="h-4 w-12" />
                <Skeleton className="h-4 w-16" />
              </div>
            ))}
          </CardContent>
        </Card>
      ) : items.length === 0 ? (
        !loadError && (
          <Card>
            <CardContent className="py-12 text-center text-muted-foreground">
              还没有推送记录。
            </CardContent>
          </Card>
        )
      ) : (
        <Card>
          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>推送时间（北京）</TableHead>
                  <TableHead>内容</TableHead>
                  <TableHead className="text-right">分数</TableHead>
                  <TableHead>状态</TableHead>
                  <TableHead>反馈</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {items.map((it) => (
                  <TableRow
                    key={it.id}
                    className={it.status === "failed" ? "bg-destructive/5" : ""}
                  >
                    <TableCell
                      className="text-sm whitespace-nowrap"
                      title={`delivery ${it.id} · batch ${it.batch_id}`}
                    >
                      {fmtBeijing(it.sent_at ?? it.created_at)}
                    </TableCell>
                    <TableCell className="max-w-[200px] truncate">
                      {it.url ? (
                        <a
                          href={it.url}
                          target="_blank"
                          rel="noreferrer"
                          className="inline-flex items-center gap-1 text-primary hover:underline"
                        >
                          {it.title || "（无内容）"}
                          <ExternalLink className="size-3 shrink-0" />
                        </a>
                      ) : (
                        <span className="text-muted-foreground">
                          {it.title || "（内容已删除）"}
                        </span>
                      )}
                    </TableCell>
                    <TableCell className="text-right font-mono text-sm">
                      {it.score}
                    </TableCell>
                    <TableCell>
                      <Badge variant={statusBadge(it.status)}>{it.status}</Badge>
                    </TableCell>
                    <TableCell>
                      {it.feedbacks.length === 0 ? (
                        <span className="text-muted-foreground">—</span>
                      ) : (
                        <div className="flex flex-wrap gap-1">
                          {it.feedbacks.map((fb, i) => {
                            const meta = FEEDBACK_META[fb.action];
                            const tooltipText =
                              fmtBeijing(fb.created_at) +
                              (fb.detail ? ` · ${clipDetail(fb.detail)}` : "");
                            return (
                              <Tooltip key={i}>
                                <TooltipTrigger render={<span />}>
                                  <Badge variant={meta?.variant ?? "secondary"}>
                                    {meta?.label ?? fb.action}
                                    {meta?.showDetail && fb.detail && (
                                      <span className="ml-1 opacity-75">
                                        {clipDetail(fb.detail, 30)}
                                      </span>
                                    )}
                                  </Badge>
                                </TooltipTrigger>
                                <TooltipContent>
                                  <p className="max-w-xs">{tooltipText}</p>
                                </TooltipContent>
                              </Tooltip>
                            );
                          })}
                        </div>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
          <div className="flex items-center justify-between border-t px-4 py-3">
            <span className="text-sm text-muted-foreground">
              已显示 {items.length} / {total} 条
            </span>
            {nextToken && (
              <Button
                variant="outline"
                size="sm"
                onClick={loadMore}
                disabled={loadingMore}
              >
                {loadingMore ? (
                  <>
                    <Loader2 className="mr-2 size-4 animate-spin" />
                    加载中…
                  </>
                ) : (
                  "加载更多"
                )}
              </Button>
            )}
          </div>
        </Card>
      )}
    </div>
  );
}

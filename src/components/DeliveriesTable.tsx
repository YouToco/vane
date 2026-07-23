import { useEffect, useState } from "react";
import { ApiError } from "../api";
import type { DeliveriesResp, DeliveryHistoryItem } from "../api";
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
import { Loader2, ExternalLink } from "lucide-react";
import { fmt, useI18n, type Dict } from "@/i18n";
import { fmtBeijing } from "@/lib/time";

const PAGE_SIZE = 20;

function statusBadge(status: string): "destructive" | "secondary" | "outline" {
  if (status === "failed" || status === "pending") return "destructive";
  return "secondary";
}

type FeedbackMeta = Record<
  string,
  { label: string; variant: "default" | "secondary" | "outline" | "destructive"; showDetail: boolean }
>;

function feedbackMeta(h: Dict["app"]["history"]): FeedbackMeta {
  return {
    interested: { label: h.fbInterested, variant: "default", showDetail: false },
    not_interested: { label: h.fbNotInterested, variant: "secondary", showDetail: false },
    misjudged: { label: h.fbMisjudged, variant: "destructive", showDetail: true },
    deep_dive: { label: h.fbDeepDive, variant: "outline", showDetail: false },
    question: { label: h.fbQuestion, variant: "outline", showDetail: true },
  };
}

function clipDetail(s: string, max = 120): string {
  const runes = Array.from(s);
  return runes.length <= max ? s : runes.slice(0, max).join("") + "…";
}

// 投递记录表：推送历史页（全局）与任务详情页（单任务）共用同一渲染与三态逻辑，
// 差异只在取数函数与空态文案。加载失败与空数据是两种状态（vane-web#18 教训），
// 这里分别渲染，绝不把错误吞成空表。
// 重新加载：父组件换 key 触发整体重挂载（History 的刷新按钮就是这么做的）。
export default function DeliveriesTable({
  fetchPage,
  emptyText,
  onLoadingChange,
}: {
  fetchPage: (pageSize: number, pageToken?: string) => Promise<DeliveriesResp>;
  emptyText: string;
  onLoadingChange?: (loading: boolean) => void;
}) {
  const { t } = useI18n();
  const H = t.app.history;
  const FEEDBACK_META = feedbackMeta(H);
  const [items, setItems] = useState<DeliveryHistoryItem[]>([]);
  const [total, setTotal] = useState(0);
  const [nextToken, setNextToken] = useState("");
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [loadError, setLoadError] = useState("");

  useEffect(() => {
    let alive = true;
    onLoadingChange?.(true);
    fetchPage(PAGE_SIZE)
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
        setItems([]);
        setTotal(0);
        setNextToken("");
      })
      .finally(() => {
        if (!alive) return;
        setLoading(false);
        onLoadingChange?.(false);
      });
    return () => {
      alive = false;
    };
    // fetchPage/onLoadingChange 由父组件以稳定引用传入；重新加载走 key 重挂载。
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function loadMore() {
    if (!nextToken || loadingMore) return;
    setLoadingMore(true);
    try {
      const r = await fetchPage(PAGE_SIZE, nextToken);
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
    );
  }

  return (
    <div className="space-y-4">
      {loadError && (
        <Alert variant="destructive">
          <AlertDescription>{loadError}</AlertDescription>
        </Alert>
      )}

      {items.length === 0 ? (
        !loadError && (
          <Card>
            <CardContent className="py-12 text-center text-muted-foreground">
              {emptyText}
            </CardContent>
          </Card>
        )
      ) : (
        <Card>
          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>{H.colTime}</TableHead>
                  <TableHead>{H.colContent}</TableHead>
                  <TableHead className="text-right">{H.colScore}</TableHead>
                  <TableHead>{H.colStatus}</TableHead>
                  <TableHead>{H.colFeedback}</TableHead>
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
                          {it.title || H.noContent}
                          <ExternalLink className="size-3 shrink-0" />
                        </a>
                      ) : (
                        <span className="text-muted-foreground">
                          {it.title || H.contentDeleted}
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
              {fmt(H.shown, { shown: items.length, total })}
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
                    {t.app.common.loading}
                  </>
                ) : (
                  t.app.common.loadMore
                )}
              </Button>
            )}
          </div>
        </Card>
      )}
    </div>
  );
}

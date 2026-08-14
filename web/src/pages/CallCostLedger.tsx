import { FormEvent, useEffect, useState } from "react";
import {
  AlertTriangle,
  Bot,
  ExternalLink,
  Loader2,
  ReceiptText,
  RefreshCw,
  Wrench,
} from "lucide-react";

import {
  api,
  ApiError,
  type CallCostKind,
  type CallCostLedgerFilters,
  type CallCostLedgerItem,
  type CallCostPricingStatus,
} from "@/api";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

const PAGE_SIZE = 50;

type FilterDraft = {
  kind: CallCostKind | "all";
  provider: string;
  pricingStatus: CallCostPricingStatus | "all";
  taskID: string;
};

const EMPTY_FILTERS: FilterDraft = {
  kind: "all",
  provider: "all",
  pricingStatus: "all",
  taskID: "",
};

const STATUS_LABELS: Record<CallCostPricingStatus, string> = {
  provider_reported: "供应商实报",
  calculated: "精确计算",
  estimated: "估算",
  unpriced: "未定价",
  legacy: "历史记录",
};

const KIND_LABELS: Record<FilterDraft["kind"], string> = {
  all: "全部调用",
  llm: "模型调用",
  tool: "采集调用",
};

const PROVIDER_LABELS: Record<string, string> = {
  all: "全部供应商",
  kimi: "Kimi",
  deepseek: "DeepSeek",
  exa: "Exa",
  tikhub: "TikHub",
};

function compactNumber(value: number, maximumFractionDigits = 8): string {
  return new Intl.NumberFormat("zh-CN", {
    maximumFractionDigits,
  }).format(value);
}

export function formatCallCostAmount(item: CallCostLedgerItem): string {
  if (item.cost_amount === undefined || !item.cost_currency) return "待定价";
  const symbol = item.cost_currency === "USD" ? "$" : "¥";
  return `${symbol}${compactNumber(item.cost_amount)}`;
}

function unitPrice(value: number | undefined, currency: string): string {
  if (value === undefined) return "—";
  const symbol = currency === "USD" ? "$" : "¥";
  return `${symbol}${compactNumber(value)}`;
}

export function describeCallCostFormula(item: CallCostLedgerItem): string {
  if (item.pricing_status === "provider_reported") {
    return "金额由供应商响应直接返回，没有套用本地价格表。";
  }
  if (item.pricing_status === "unpriced") {
    return "用量已完整记录，但调用发生时没有匹配到生效价格，因此不猜测金额。";
  }
  if (item.pricing_status === "legacy") {
    return "这是价格版本账本上线前的历史金额，保留原记录，不追溯重算。";
  }
  const rule = item.pricing_rule;
  if (!rule) {
    return "本条记录没有可展示的价格版本。";
  }
  if (item.kind === "llm" && item.llm_usage) {
    const usage = item.llm_usage;
    const output = `${compactNumber(usage.completion_tokens)} 输出 × ${unitPrice(
      rule.output_per_million,
      rule.currency,
    )}/百万`;
    if (
      usage.prompt_cache_hit_tokens !== undefined &&
      usage.prompt_cache_miss_tokens !== undefined
    ) {
      return `${compactNumber(
        usage.prompt_cache_hit_tokens,
      )} 缓存命中输入 × ${unitPrice(
        rule.input_cache_hit_per_million,
        rule.currency,
      )}/百万 + ${compactNumber(
        usage.prompt_cache_miss_tokens,
      )} 缓存未命中输入 × ${unitPrice(
        rule.input_cache_miss_per_million,
        rule.currency,
      )}/百万 + ${output} = ${formatCallCostAmount(item)}`;
    }
    return `${compactNumber(
      usage.prompt_tokens,
    )} 输入全部按缓存未命中价 ${unitPrice(
      rule.input_cache_miss_per_million,
      rule.currency,
    )}/百万估算 + ${output} = ${formatCallCostAmount(item)}`;
  }
  if (item.kind === "tool" && item.tool_usage) {
    const quantity = item.tool_usage.usage_quantity;
    const included = rule.request_included_quantity ?? 0;
    const extra = Math.max(quantity - included, 0);
    return `基础请求 ${unitPrice(
      rule.request_unit_price,
      rule.currency,
    )} + ${compactNumber(extra)} 个额外计费单位 × ${unitPrice(
      rule.request_additional_unit_price,
      rule.currency,
    )} = ${formatCallCostAmount(item)}`;
  }
  return "本条记录没有可展示的计算公式。";
}

export function describeCallUsage(item: CallCostLedgerItem): string {
  if (item.kind === "llm" && item.llm_usage) {
    const usage = item.llm_usage;
    const cache =
      usage.prompt_cache_hit_tokens !== undefined &&
      usage.prompt_cache_miss_tokens !== undefined
        ? `（缓存命中 ${compactNumber(
            usage.prompt_cache_hit_tokens,
          )} / 未命中 ${compactNumber(usage.prompt_cache_miss_tokens)}）`
        : "（供应商未返回缓存拆分）";
    const reasoning =
      usage.reasoning_tokens === undefined
        ? ""
        : `（其中推理 ${compactNumber(usage.reasoning_tokens)}）`;
    return `输入 ${compactNumber(
      usage.prompt_tokens,
    )} token ${cache}；输出 ${compactNumber(
      usage.completion_tokens,
    )} token ${reasoning}`;
  }
  if (item.kind === "tool" && item.tool_usage) {
    const usage = item.tool_usage;
    const http =
      usage.http_status === undefined ? "" : `；HTTP ${usage.http_status}`;
    return `计费单位 ${compactNumber(usage.usage_quantity)}；工具 ${
      usage.tool_name || "未命名"
    }${http}`;
  }
  return "没有可展示的用量明细。";
}

function formatTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(date);
}

function usageSummary(item: CallCostLedgerItem): string {
  if (item.kind === "llm" && item.llm_usage) {
    return `${compactNumber(item.llm_usage.prompt_tokens)} 输入 · ${compactNumber(
      item.llm_usage.completion_tokens,
    )} 输出`;
  }
  if (item.kind === "tool" && item.tool_usage) {
    return `${compactNumber(item.tool_usage.usage_quantity)} 个计费单位`;
  }
  return "无用量明细";
}

function statusVariant(
  status: CallCostPricingStatus,
): "default" | "secondary" | "destructive" | "outline" {
  switch (status) {
    case "calculated":
    case "provider_reported":
      return "default";
    case "estimated":
      return "secondary";
    case "unpriced":
      return "destructive";
    default:
      return "outline";
  }
}

function toAPIFilters(draft: FilterDraft): CallCostLedgerFilters {
  return {
    ...(draft.kind === "all" ? {} : { kind: draft.kind }),
    ...(draft.provider === "all" ? {} : { provider: draft.provider }),
    ...(draft.pricingStatus === "all"
      ? {}
      : { pricing_status: draft.pricingStatus }),
    ...(draft.taskID.trim() ? { task_id: draft.taskID.trim() } : {}),
  };
}

function ReceiptDetails({ item }: { item: CallCostLedgerItem }) {
  return (
    <details className="group">
      <summary className="cursor-pointer list-none text-xs font-medium text-brand-strong hover:underline">
        查看明细
      </summary>
      <div className="mt-3 grid gap-3 rounded-lg border bg-muted/30 p-3 text-xs md:grid-cols-2">
        <div className="space-y-1">
          <p className="font-medium text-foreground">金额如何得出</p>
          <p className="leading-5 text-muted-foreground">
            {describeCallCostFormula(item)}
          </p>
          <p className="pt-1 font-medium text-foreground">用量原始记录</p>
          <p className="leading-5 text-muted-foreground">
            {describeCallUsage(item)}
          </p>
        </div>
        <div className="space-y-1">
          <p className="font-medium text-foreground">调用追踪</p>
          <p className="break-all font-mono text-muted-foreground">
            {item.trace_id || "无 trace"}
          </p>
          <p className="text-muted-foreground">
            耗时 {compactNumber(item.duration_ms, 0)} ms
            {item.failed
              ? ` · 失败${item.error_type ? `（${item.error_type}）` : ""}`
              : " · 成功"}
          </p>
        </div>
        {item.pricing_rule && (
          <div className="space-y-1 md:col-span-2">
            <p className="font-medium text-foreground">
              价格版本 #{item.pricing_rule.id}
            </p>
            <p className="text-muted-foreground">
              自 {formatTime(item.pricing_rule.effective_from)} 生效
              {item.pricing_rule.note ? ` · ${item.pricing_rule.note}` : ""}
            </p>
            <a
              className="inline-flex items-center gap-1 text-brand-strong hover:underline"
              href={item.pricing_rule.source_url}
              target="_blank"
              rel="noreferrer"
            >
              查看定价来源
              <ExternalLink className="size-3" />
            </a>
          </div>
        )}
      </div>
    </details>
  );
}

function ReceiptMobileCard({ item }: { item: CallCostLedgerItem }) {
  return (
    <Card size="sm">
      <CardHeader>
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <CardTitle className="flex items-center gap-2">
              {item.kind === "llm" ? (
                <Bot className="size-4" />
              ) : (
                <Wrench className="size-4" />
              )}
              <span className="truncate">{item.resource || "未命名资源"}</span>
            </CardTitle>
            <CardDescription>
              {item.provider || "内部调用"} · {formatTime(item.created_at)}
            </CardDescription>
          </div>
          <span className="font-mono font-semibold">
            {formatCallCostAmount(item)}
          </span>
        </div>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="flex flex-wrap items-center gap-2">
          <Badge variant={statusVariant(item.pricing_status)}>
            {STATUS_LABELS[item.pricing_status]}
          </Badge>
          {item.failed && <Badge variant="destructive">调用失败</Badge>}
          <span className="text-xs text-muted-foreground">
            {usageSummary(item)}
          </span>
        </div>
        <p className="text-sm">
          {item.task_title || item.task_id || "系统调用"}
        </p>
        <ReceiptDetails item={item} />
      </CardContent>
    </Card>
  );
}

export default function CallCostLedger() {
  const [draft, setDraft] = useState<FilterDraft>(EMPTY_FILTERS);
  const [filters, setFilters] = useState<CallCostLedgerFilters>({});
  const [items, setItems] = useState<CallCostLedgerItem[]>([]);
  const [nextPageToken, setNextPageToken] = useState("");
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [loadError, setLoadError] = useState("");
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    api
      .adminListCostCalls(filters, undefined, PAGE_SIZE)
      .then((response) => {
        if (!alive) return;
        setItems(response.items);
        setNextPageToken(response.next_page_token ?? "");
        setLoadError("");
      })
      .catch((error) => {
        if (!alive) return;
        setItems([]);
        setNextPageToken("");
        setLoadError(error instanceof ApiError ? error.message : "调用账单加载失败");
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [filters, nonce]);

  async function loadMore() {
    if (!nextPageToken || loadingMore) return;
    setLoadingMore(true);
    try {
      const response = await api.adminListCostCalls(
        filters,
        nextPageToken,
        PAGE_SIZE,
      );
      setItems((current) => [...current, ...response.items]);
      setNextPageToken(response.next_page_token ?? "");
      setLoadError("");
    } catch (error) {
      setLoadError(error instanceof ApiError ? error.message : "下一页加载失败");
    } finally {
      setLoadingMore(false);
    }
  }

  function applyFilters(event: FormEvent) {
    event.preventDefault();
    setFilters(toAPIFilters(draft));
  }

  function resetFilters() {
    setDraft(EMPTY_FILTERS);
    setFilters({});
  }

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="flex items-center gap-2 text-lg font-semibold">
            <ReceiptText className="size-5" />
            逐笔调用账单
          </h2>
          <p className="mt-1 max-w-3xl text-sm text-muted-foreground">
            每次模型、Exa 或 TikHub 调用都保留原始用量、实际金额或计算依据。价格变化后，历史账单仍指向当时使用的版本。
          </p>
        </div>
        <Button
          variant="outline"
          size="sm"
          aria-label="刷新调用账单"
          onClick={() => setNonce((value) => value + 1)}
          disabled={loading}
        >
          {loading ? (
            <Loader2 className="size-4 animate-spin" />
          ) : (
            <RefreshCw className="size-4" />
          )}
        </Button>
      </div>

      <form
        className="grid gap-3 rounded-xl border bg-card p-4 sm:grid-cols-2 lg:grid-cols-[auto_auto_auto_minmax(16rem,1fr)_auto]"
        onSubmit={applyFilters}
      >
        <Select
          value={draft.kind}
          onValueChange={(value) =>
            setDraft((current) => ({
              ...current,
              kind: value as FilterDraft["kind"],
            }))
          }
        >
          <SelectTrigger aria-label="调用类型" className="w-full lg:w-32">
            <SelectValue>{KIND_LABELS[draft.kind]}</SelectValue>
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">全部调用</SelectItem>
            <SelectItem value="llm">模型调用</SelectItem>
            <SelectItem value="tool">采集调用</SelectItem>
          </SelectContent>
        </Select>
        <Select
          value={draft.provider}
          onValueChange={(value) =>
            setDraft((current) => ({ ...current, provider: value ?? "all" }))
          }
        >
          <SelectTrigger aria-label="供应商" className="w-full lg:w-32">
            <SelectValue>
              {PROVIDER_LABELS[draft.provider] ?? draft.provider}
            </SelectValue>
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">全部供应商</SelectItem>
            <SelectItem value="kimi">Kimi</SelectItem>
            <SelectItem value="deepseek">DeepSeek</SelectItem>
            <SelectItem value="exa">Exa</SelectItem>
            <SelectItem value="tikhub">TikHub</SelectItem>
          </SelectContent>
        </Select>
        <Select
          value={draft.pricingStatus}
          onValueChange={(value) =>
            setDraft((current) => ({
              ...current,
              pricingStatus: value as FilterDraft["pricingStatus"],
            }))
          }
        >
          <SelectTrigger aria-label="计价状态" className="w-full lg:w-36">
            <SelectValue>
              {draft.pricingStatus === "all"
                ? "全部计价状态"
                : STATUS_LABELS[draft.pricingStatus]}
            </SelectValue>
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">全部计价状态</SelectItem>
            <SelectItem value="provider_reported">供应商实报</SelectItem>
            <SelectItem value="calculated">精确计算</SelectItem>
            <SelectItem value="estimated">估算</SelectItem>
            <SelectItem value="unpriced">未定价</SelectItem>
            <SelectItem value="legacy">历史记录</SelectItem>
          </SelectContent>
        </Select>
        <Input
          aria-label="任务 ID"
          placeholder="按任务 ID 精确筛选"
          value={draft.taskID}
          onChange={(event) =>
            setDraft((current) => ({ ...current, taskID: event.target.value }))
          }
        />
        <div className="flex gap-2">
          <Button type="submit" className="flex-1">
            筛选
          </Button>
          <Button type="button" variant="outline" onClick={resetFilters}>
            重置
          </Button>
        </div>
      </form>

      {loadError && (
        <Alert variant="destructive">
          <AlertTriangle className="size-4" />
          <AlertDescription>{loadError}</AlertDescription>
        </Alert>
      )}

      {loading ? (
        <div className="space-y-3">
          {Array.from({ length: 5 }).map((_, index) => (
            <Skeleton key={index} className="h-16 w-full rounded-xl" />
          ))}
        </div>
      ) : items.length === 0 ? (
        <Card>
          <CardContent className="py-10 text-center">
            <ReceiptText className="mx-auto mb-3 size-8 text-muted-foreground" />
            <p className="font-medium">没有符合条件的调用</p>
            <p className="mt-1 text-sm text-muted-foreground">
              新调用会自动出现在这里；未定价调用也会保留完整用量。
            </p>
          </CardContent>
        </Card>
      ) : (
        <>
          <div className="space-y-3 md:hidden">
            {items.map((item) => (
              <ReceiptMobileCard key={`${item.kind}:${item.id}`} item={item} />
            ))}
          </div>
          <Card className="hidden md:flex">
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>时间 / 类型</TableHead>
                    <TableHead>供应商 / 资源</TableHead>
                    <TableHead>任务</TableHead>
                    <TableHead>用量</TableHead>
                    <TableHead>计价</TableHead>
                    <TableHead className="text-right">金额</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {items.map((item) => (
                    <TableRow key={`${item.kind}:${item.id}`}>
                      <TableCell className="align-top">
                        <div className="flex items-center gap-1.5 font-medium">
                          {item.kind === "llm" ? (
                            <Bot className="size-4" />
                          ) : (
                            <Wrench className="size-4" />
                          )}
                          {item.kind === "llm" ? "模型" : "采集"}
                        </div>
                        <p className="mt-1 whitespace-nowrap text-xs text-muted-foreground">
                          {formatTime(item.created_at)}
                        </p>
                      </TableCell>
                      <TableCell className="max-w-56 align-top">
                        <p className="font-medium">{item.provider || "内部"}</p>
                        <p className="truncate font-mono text-xs text-muted-foreground">
                          {item.resource || "未命名资源"}
                        </p>
                        {item.failed && (
                          <Badge variant="destructive" className="mt-2">
                            调用失败
                          </Badge>
                        )}
                      </TableCell>
                      <TableCell className="max-w-56 align-top">
                        <p className="truncate">
                          {item.task_title || item.task_id || "系统调用"}
                        </p>
                        {item.task_title && item.task_id && (
                          <p className="truncate font-mono text-xs text-muted-foreground">
                            {item.task_id}
                          </p>
                        )}
                      </TableCell>
                      <TableCell className="whitespace-nowrap align-top">
                        {usageSummary(item)}
                      </TableCell>
                      <TableCell className="min-w-40 align-top">
                        <Badge variant={statusVariant(item.pricing_status)}>
                          {STATUS_LABELS[item.pricing_status]}
                        </Badge>
                        <div className="mt-2">
                          <ReceiptDetails item={item} />
                        </div>
                      </TableCell>
                      <TableCell className="whitespace-nowrap text-right align-top font-mono font-semibold">
                        {formatCallCostAmount(item)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </Card>
        </>
      )}

      {nextPageToken && !loading && (
        <div className="flex justify-center">
          <Button
            variant="outline"
            onClick={loadMore}
            disabled={loadingMore}
          >
            {loadingMore && <Loader2 className="size-4 animate-spin" />}
            加载更多
          </Button>
        </div>
      )}
    </div>
  );
}

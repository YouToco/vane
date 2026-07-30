import { FormEvent, useEffect, useMemo, useRef, useState } from "react";
import { toast } from "sonner";
import {
  BadgeDollarSign,
  Calculator,
  ExternalLink,
  History,
  Loader2,
  PencilLine,
  RefreshCw,
  Save,
} from "lucide-react";

import { api, ApiError } from "@/api";
import type {
  ProviderPriceCurrency,
  ProviderPriceMeter,
  ProviderPriceRule,
  ReplaceProviderPriceRule,
} from "@/api";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

interface PriceDraft {
  provider: string;
  resource: string;
  meter: ProviderPriceMeter;
  currency: ProviderPriceCurrency;
  hit: string;
  miss: string;
  output: string;
  request: string;
  included: string;
  additional: string;
  sourceURL: string;
  note: string;
}

const emptyDraft: PriceDraft = {
  provider: "kimi",
  resource: "kimi-k2.6",
  meter: "llm_tokens",
  currency: "USD",
  hit: "",
  miss: "",
  output: "",
  request: "",
  included: "1",
  additional: "",
  sourceURL: "",
  note: "",
};

function numberText(value: number | undefined): string {
  return value === undefined ? "" : String(value);
}

function ruleToDraft(rule: ProviderPriceRule): PriceDraft {
  return {
    provider: rule.provider,
    resource: rule.resource,
    meter: rule.meter,
    currency: rule.currency,
    hit: numberText(rule.input_cache_hit_per_million),
    miss: numberText(rule.input_cache_miss_per_million),
    output: numberText(rule.output_per_million),
    request: numberText(rule.request_unit_price),
    included: numberText(rule.request_included_quantity),
    additional: numberText(rule.request_additional_unit_price),
    sourceURL: rule.source_url,
    note: rule.note,
  };
}

function formatPrice(value: number | undefined, currency: string): string {
  if (value === undefined) return "—";
  const symbol = currency === "CNY" ? "¥" : "$";
  return `${symbol}${value.toLocaleString("zh-CN", { maximumFractionDigits: 8 })}`;
}

function formatTime(value: string): string {
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime())
    ? value
    : parsed.toLocaleString("zh-CN", { hour12: false });
}

function formula(rule: ProviderPriceRule): string {
  if (rule.meter === "request") {
    return `${formatPrice(rule.request_unit_price, rule.currency)} 含 ${
      rule.request_included_quantity ?? 1
    } 单位 · 超出每单位 ${formatPrice(
      rule.request_additional_unit_price,
      rule.currency,
    )}`;
  }
  return [
    `命中 ${formatPrice(rule.input_cache_hit_per_million, rule.currency)}`,
    `未命中 ${formatPrice(rule.input_cache_miss_per_million, rule.currency)}`,
    `输出 ${formatPrice(rule.output_per_million, rule.currency)}`,
  ].join(" · ");
}

function nonnegative(value: string): number | undefined {
  if (value.trim() === "") return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : undefined;
}

export default function Pricing() {
  const [rules, setRules] = useState<ProviderPriceRule[]>([]);
  const [draft, setDraft] = useState<PriceDraft>(emptyDraft);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [loadError, setLoadError] = useState("");
  const [nonce, setNonce] = useState(0);
  const submitIntent = useRef<{ signature: string; key: string } | null>(null);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    api
      .adminListProviderPrices()
      .then((items) => {
        if (!alive) return;
        setRules(items);
        setLoadError("");
      })
      .catch((error) => {
        if (!alive) return;
        setLoadError(
          error instanceof ApiError ? error.message : "价格目录加载失败",
        );
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [nonce]);

  const now = Date.now();
  const active = useMemo(
    () =>
      rules.filter((rule) => {
        const from = new Date(rule.effective_from).getTime();
        const to = rule.effective_to
          ? new Date(rule.effective_to).getTime()
          : Number.POSITIVE_INFINITY;
        return from <= now && now < to;
      }),
    [now, rules],
  );
  const upcoming = useMemo(
    () => rules.filter((rule) => new Date(rule.effective_from).getTime() > now),
    [now, rules],
  );
  const history = useMemo(
    () =>
      rules.filter(
        (rule) =>
          Boolean(rule.effective_to) &&
          new Date(rule.effective_to!).getTime() <= now,
      ),
    [now, rules],
  );

  function editRule(rule: ProviderPriceRule) {
    setDraft(ruleToDraft(rule));
    const editor = document.getElementById("provider-price-editor");
    if (typeof editor?.scrollIntoView === "function") {
      editor.scrollIntoView({ behavior: "smooth", block: "start" });
    }
    requestAnimationFrame(() => {
      document.getElementById("price-provider")?.focus();
    });
  }

  async function submit(event: FormEvent) {
    event.preventDefault();
    const hit = nonnegative(draft.hit);
    const miss = nonnegative(draft.miss);
    const output = nonnegative(draft.output);
    const request = nonnegative(draft.request);
    const included = nonnegative(draft.included);
    const additional = nonnegative(draft.additional);
    if (
      !draft.provider.trim() ||
      !draft.resource.trim() ||
      !draft.sourceURL.trim() ||
      (draft.meter === "llm_tokens" &&
        (hit === undefined || miss === undefined || output === undefined)) ||
      (draft.meter === "request" &&
        (request === undefined ||
          included === undefined ||
          additional === undefined))
    ) {
      toast.error("请完整填写非负单价、计价资源和官方来源。");
      return;
    }
    const input: ReplaceProviderPriceRule = {
      provider: draft.provider.trim(),
      resource: draft.resource.trim(),
      meter: draft.meter,
      currency: draft.currency,
      source_url: draft.sourceURL.trim(),
      note: draft.note.trim(),
      ...(draft.meter === "llm_tokens"
        ? {
            input_cache_hit_per_million: hit,
            input_cache_miss_per_million: miss,
            output_per_million: output,
          }
        : {
            request_unit_price: request,
            request_included_quantity: included,
            request_additional_unit_price: additional,
          }),
    };
    const signature = JSON.stringify(input);
    const idempotencyKey =
      submitIntent.current?.signature === signature
        ? submitIntent.current.key
        : crypto.randomUUID();
    submitIntent.current = { signature, key: idempotencyKey };
    setSaving(true);
    try {
      await api.adminReplaceProviderPrice(input, idempotencyKey);
      submitIntent.current = null;
      toast.success("新价格版本已生效，历史调用不会被重算。");
      setNonce((value) => value + 1);
    } catch (error) {
      toast.error(error instanceof ApiError ? error.message : "保存价格失败");
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between gap-4">
        <div className="space-y-1">
          <p className="text-sm text-muted-foreground">
            每次调用先保存真实 token
            或请求次数，再绑定当时生效的价格版本。这里改价只影响后续调用。
          </p>
          <p className="text-xs text-muted-foreground">
            供应商直接返回金额时，以供应商回执为准；通配价格和缺少缓存明细的模型费用会标为估算。
          </p>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={() => setNonce((value) => value + 1)}
          disabled={loading}
          aria-label="刷新价格目录"
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

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-base">
            <BadgeDollarSign className="size-4" />
            当前生效价格
          </CardTitle>
        </CardHeader>
        <CardContent>
          {active.length === 0 ? (
            <p className="py-6 text-center text-sm text-muted-foreground">
              暂无生效价格。未定价调用仍会记录用量，但不会猜测金额。
            </p>
          ) : (
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>供应商 / 资源</TableHead>
                    <TableHead>计价</TableHead>
                    <TableHead>单价</TableHead>
                    <TableHead>生效时间</TableHead>
                    <TableHead>来源</TableHead>
                    <TableHead className="text-right">操作</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {active.map((rule) => (
                    <TableRow key={rule.id}>
                      <TableCell>
                        <div className="font-medium">{rule.provider}</div>
                        <div className="font-mono text-xs text-muted-foreground">
                          {rule.resource}
                        </div>
                      </TableCell>
                      <TableCell>
                        <Badge variant="outline">
                          {rule.meter === "llm_tokens"
                            ? "Token / 百万"
                            : "按次"}
                        </Badge>
                      </TableCell>
                      <TableCell className="max-w-md text-xs">
                        {formula(rule)}
                      </TableCell>
                      <TableCell className="whitespace-nowrap text-xs">
                        {formatTime(rule.effective_from)}
                      </TableCell>
                      <TableCell>
                        <a
                          href={rule.source_url}
                          target="_blank"
                          rel="noreferrer"
                          className="inline-flex items-center gap-1 text-xs text-brand-strong hover:underline"
                        >
                          官方文档
                          <ExternalLink className="size-3" />
                        </a>
                      </TableCell>
                      <TableCell className="text-right">
                        <Button
                          type="button"
                          variant="ghost"
                          size="sm"
                          onClick={() => editRule(rule)}
                        >
                          <PencilLine className="mr-1 size-3.5" />
                          更新
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      {upcoming.length > 0 && (
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="flex items-center gap-2 text-base">
              <History className="size-4" />
              待生效价格
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>供应商 / 资源</TableHead>
                    <TableHead>单价</TableHead>
                    <TableHead>生效时间</TableHead>
                    <TableHead>来源</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {upcoming.map((rule) => (
                    <TableRow key={rule.id}>
                      <TableCell>
                        <div className="font-medium">{rule.provider}</div>
                        <div className="font-mono text-xs text-muted-foreground">
                          {rule.resource}
                        </div>
                      </TableCell>
                      <TableCell className="text-xs">{formula(rule)}</TableCell>
                      <TableCell className="whitespace-nowrap text-xs">
                        {formatTime(rule.effective_from)}
                      </TableCell>
                      <TableCell>
                        <a
                          href={rule.source_url}
                          target="_blank"
                          rel="noreferrer"
                          className="inline-flex items-center gap-1 text-xs text-brand-strong hover:underline"
                        >
                          官方文档
                          <ExternalLink className="size-3" />
                        </a>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </CardContent>
        </Card>
      )}

      <Card id="provider-price-editor">
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-base">
            <Calculator className="size-4" />
            新建价格版本
          </CardTitle>
        </CardHeader>
        <CardContent>
          <form className="space-y-5" onSubmit={submit}>
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
              <div className="space-y-2">
                <Label htmlFor="price-provider">供应商</Label>
                <Input
                  id="price-provider"
                  value={draft.provider}
                  onChange={(event) =>
                    setDraft((value) => ({
                      ...value,
                      provider: event.target.value,
                    }))
                  }
                  placeholder="kimi / deepseek / exa / tikhub"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="price-resource">模型或端点</Label>
                <Input
                  id="price-resource"
                  value={draft.resource}
                  onChange={(event) =>
                    setDraft((value) => ({
                      ...value,
                      resource: event.target.value,
                    }))
                  }
                  placeholder="kimi-k2.6 / /search / *"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="price-meter">计价方式</Label>
                <Select
                  value={draft.meter}
                  onValueChange={(meter) =>
                    setDraft((value) => ({
                      ...value,
                      meter: meter as ProviderPriceMeter,
                    }))
                  }
                >
                  <SelectTrigger id="price-meter" className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="llm_tokens">Token / 百万</SelectItem>
                    <SelectItem value="request">按请求次数</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label htmlFor="price-currency">币种</Label>
                <Select
                  value={draft.currency}
                  onValueChange={(currency) =>
                    setDraft((value) => ({
                      ...value,
                      currency: currency as ProviderPriceCurrency,
                    }))
                  }
                >
                  <SelectTrigger id="price-currency" className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="USD">USD 美元</SelectItem>
                    <SelectItem value="CNY">CNY 人民币</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            {draft.meter === "llm_tokens" ? (
              <div className="grid gap-4 md:grid-cols-3">
                {[
                  ["hit", "输入缓存命中 / 百万 token"],
                  ["miss", "输入缓存未命中 / 百万 token"],
                  ["output", "输出 / 百万 token"],
                ].map(([key, label]) => (
                  <div className="space-y-2" key={key}>
                    <Label htmlFor={`price-${key}`}>{label}</Label>
                    <Input
                      id={`price-${key}`}
                      type="number"
                      min="0"
                      step="any"
                      value={draft[key as "hit" | "miss" | "output"]}
                      onChange={(event) =>
                        setDraft((value) => ({
                          ...value,
                          [key]: event.target.value,
                        }))
                      }
                    />
                  </div>
                ))}
              </div>
            ) : (
              <div className="grid gap-4 md:grid-cols-3">
                <div className="space-y-2">
                  <Label htmlFor="price-request">基础请求价格</Label>
                  <Input
                    id="price-request"
                    type="number"
                    min="0"
                    step="any"
                    value={draft.request}
                    onChange={(event) =>
                      setDraft((value) => ({
                        ...value,
                        request: event.target.value,
                      }))
                    }
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="price-included">基础价包含单位数</Label>
                  <Input
                    id="price-included"
                    type="number"
                    min="0"
                    step="any"
                    value={draft.included}
                    onChange={(event) =>
                      setDraft((value) => ({
                        ...value,
                        included: event.target.value,
                      }))
                    }
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="price-additional">每个额外单位价格</Label>
                  <Input
                    id="price-additional"
                    type="number"
                    min="0"
                    step="any"
                    value={draft.additional}
                    onChange={(event) =>
                      setDraft((value) => ({
                        ...value,
                        additional: event.target.value,
                      }))
                    }
                  />
                </div>
              </div>
            )}

            <div className="grid gap-4 md:grid-cols-2">
              <div className="space-y-2">
                <Label htmlFor="price-source">官方价格来源</Label>
                <Input
                  id="price-source"
                  type="url"
                  value={draft.sourceURL}
                  onChange={(event) =>
                    setDraft((value) => ({
                      ...value,
                      sourceURL: event.target.value,
                    }))
                  }
                  placeholder="https://..."
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="price-note">备注</Label>
                <Input
                  id="price-note"
                  value={draft.note}
                  onChange={(event) =>
                    setDraft((value) => ({
                      ...value,
                      note: event.target.value,
                    }))
                  }
                  placeholder="例如：2026-07 官方公开价"
                />
              </div>
            </div>

            <div className="flex flex-wrap items-center justify-between gap-3 border-t pt-4">
              <p className="text-xs text-muted-foreground">
                保存会关闭同一供应商、资源和计价方式的旧版本；已发生调用的金额保持不变。
              </p>
              <Button type="submit" disabled={saving}>
                {saving ? (
                  <Loader2 className="mr-1 size-4 animate-spin" />
                ) : (
                  <Save className="mr-1 size-4" />
                )}
                保存并生效
              </Button>
            </div>
          </form>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-base">
            <History className="size-4" />
            历史价格版本
          </CardTitle>
        </CardHeader>
        <CardContent>
          {history.length === 0 ? (
            <p className="py-6 text-center text-sm text-muted-foreground">
              还没有被替换的历史价格。
            </p>
          ) : (
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>供应商 / 资源</TableHead>
                    <TableHead>旧单价</TableHead>
                    <TableHead>有效区间</TableHead>
                    <TableHead>备注</TableHead>
                    <TableHead>来源</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {history.map((rule) => (
                    <TableRow key={rule.id}>
                      <TableCell>
                        <div>{rule.provider}</div>
                        <div className="font-mono text-xs text-muted-foreground">
                          {rule.resource}
                        </div>
                      </TableCell>
                      <TableCell className="text-xs">{formula(rule)}</TableCell>
                      <TableCell className="whitespace-nowrap text-xs">
                        {formatTime(rule.effective_from)}
                        <br />至 {formatTime(rule.effective_to!)}
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground">
                        {rule.note || "—"}
                      </TableCell>
                      <TableCell>
                        <a
                          href={rule.source_url}
                          target="_blank"
                          rel="noreferrer"
                          className="inline-flex items-center gap-1 text-xs text-brand-strong hover:underline"
                        >
                          官方文档
                          <ExternalLink className="size-3" />
                        </a>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

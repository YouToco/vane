import { useEffect, useState } from "react";
import type { KeyboardEvent } from "react";
import { api, ApiError } from "../api";
import type { Source } from "../api";
import { toast } from "sonner";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Select,
  SelectTrigger,
  SelectValue,
  SelectContent,
  SelectItem,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Plus,
  Trash2,
  Loader2,
  Rss,
  Search,
  BookOpen,
  RotateCcw,
  ScanSearch,
  Boxes,
} from "lucide-react";
import { fmt, useI18n, type Dict } from "@/i18n";
import { fmtBeijing } from "@/lib/time";
import {
  buildSubscriptionRequest,
  normalizeCategory,
  safeHttpHref,
  sourceView,
  type SourceFormKind,
} from "@/lib/source-view";

const MAX_PARAM_RUNES = 256;
const NONE_CATEGORY = "__none__";

function runeCount(s: string): number {
  return [...s].length;
}

function statusDot(
  status: string,
  failCount: number,
  t: Dict["app"]["sources"],
): { color: string; label: string } {
  if (status === "active") {
    return failCount > 0
      ? {
          color: "bg-amber-500",
          label: fmt(t.statusFetchFailed, { n: failCount }),
        }
      : { color: "bg-emerald-500", label: t.statusActive };
  }
  if (status === "disabled") {
    return { color: "bg-red-500", label: t.statusDisabled };
  }
  if (status === "paused") {
    return { color: "bg-muted-foreground/40", label: t.statusPaused };
  }
  return { color: "bg-muted-foreground/40", label: status };
}

function isSubmitEnter(e: KeyboardEvent<HTMLInputElement>): boolean {
  return e.key === "Enter" && !e.nativeEvent.isComposing && e.nativeEvent.keyCode !== 229;
}

const CATEGORY_KEYS = [
  ["", "categoryAny"],
  ["company", "categoryCompany"],
  ["research paper", "categoryResearch"],
  ["news", "categoryNews"],
  ["github", "categoryGithub"],
  ["personal site", "categoryPersonal"],
  ["people", "categoryPeople"],
  ["financial report", "categoryFinancial"],
] as const;

function categoryLabel(value: string, t: Dict["app"]["sources"]): string {
  const hit = CATEGORY_KEYS.find(([candidate]) => candidate === value);
  return hit ? t[hit[1]] : value;
}

function sourceMeta(source: Source, t: Dict["app"]["sources"]) {
  const view = sourceView(source);
  switch (view.kind) {
    case "rss":
      return { badge: t.typeRss, term: view.term, icon: Rss };
    case "webSearch":
      return {
        badge: view.category
          ? fmt(t.typeExaCategory, { category: categoryLabel(view.category, t) })
          : t.typeExa,
        term: view.term,
        icon: Search,
      };
    case "xhsSearch":
      return { badge: t.typeXhs, term: view.term, icon: BookOpen };
    case "webContents":
      return { badge: t.typeWebContents, term: view.term, icon: ScanSearch };
    case "platformCapability":
      return {
        badge: fmt(t.typePlatformCapability, { type: view.platformCapability }),
        term: view.term,
        icon: Boxes,
      };
  }
}

export default function Sources() {
  const { t } = useI18n();
  const T = t.app.sources;
  const [sources, setSources] = useState<Source[] | null>(null);
  const [loadError, setLoadError] = useState("");
  const [srcType, setSrcType] = useState<SourceFormKind>("rss");
  const [url, setUrl] = useState("");
  const [query, setQuery] = useState("");
  const [category, setCategory] = useState("");
  const [keyword, setKeyword] = useState("");
  const [adding, setAdding] = useState(false);
  const [removingId, setRemovingId] = useState<number | null>(null);
  const [enablingId, setEnablingId] = useState<number | null>(null);

  async function load() {
    try {
      const list = await api.listSubscriptions();
      setSources(list ?? []);
      setLoadError("");
    } catch (err) {
      setLoadError(err instanceof ApiError ? err.message : T.loadFailed);
      setSources([]);
    }
  }

  useEffect(() => {
    void load();
    // The API endpoint is stable; locale changes should update labels without
    // refetching account data.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function validate(): { ok: boolean; warn: string } {
    if (srcType === "rss" || srcType === "web_contents") {
      if (url.trim() === "") return { ok: false, warn: "" };
      return safeHttpHref(url)
        ? { ok: true, warn: "" }
        : { ok: false, warn: T.invalidUrl };
    }
    const term = (srcType === "exa" ? query : keyword).trim();
    if (term === "") return { ok: false, warn: "" };
    if (runeCount(term) > MAX_PARAM_RUNES) {
      return {
        ok: false,
        warn: fmt(T.termTooLong, { n: MAX_PARAM_RUNES }),
      };
    }
    return { ok: true, warn: "" };
  }

  const valid = validate();
  const canAdd = valid.ok && !adding;

  async function onAdd() {
    if (!canAdd) return;
    const req = buildSubscriptionRequest(srcType, {
      url,
      query,
      category,
      keyword,
    });
    setAdding(true);
    try {
      await api.addSubscription(req);
      if (srcType === "rss" || srcType === "web_contents") setUrl("");
      else if (srcType === "exa") setQuery("");
      else setKeyword("");
      await load();
      toast.success(T.added);
    } catch (err) {
      toast.error(err instanceof ApiError ? err.message : T.addFailed);
    } finally {
      setAdding(false);
    }
  }

  async function onRemove(id: number) {
    setRemovingId(id);
    try {
      await api.removeSubscription(id);
      await load();
      toast.success(T.removed);
    } catch (err) {
      toast.error(err instanceof ApiError ? err.message : T.removeFailed);
    } finally {
      setRemovingId(null);
    }
  }

  async function onEnable(id: number) {
    setEnablingId(id);
    try {
      await api.enableSource(id);
      await load();
      toast.success(T.enabled);
    } catch (err) {
      toast.error(err instanceof ApiError ? err.message : T.enableFailed);
    } finally {
      setEnablingId(null);
    }
  }

  const urlPlaceholder =
    srcType === "web_contents" ? T.webContentsPlaceholder : T.rssPlaceholder;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-semibold">{T.title}</h1>
        <p className="mt-1 text-sm text-muted-foreground">{T.desc}</p>
      </div>

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">{T.addTitle}</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <Tabs value={srcType} onValueChange={(v) => setSrcType(v as SourceFormKind)}>
            <TabsList className="h-auto flex-wrap">
              <TabsTrigger value="rss">
                <Rss className="size-4 mr-1.5" />
                {T.tabRss}
              </TabsTrigger>
              <TabsTrigger value="exa">
                <Search className="size-4 mr-1.5" />
                {T.tabExa}
              </TabsTrigger>
              <TabsTrigger value="tikhub_xhs">
                <BookOpen className="size-4 mr-1.5" />
                {T.tabXhs}
              </TabsTrigger>
              <TabsTrigger value="web_contents">
                <ScanSearch className="size-4 mr-1.5" />
                {T.tabWebContents}
              </TabsTrigger>
            </TabsList>
          </Tabs>

          <div className="flex flex-col gap-2 sm:flex-row">
            {(srcType === "rss" || srcType === "web_contents") && (
              <Input
                className="flex-1"
                placeholder={urlPlaceholder}
                value={url}
                onChange={(e) => setUrl(e.target.value)}
                onKeyDown={(e) => {
                  if (isSubmitEnter(e)) void onAdd();
                }}
                autoComplete="off"
              />
            )}
            {srcType === "exa" && (
              <>
                <Input
                  className="flex-1"
                  placeholder={T.exaPlaceholder}
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  onKeyDown={(e) => {
                    if (isSubmitEnter(e)) void onAdd();
                  }}
                  autoComplete="off"
                />
                <Select
                  value={category || NONE_CATEGORY}
                  onValueChange={(v) => setCategory(normalizeCategory(v))}
                >
                  <SelectTrigger className="w-full sm:w-40">
                    <SelectValue placeholder={T.categoryAny} />
                  </SelectTrigger>
                  <SelectContent>
                    {CATEGORY_KEYS.map(([value, key]) => (
                      <SelectItem key={value || NONE_CATEGORY} value={value || NONE_CATEGORY}>
                        {T[key]}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </>
            )}
            {srcType === "tikhub_xhs" && (
              <Input
                className="flex-1"
                placeholder={T.xhsPlaceholder}
                value={keyword}
                onChange={(e) => setKeyword(e.target.value)}
                onKeyDown={(e) => {
                  if (isSubmitEnter(e)) void onAdd();
                }}
                autoComplete="off"
              />
            )}
            <Button onClick={() => void onAdd()} disabled={!canAdd}>
              {adding ? (
                <Loader2 className="size-4 animate-spin" />
              ) : (
                <>
                  <Plus className="size-4 mr-1" />
                  {T.add}
                </>
              )}
            </Button>
          </div>
          {srcType === "web_contents" && (
            <p className="text-xs text-muted-foreground">{T.webContentsNote}</p>
          )}
          {valid.warn && (
            <p className="text-sm text-amber-600 dark:text-amber-400">{valid.warn}</p>
          )}
        </CardContent>
      </Card>

      {loadError && (
        <Alert variant="destructive">
          <AlertDescription>{loadError}</AlertDescription>
        </Alert>
      )}
      {sources === null && !loadError && (
        <div className="space-y-3">
          {Array.from({ length: 3 }).map((_, i) => (
            <Card key={i}>
              <CardContent className="py-4 flex gap-3">
                <Skeleton className="size-3 rounded-full mt-1" />
                <div className="flex-1 space-y-2">
                  <Skeleton className="h-4 w-48" />
                  <Skeleton className="h-3 w-64" />
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}
      {sources !== null && sources.length === 0 && !loadError && (
        <Card>
          <CardContent className="py-12 text-center text-muted-foreground">
            {T.empty}
          </CardContent>
        </Card>
      )}
      <div className="space-y-2">
        {sources?.map((source) => {
          const dot = statusDot(source.status, source.fail_count, T);
          const meta = sourceMeta(source, T);
          const href = safeHttpHref(meta.term);
          return (
            <Card key={source.id} className="transition-colors hover:bg-muted/30">
              <CardContent className="py-3 flex items-center gap-3">
                <span
                  className={`flex size-2.5 shrink-0 rounded-full ${dot.color}`}
                  title={dot.label}
                  aria-label={dot.label}
                />
                <meta.icon className="size-4 shrink-0 text-muted-foreground" />
                <div className="flex-1 min-w-0">
                  <div className="text-sm font-medium truncate">
                    {source.title || meta.term}
                  </div>
                  <div className="flex flex-wrap items-center gap-2 mt-0.5">
                    <Badge variant="outline" className="text-xs">
                      {meta.badge}
                    </Badge>
                    {href ? (
                      <a
                        className="text-xs text-muted-foreground hover:text-primary truncate"
                        href={href}
                        target="_blank"
                        rel="noreferrer"
                      >
                        {meta.term}
                      </a>
                    ) : (
                      <span className="text-xs text-muted-foreground truncate">
                        {meta.term}
                      </span>
                    )}
                    {source.last_fetched_at && (
                      <span className="text-xs text-muted-foreground">
                        {T.lastFetched} {fmtBeijing(source.last_fetched_at)}
                      </span>
                    )}
                  </div>
                </div>
                {source.status === "disabled" && (
                  <Button
                    variant="outline"
                    size="sm"
                    className="shrink-0"
                    onClick={() => void onEnable(source.id)}
                    disabled={enablingId === source.id}
                    title={T.enableTitle}
                  >
                    {enablingId === source.id ? (
                      <Loader2 className="size-4 animate-spin" />
                    ) : (
                      <>
                        <RotateCcw className="size-3.5 mr-1" />
                        {T.enable}
                      </>
                    )}
                  </Button>
                )}
                <Button
                  variant="ghost"
                  size="icon"
                  className="shrink-0 text-muted-foreground hover:text-destructive"
                  onClick={() => void onRemove(source.id)}
                  disabled={removingId === source.id}
                  title={T.remove}
                  aria-label={T.remove}
                >
                  {removingId === source.id ? (
                    <Loader2 className="size-4 animate-spin" />
                  ) : (
                    <Trash2 className="size-4" />
                  )}
                </Button>
              </CardContent>
            </Card>
          );
        })}
      </div>
    </div>
  );
}

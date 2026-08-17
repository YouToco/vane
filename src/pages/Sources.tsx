import { useEffect, useRef, useState } from "react";
import type { KeyboardEvent } from "react";
import { api, ApiError } from "../api";
import type { AddSubscriptionReq, Source } from "../api";
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
import { Plus, Trash2, Loader2, Rss, Search, BookOpen } from "lucide-react";

type SrcType = "rss" | "exa" | "tikhub_xhs";

const MAX_PARAM_RUNES = 256;

function runeCount(s: string): number {
  return [...s].length;
}

function statusDot(status: string, failCount: number): { color: string; label: string } {
  if (status === "active") {
    return failCount > 0
      ? { color: "bg-amber-500", label: `抓取异常（连续失败 ${failCount} 次）` }
      : { color: "bg-emerald-500", label: "正常抓取中" };
  }
  if (status === "disabled") return { color: "bg-red-500", label: "已禁用（多次抓取失败）" };
  if (status === "paused") return { color: "bg-muted-foreground/40", label: "已暂停" };
  return { color: "bg-muted-foreground/40", label: status };
}

function looksLikeUrl(s: string): boolean {
  const t = s.trim();
  return /^https?:\/\/.+\..+/i.test(t);
}

function isSubmitEnter(e: KeyboardEvent<HTMLInputElement>): boolean {
  return e.key === "Enter" && !e.nativeEvent.isComposing && e.nativeEvent.keyCode !== 229;
}

function syntheticParam(url: string, param: string): string | null {
  const qs = url.split("?")[1] ?? "";
  return new URLSearchParams(qs).get(param);
}

function typeMeta(s: Source): { badge: string; term: string; icon: typeof Rss } | null {
  if (s.type === "exa") {
    const cat = syntheticParam(s.url, "category");
    return {
      badge: cat ? `Exa 搜索 · ${categoryLabel(cat)}` : "Exa 搜索",
      term: syntheticParam(s.url, "q") ?? s.url,
      icon: Search,
    };
  }
  if (s.type === "tikhub_xhs") {
    return { badge: "小红书", term: syntheticParam(s.url, "keyword") ?? s.url, icon: BookOpen };
  }
  return null;
}

const EXA_CATEGORIES: [string, string][] = [
  ["", "类别不限"],
  ["company", "公司"],
  ["research paper", "研究论文"],
  ["news", "新闻"],
  ["github", "GitHub"],
  ["personal site", "个人网站"],
  ["people", "人物"],
  ["financial report", "财报"],
];

function categoryLabel(v: string): string {
  const hit = EXA_CATEGORIES.find(([value]) => value === v);
  return hit ? hit[1] : v;
}

export default function Sources() {
  const [sources, setSources] = useState<Source[] | null>(null);
  const [loadError, setLoadError] = useState("");
  const [srcType, setSrcType] = useState<SrcType>("rss");
  const [url, setUrl] = useState("");
  const [query, setQuery] = useState("");
  const [category, setCategory] = useState("");
  const [keyword, setKeyword] = useState("");
  const [adding, setAdding] = useState(false);
  const [removingId, setRemovingId] = useState<number | null>(null);

  // keep ref for cleanup parity with original, though sonner handles its own timers
  const _toastCleanup = useRef<undefined>(undefined);
  void _toastCleanup;

  async function load() {
    try {
      const list = await api.listSubscriptions();
      setSources(list ?? []);
      setLoadError("");
    } catch (err) {
      setLoadError(err instanceof ApiError ? err.message : "加载失败");
      setSources([]);
    }
  }

  useEffect(() => {
    load();
  }, []);

  function validate(): { ok: boolean; warn: string } {
    if (srcType === "rss") {
      if (url.trim() === "") return { ok: false, warn: "" };
      return looksLikeUrl(url)
        ? { ok: true, warn: "" }
        : { ok: false, warn: "请输入以 http(s):// 开头的完整链接" };
    }
    const term = (srcType === "exa" ? query : keyword).trim();
    if (term === "") return { ok: false, warn: "" };
    if (runeCount(term) > MAX_PARAM_RUNES) {
      return { ok: false, warn: `搜索词过长（上限 ${MAX_PARAM_RUNES} 字符）` };
    }
    return { ok: true, warn: "" };
  }
  const valid = validate();
  const canAdd = valid.ok && !adding;

  async function onAdd() {
    if (!canAdd) return;
    let req: AddSubscriptionReq;
    if (srcType === "rss") {
      req = { url: url.trim() };
    } else if (srcType === "exa") {
      req = category
        ? { type: "exa", query: query.trim(), category }
        : { type: "exa", query: query.trim() };
    } else {
      req = { type: "tikhub_xhs", keyword: keyword.trim() };
    }
    setAdding(true);
    try {
      await api.addSubscription(req);
      if (srcType === "rss") setUrl("");
      else if (srcType === "exa") setQuery("");
      else setKeyword("");
      await load();
      toast.success("信源已添加");
    } catch (err) {
      toast.error(err instanceof ApiError ? err.message : "添加失败");
    } finally {
      setAdding(false);
    }
  }

  async function onRemove(id: number) {
    setRemovingId(id);
    try {
      await api.removeSubscription(id);
      await load();
      toast.success("已移除");
    } catch (err) {
      toast.error(err instanceof ApiError ? err.message : "移除失败");
    } finally {
      setRemovingId(null);
    }
  }

  return (
    <div className="space-y-6">
      <p className="text-sm text-muted-foreground">
        添加 RSS 链接、Exa 搜索词或小红书关键词，见微 Vane 会定期抓取并纳入推送候选。
      </p>

      {/* Add source card */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">添加信源</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <Tabs value={srcType} onValueChange={(v) => setSrcType(v as SrcType)}>
            <TabsList>
              <TabsTrigger value="rss">
                <Rss className="size-4 mr-1.5" />
                RSS
              </TabsTrigger>
              <TabsTrigger value="exa">
                <Search className="size-4 mr-1.5" />
                Exa 搜索
              </TabsTrigger>
              <TabsTrigger value="tikhub_xhs">
                <BookOpen className="size-4 mr-1.5" />
                小红书关键词
              </TabsTrigger>
            </TabsList>
          </Tabs>

          <div className="flex gap-2">
            {srcType === "rss" && (
              <Input
                className="flex-1"
                placeholder="https://example.com/feed.xml"
                value={url}
                onChange={(e) => setUrl(e.target.value)}
                onKeyDown={(e) => {
                  if (isSubmitEnter(e)) onAdd();
                }}
                autoComplete="off"
              />
            )}
            {srcType === "exa" && (
              <>
                <Input
                  className="flex-1"
                  placeholder="输入搜索词，如：AI Agent 落地案例"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  onKeyDown={(e) => {
                    if (isSubmitEnter(e)) onAdd();
                  }}
                  autoComplete="off"
                />
                <Select value={category} onValueChange={setCategory}>
                  <SelectTrigger className="w-36">
                    <SelectValue placeholder="类别不限" />
                  </SelectTrigger>
                  <SelectContent>
                    {EXA_CATEGORIES.map(([v, label]) => (
                      <SelectItem key={v} value={v || "__none__"}>
                        {label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </>
            )}
            {srcType === "tikhub_xhs" && (
              <Input
                className="flex-1"
                placeholder="输入小红书搜索关键词，如：手冲咖啡"
                value={keyword}
                onChange={(e) => setKeyword(e.target.value)}
                onKeyDown={(e) => {
                  if (isSubmitEnter(e)) onAdd();
                }}
                autoComplete="off"
              />
            )}
            <Button onClick={onAdd} disabled={!canAdd}>
              {adding ? (
                <Loader2 className="size-4 animate-spin" />
              ) : (
                <>
                  <Plus className="size-4 mr-1" />
                  添加
                </>
              )}
            </Button>
          </div>
          {valid.warn && (
            <p className="text-sm text-amber-600 dark:text-amber-400">{valid.warn}</p>
          )}
        </CardContent>
      </Card>

      {/* Source list */}
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
            还没有信源，添加第一个订阅源开始吧。
          </CardContent>
        </Card>
      )}
      <div className="space-y-2">
        {sources?.map((s) => {
          const dot = statusDot(s.status, s.fail_count);
          const meta = typeMeta(s);
          return (
            <Card key={s.id} className="transition-colors hover:bg-muted/30">
              <CardContent className="py-3 flex items-center gap-3">
                <span
                  className={`flex size-2.5 shrink-0 rounded-full ${dot.color}`}
                  title={dot.label}
                />
                <div className="flex-1 min-w-0">
                  <div className="text-sm font-medium truncate">
                    {s.title || s.url}
                  </div>
                  <div className="flex flex-wrap items-center gap-2 mt-0.5">
                    {meta ? (
                      <>
                        <Badge variant="outline" className="text-xs">
                          {meta.badge}
                        </Badge>
                        <span className="text-xs text-muted-foreground truncate">
                          {meta.term}
                        </span>
                      </>
                    ) : (
                      <a
                        className="text-xs text-muted-foreground hover:text-primary truncate"
                        href={s.url}
                        target="_blank"
                        rel="noreferrer"
                      >
                        {s.url}
                      </a>
                    )}
                    {s.last_fetched_at && (
                      <span className="text-xs text-muted-foreground">
                        上次抓取 {new Date(s.last_fetched_at).toLocaleString("zh-CN")}
                      </span>
                    )}
                  </div>
                </div>
                <Button
                  variant="ghost"
                  size="icon"
                  className="shrink-0 text-muted-foreground hover:text-destructive"
                  onClick={() => onRemove(s.id)}
                  disabled={removingId === s.id}
                >
                  {removingId === s.id ? (
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

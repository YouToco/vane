import { useEffect, useRef, useState } from "react";
import { Compass, ExternalLink, Loader2 } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";

export type ExplorationReason =
  | "challenges_judgment"
  | "adjacent_opportunity"
  | "new_source";

export type ExplorationFeedback =
  | "inspiring"
  | "off_target"
  | "mute_direction";

export interface ExplorationItem {
  content_item_id: number;
  direction_key: string;
  title: string;
  summary: string;
  source_title: string;
  source_url: string;
  reason: ExplorationReason;
}

export interface ExplorationCopy {
  title: string;
  description: string;
  empty: string;
  reasons: Record<ExplorationReason, string>;
  feedback: Record<ExplorationFeedback, string>;
  feedbackFailed: string;
  feedbackSaved: string;
  source: string;
}

function safeExplorationURL(raw: string): string | null {
  try {
    const parsed = new URL(raw);
    if (
      (parsed.protocol !== "https:" && parsed.protocol !== "http:") ||
      parsed.username !== "" ||
      parsed.password !== "" ||
      parsed.search !== "" ||
      parsed.hash !== ""
    ) {
      return null;
    }
    return parsed.toString();
  } catch {
    return null;
  }
}

export default function ExplorationPanel({
  scopeKey,
  items,
  copy,
  onFeedback,
}: {
  scopeKey: string;
  items: ExplorationItem[];
  copy: ExplorationCopy;
  onFeedback?: (
    item: ExplorationItem,
    feedback: ExplorationFeedback,
  ) => Promise<void>;
}) {
  const [pending, setPending] = useState("");
  const [error, setError] = useState("");
  const [saved, setSaved] = useState<Record<string, ExplorationFeedback>>({});
  const generation = useRef(0);

  useEffect(() => {
    generation.current += 1;
    setPending("");
    setError("");
    setSaved({});
    return () => {
      generation.current += 1;
    };
  }, [scopeKey]);

  async function submit(
    item: ExplorationItem,
    feedback: ExplorationFeedback,
  ) {
    if (!onFeedback || pending) return;
    const requestGeneration = generation.current;
    const key = `${item.content_item_id}:${item.direction_key}:${feedback}`;
    setPending(key);
    setError("");
    try {
      await onFeedback(item, feedback);
      if (generation.current === requestGeneration) {
        setSaved((current) => ({
          ...current,
          [`${item.content_item_id}:${item.direction_key}`]: feedback,
        }));
      }
    } catch {
      if (generation.current === requestGeneration) {
        setError(copy.feedbackFailed);
      }
    } finally {
      if (generation.current === requestGeneration) {
        setPending("");
      }
    }
  }

  return (
    <section aria-labelledby="exploration-title" className="space-y-3">
      <header className="space-y-1">
        <h2
          id="exploration-title"
          className="flex items-center gap-2 text-base font-semibold"
        >
          <Compass className="size-4" />
          {copy.title}
        </h2>
        <p className="text-sm text-muted-foreground">{copy.description}</p>
      </header>
      {items.length === 0 ? (
        <p className="rounded-lg border border-dashed px-4 py-6 text-sm text-muted-foreground">
          {copy.empty}
        </p>
      ) : (
        <div className="grid min-w-0 gap-3">
          {items.slice(0, 3).map((item) => {
            const sourceURL = safeExplorationURL(item.source_url);
            const itemKey = `${item.content_item_id}:${item.direction_key}`;
            const savedFeedback = saved[itemKey];
            return (
              <Card key={item.content_item_id}>
                <CardContent className="min-w-0 space-y-3 p-4">
                  <div className="flex min-w-0 flex-wrap items-start justify-between gap-2">
                    <h3 className="min-w-0 break-words font-medium leading-6 [overflow-wrap:anywhere]">
                      {item.title}
                    </h3>
                    <Badge variant="outline">{copy.reasons[item.reason]}</Badge>
                  </div>
                  <p className="min-w-0 break-words text-sm leading-6 text-foreground/85 [overflow-wrap:anywhere]">
                    {item.summary}
                  </p>
                  <div className="flex min-w-0 flex-wrap items-center justify-between gap-2">
                    {sourceURL ? (
                      <a
                        href={sourceURL}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="inline-flex min-w-0 max-w-full items-center gap-1 break-all text-xs text-muted-foreground underline [overflow-wrap:anywhere]"
                      >
                        {copy.source}: {item.source_title}
                        <ExternalLink className="size-3" />
                      </a>
                    ) : (
                      <span className="min-w-0 break-all text-xs text-muted-foreground [overflow-wrap:anywhere]">
                        {copy.source}: {item.source_title}
                      </span>
                    )}
                    {onFeedback && (
                      <div className="flex flex-wrap gap-1.5">
                        {(
                          [
                            "inspiring",
                            "off_target",
                            "mute_direction",
                          ] as const
                        ).map((feedback) => {
                          const key =
                            `${item.content_item_id}:${item.direction_key}:${feedback}`;
                          return (
                            <Button
                              key={feedback}
                              variant="ghost"
                              size="sm"
                              aria-pressed={savedFeedback === feedback}
                              disabled={pending !== "" || savedFeedback !== undefined}
                              onClick={() => void submit(item, feedback)}
                            >
                              {pending === key && (
                                <Loader2 className="mr-1 size-3 animate-spin" />
                              )}
                              {copy.feedback[feedback]}
                            </Button>
                          );
                        })}
                      </div>
                    )}
                  </div>
                  {savedFeedback && (
                    <p
                      role="status"
                      aria-live="polite"
                      className="text-xs text-muted-foreground"
                    >
                      {copy.feedbackSaved}
                    </p>
                  )}
                </CardContent>
              </Card>
            );
          })}
        </div>
      )}
      {error && (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      )}
    </section>
  );
}

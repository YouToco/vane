import { useEffect, useLayoutEffect, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { ExternalLink, Loader2 } from "lucide-react";

import { api, ApiError } from "@/api";
import type {
  TaskBrief,
  TaskBriefEvidenceSource,
  TaskBriefInsight,
  TaskLatestCheck,
} from "@/api";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { fmt, useI18n } from "@/i18n";
import { briefDict, type BriefDict } from "@/i18n/brief";
import {
  safeBriefMarkdownURL,
  safeBriefURL,
} from "@/lib/brief-presentation";
import { fmtBeijing } from "@/lib/time";

const PAGE_SIZE = 10;
const BRIEF_MARKDOWN_ELEMENTS = [
  "p",
  "h1",
  "h2",
  "h3",
  "h4",
  "h5",
  "h6",
  "strong",
  "em",
  "del",
  "a",
  "ul",
  "ol",
  "li",
  "blockquote",
  "code",
  "pre",
  "br",
  "hr",
  "table",
  "thead",
  "tbody",
  "tr",
  "th",
  "td",
] as const;

function feedbackLabel(
  action: string,
  d: BriefDict,
): string {
  const labels: Record<string, string> = {
    interested: d.briefFeedbackInterested,
    not_interested: d.briefFeedbackNotInterested,
    misjudged: d.briefFeedbackIssue,
    deep_dive: d.briefFeedbackDeepDive,
  };
  return labels[action] ?? action;
}

export function InsightBody({ markdown }: { markdown: string }) {
  return (
    <div className="text-sm leading-6 text-foreground/90 [&_a]:text-primary [&_a]:underline [&_blockquote]:border-l-2 [&_blockquote]:pl-3 [&_blockquote]:text-muted-foreground [&_code]:rounded [&_code]:bg-muted [&_code]:px-1 [&_h1]:text-lg [&_h1]:font-semibold [&_h2]:text-base [&_h2]:font-semibold [&_h3]:font-semibold [&_h4]:font-medium [&_h5]:font-medium [&_h6]:font-medium [&_li]:ml-5 [&_li]:list-disc [&_ol_li]:list-decimal [&_p+p]:mt-2 [&_pre]:overflow-x-auto [&_pre]:rounded-md [&_pre]:bg-muted [&_pre]:p-3">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        skipHtml
        allowedElements={[...BRIEF_MARKDOWN_ELEMENTS]}
        urlTransform={safeBriefMarkdownURL}
        components={{
          a: ({ href, children }) => {
            const safe = safeBriefURL(href);
            return safe ? (
              <a href={safe} target="_blank" rel="noopener noreferrer">
                {children}
              </a>
            ) : (
              <span>{children}</span>
            );
          },
          table: ({ children, ...props }) => (
            <div className="my-3 max-w-full overflow-x-auto">
              <table
                {...props}
                className="min-w-full border-collapse text-left"
              >
                {children}
              </table>
            </div>
          ),
          th: ({ children, ...props }) => (
            <th
              {...props}
              className="min-w-32 break-all border px-2 py-1 font-medium"
            >
              {children}
            </th>
          ),
          td: ({ children, ...props }) => (
            <td {...props} className="min-w-32 break-all border px-2 py-1">
              {children}
            </td>
          ),
        }}
      >
        {markdown}
      </ReactMarkdown>
    </div>
  );
}

function hasStructuredProjection(insight: TaskBriefInsight): boolean {
  const structured = insight.structured;
  return Boolean(
    structured &&
      structured.schema_version === "vane.cardgen-insight/v1" &&
      structured.body_md === insight.body_md &&
      structured.what_changed &&
      structured.why_it_matters &&
      structured.importance_reason,
  );
}

type ValidatedEvidenceSource = TaskBriefEvidenceSource & {
  safeURL: string;
};

function validBriefTime(value: string | undefined): boolean {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    !Number.isNaN(Date.parse(value))
  );
}

export function validatedEventEvidence(
  insight: TaskBriefInsight,
): ValidatedEvidenceSource[] | null {
  const eventEvidence = insight.event_evidence;
  const claims = insight.structured?.claims;
  if (
    !hasStructuredProjection(insight) ||
    !eventEvidence ||
    eventEvidence.schema_version !== "vane.structured-event-evidence/v1" ||
    !Array.isArray(eventEvidence.sources) ||
    eventEvidence.sources.length === 0 ||
    !Array.isArray(claims)
  ) {
    return null;
  }
  const projected: ValidatedEvidenceSource[] = [];
  const refs = new Set<string>();
  for (const [index, source] of eventEvidence.sources.entries()) {
    const expectedRef = `source-${index + 1}`;
    const safeURL = safeBriefURL(source?.source_url);
    if (
      !source ||
      source.ref !== expectedRef ||
      typeof source.title !== "string" ||
      !source.title ||
      typeof source.source_title !== "string" ||
      typeof source.platform !== "string" ||
      !source.platform ||
      typeof source.source_url !== "string" ||
      source.source_url.trim() !== source.source_url ||
      !safeURL ||
      !validBriefTime(source.discovered_at) ||
      (source.published_at !== undefined &&
        !validBriefTime(source.published_at))
    ) {
      return null;
    }
    refs.add(source.ref);
    projected.push({ ...source, safeURL });
  }
  for (const claim of claims) {
    if (
      !claim ||
      !Array.isArray(claim.source_refs) ||
      claim.source_refs.length === 0
    ) {
      return null;
    }
    const claimRefs = new Set(claim.source_refs);
    if (
      claimRefs.size !== claim.source_refs.length ||
      claim.source_refs.some(
        (ref) => typeof ref !== "string" || !refs.has(ref),
      )
    ) {
      return null;
    }
  }
  return projected;
}

export function BriefInsightBody({
  insight,
  d,
}: {
  insight: TaskBriefInsight;
  d: BriefDict;
}) {
  if (!hasStructuredProjection(insight)) {
    return <InsightBody markdown={insight.body_md} />;
  }
  const structured = insight.structured!;
  const claims = Array.isArray(structured.claims) ? structured.claims : [];
  const evidenceSources = validatedEventEvidence(insight);
  const evidenceByRef = new Map(
    evidenceSources?.map((source) => [source.ref, source]) ?? [],
  );
  return (
    <div className="space-y-4">
      <dl className="grid gap-3 rounded-lg border bg-muted/20 p-4 sm:grid-cols-3">
        {[
          [d.briefWhatChanged, structured.what_changed],
          [d.briefWhyItMatters, structured.why_it_matters],
          [d.briefImportanceReason, structured.importance_reason],
        ].map(([label, value]) => (
          <div key={label} className="space-y-1">
            <dt className="text-xs font-medium text-muted-foreground">
              {label}
            </dt>
            <dd className="text-sm leading-6 text-foreground/90">{value}</dd>
          </div>
        ))}
      </dl>
      {claims.length > 0 && (
        <section
          className="space-y-2"
          aria-label={d.briefEvidence}
        >
          <h4 className="text-xs font-medium text-muted-foreground">
            {d.briefEvidence}
          </h4>
          <ul className="space-y-2">
            {claims.map((claim, index) => (
              <li
                key={`${index}-${claim.text}`}
                className="rounded-md border-l-2 border-primary/40 bg-muted/20 px-3 py-2"
              >
                <p className="text-sm leading-6">{claim.text}</p>
                <p className="mt-1 text-xs leading-5 text-muted-foreground">
                  <span className="font-medium">
                    {d.briefEvidenceExcerpt}：
                  </span>
                  “{claim.excerpt}”
                </p>
                {evidenceSources && (
                  <div
                    className="mt-2 flex flex-wrap items-center gap-1.5"
                    aria-label={d.briefClaimSources}
                  >
                    {claim.source_refs.map((ref) => {
                      const source = evidenceByRef.get(ref)!;
                      return (
                        <a
                          key={ref}
                          href={source.safeURL}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="rounded-full border px-2 py-0.5 text-xs text-primary hover:underline"
                        >
                          {source.source_title ||
                            source.platform ||
                            source.title}
                        </a>
                      );
                    })}
                  </div>
                )}
              </li>
            ))}
          </ul>
        </section>
      )}
      {evidenceSources && (
        <section className="space-y-2" aria-label={d.briefSources}>
          <h4 className="text-xs font-medium text-muted-foreground">
            {d.briefSources}
          </h4>
          <ol className="space-y-2">
            {evidenceSources.map((source) => (
              <li
                key={source.ref}
                className="flex gap-2 rounded-md border bg-background px-3 py-2"
              >
                <span className="shrink-0 font-mono text-xs text-muted-foreground">
                  {source.ref.replace("source-", "")}.
                </span>
                <div className="min-w-0 space-y-1">
                  <a
                    href={source.safeURL}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-start gap-1 text-sm font-medium text-primary hover:underline"
                  >
                    <span>{source.title}</span>
                    <ExternalLink className="mt-1 size-3 shrink-0" />
                  </a>
                  <div className="flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted-foreground">
                    <span>
                      {source.source_title || d.briefUnknownSource}
                      {source.platform ? ` · ${source.platform}` : ""}
                    </span>
                    {source.published_at && (
                      <span>
                        {d.briefPublished}{" "}
                        {fmtBeijing(source.published_at)}
                      </span>
                    )}
                    <span>
                      {d.briefDiscovered}{" "}
                      {fmtBeijing(source.discovered_at)}
                    </span>
                  </div>
                </div>
              </li>
            ))}
          </ol>
        </section>
      )}
    </div>
  );
}

function PartialBadge({
  brief,
  label,
}: {
  brief: TaskBrief;
  label: string;
}) {
  if (brief.source_coverage === "complete" && brief.processing === "complete") {
    return null;
  }
  return (
    <Badge variant="outline" className="text-amber-700 dark:text-amber-300">
      {label}
    </Badge>
  );
}

export default function TaskBriefFeed({
  scheduleID,
  onLatestCheck,
}: {
  scheduleID: string;
  onLatestCheck?: (check?: TaskLatestCheck) => void;
}) {
  const { t, locale } = useI18n();
  const D = briefDict(locale);
  const [items, setItems] = useState<TaskBrief[]>([]);
  const [total, setTotal] = useState(0);
  const [nextToken, setNextToken] = useState("");
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [loadError, setLoadError] = useState("");
  const requestGeneration = useRef(0);
  const activeScheduleID = useRef(scheduleID);

  useLayoutEffect(() => {
    activeScheduleID.current = scheduleID;
    requestGeneration.current += 1;
    return () => {
      requestGeneration.current += 1;
      if (activeScheduleID.current === scheduleID) {
        activeScheduleID.current = "";
      }
    };
  }, [scheduleID]);

  useEffect(() => {
    let alive = true;
    const generation = ++requestGeneration.current;
    const isCurrent = () =>
      alive &&
      requestGeneration.current === generation &&
      activeScheduleID.current === scheduleID;
    setLoading(true);
    setLoadingMore(false);
    setItems([]);
    setTotal(0);
    setNextToken("");
    setLoadError("");
    api
      .scheduleBriefs(scheduleID, PAGE_SIZE)
      .then((page) => {
        if (!isCurrent()) return;
        setItems(page.items);
        setTotal(page.total);
        setNextToken(page.next_page_token ?? "");
        setLoadError("");
        onLatestCheck?.(page.latest_check);
      })
      .catch((error) => {
        if (!isCurrent()) return;
        setItems([]);
        setTotal(0);
        setNextToken("");
        setLoadError(
          error instanceof ApiError ? error.message : t.app.common.loadFailed,
        );
      })
      .finally(() => isCurrent() && setLoading(false));
    return () => {
      alive = false;
    };
    // onLatestCheck is a notification sink; refetch is task-bound only.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scheduleID]);

  async function loadMore() {
    if (!nextToken || loadingMore) return;
    const generation = requestGeneration.current;
    const requestScheduleID = scheduleID;
    const requestToken = nextToken;
    const isCurrent = () =>
      requestGeneration.current === generation &&
      activeScheduleID.current === requestScheduleID;
    setLoadingMore(true);
    try {
      const page = await api.scheduleBriefs(
        requestScheduleID,
        PAGE_SIZE,
        requestToken,
      );
      if (!isCurrent()) return;
      setItems((current) => current.concat(page.items));
      setTotal(page.total);
      setNextToken(page.next_page_token ?? "");
      setLoadError("");
      onLatestCheck?.(page.latest_check);
    } catch (error) {
      if (!isCurrent()) return;
      setLoadError(
        error instanceof ApiError ? error.message : t.app.common.loadFailed,
      );
    } finally {
      if (isCurrent()) setLoadingMore(false);
    }
  }

  if (loading) {
    return (
      <Card>
        <CardContent className="space-y-4 py-6">
          {Array.from({ length: 3 }).map((_, index) => (
            <div key={index} className="space-y-2">
              <Skeleton className="h-4 w-36" />
              <Skeleton className="h-5 w-3/4" />
              <Skeleton className="h-12 w-full" />
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
              {D.briefsEmpty}
            </CardContent>
          </Card>
        )
      ) : (
        <>
          {items.map((brief) => (
            <Card key={brief.id}>
              <CardContent className="p-0">
                <header className="flex flex-wrap items-center justify-between gap-2 border-b px-4 py-3 sm:px-5">
                  <div>
                    <h2 className="text-sm font-medium">{D.briefTitle}</h2>
                    <time className="text-xs text-muted-foreground">
                      {fmtBeijing(brief.generated_at)}
                    </time>
                  </div>
                  <div className="flex items-center gap-2">
                    <PartialBadge brief={brief} label={D.briefPartial} />
                    <Badge variant="secondary">
                      {fmt(D.briefInsightCount, {
                        n: brief.insights.length,
                      })}
                    </Badge>
                  </div>
                </header>
                <div className="divide-y">
                  {brief.insights.map((insight) => {
                    const hasEventEvidence =
                      validatedEventEvidence(insight) !== null;
                    const href = hasEventEvidence
                      ? null
                      : safeBriefURL(insight.source_url);
                    return (
                      <article
                        key={insight.id}
                        className="space-y-3 px-4 py-5 sm:px-5"
                      >
                        <div className="flex gap-3">
                          <span className="mt-0.5 text-xs font-mono text-muted-foreground">
                            {insight.rank_position}
                          </span>
                          <div className="min-w-0 flex-1 space-y-2">
                            <h3 className="font-medium leading-6">
                              {href ? (
                                <a
                                  href={href}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  className="inline-flex items-start gap-1 text-primary hover:underline"
                                >
                                  <span>{insight.title}</span>
                                  <ExternalLink className="mt-1 size-3 shrink-0" />
                                </a>
                              ) : (
                                insight.title
                              )}
                            </h3>
                            <BriefInsightBody insight={insight} d={D} />
                            {!hasEventEvidence && (
                              <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
                                <span>
                                  {insight.source_title ||
                                    D.briefUnknownSource}
                                </span>
                                {insight.published_at && (
                                  <span>
                                    {D.briefPublished}{" "}
                                    {fmtBeijing(insight.published_at)}
                                  </span>
                                )}
                                <span>
                                  {D.briefDiscovered}{" "}
                                  {fmtBeijing(insight.discovered_at)}
                                </span>
                              </div>
                            )}
                            {(insight.feedback.preference ||
                              insight.feedback.misjudged ||
                              insight.feedback.deep_dive_requested) && (
                              <div
                                className="flex flex-wrap gap-1"
                                aria-label={D.briefFeedback}
                              >
                                {[
                                  insight.feedback.preference,
                                  insight.feedback.misjudged
                                    ? "misjudged"
                                    : undefined,
                                  insight.feedback.deep_dive_requested
                                    ? "deep_dive"
                                    : undefined,
                                ]
                                  .filter(
                                    (action): action is string =>
                                      action !== undefined,
                                  )
                                  .map((action) => (
                                    <Badge key={action} variant="outline">
                                      {feedbackLabel(action, D)}
                                    </Badge>
                                  ))}
                              </div>
                            )}
                          </div>
                        </div>
                      </article>
                    );
                  })}
                </div>
              </CardContent>
            </Card>
          ))}
          <div className="flex items-center justify-between px-1">
            <span className="text-sm text-muted-foreground">
              {fmt(D.briefsShown, { shown: items.length, total })}
            </span>
            {nextToken && (
              <Button
                variant="outline"
                size="sm"
                onClick={loadMore}
                disabled={loadingMore}
              >
                {loadingMore && <Loader2 className="mr-2 size-4 animate-spin" />}
                {loadingMore ? t.app.common.loading : t.app.common.loadMore}
              </Button>
            )}
          </div>
        </>
      )}
    </div>
  );
}

import { useEffect, useLayoutEffect, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { ExternalLink, Loader2, MessageCircle, Settings2 } from "lucide-react";

import { api, ApiError } from "@/shared/api/client";
import type {
  TaskBrief,
  TaskBriefEvidenceSource,
  TaskBriefInsight,
  TaskLatestCheck,
  ExecutiveContent,
  PeriodicBriefReport,
  BriefReportSettings,
  GroundedBriefContext,
  TaskHealthProjection,
} from "@/shared/api/client";
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
} from "@/shared/utils/brief-presentation";
import { fmtBeijing } from "@/shared/utils/time";

const PAGE_SIZE = 10;

function executiveCopy(locale: string) {
  const english = {
    issue: "Issue",
    weekly: "Weekly",
    monthly: "Monthly",
    conclusion: "Conclusion",
    why: "Why it matters to you",
    signals: "Shared signals",
    next: "Next steps",
    coverage: "Coverage",
    complete: "Complete coverage",
    partial: "Coverage is incomplete; some signals may be missing.",
    fallback: "A conservative summary was used. Check the underlying evidence.",
    noReports: "No report for this period yet.",
    settings: "Report settings",
    auto: "Smart cadence",
    manual: "Fixed cadence",
    important: "Important only",
    always: "Every report",
    webOnly: "Web only",
    daily: "Daily",
    delivery: "Delivery",
    ask: "Ask about this brief",
    askPlaceholder: "For example: What does this change for my work?",
    send: "Send",
    evidence: "Evidence",
    decision: {
      act: "Act",
      watch: "Watch",
      no_action: "No action",
      insufficient_evidence: "Insufficient evidence",
    },
    lifecycle: {
      new: "New",
      persistent: "Persistent",
      intensified: "Intensified",
      faded: "Faded",
    },
  };
  const copies: Record<string, typeof english> = {
    en: english,
    "zh-CN": {
        issue: "本期",
        weekly: "周报",
        monthly: "月报",
        conclusion: "本期结论",
        why: "为什么与你有关",
        signals: "共同信号",
        next: "下一步",
        coverage: "覆盖状态",
        complete: "覆盖完整",
        partial: "覆盖不完整，结论可能遗漏部分信号",
        fallback: "本期使用了保守整理，建议结合下方原始证据判断。",
        noReports: "这个周期还没有报告。",
        settings: "周期设置",
        auto: "智能周期",
        manual: "固定周期",
        important: "仅重要时推送",
        always: "每期推送",
        webOnly: "仅 Web",
        daily: "日报",
        delivery: "推送方式",
        ask: "基于本简报追问",
        askPlaceholder: "例如：这对我当前负责的工作有什么直接影响？",
        send: "发送",
        evidence: "证据",
        decision: {
          act: "建议行动",
          watch: "继续观察",
          no_action: "暂不行动",
          insufficient_evidence: "证据不足",
        },
        lifecycle: {
          new: "新增",
          persistent: "持续",
          intensified: "增强",
          faded: "消退",
        },
      },
    "zh-TW": {
      issue: "本期", weekly: "週報", monthly: "月報",
      conclusion: "本期結論", why: "為什麼與你有關",
      signals: "共同訊號", next: "下一步", coverage: "覆蓋狀態",
      complete: "覆蓋完整", partial: "覆蓋不完整，結論可能遺漏部分訊號",
      fallback: "本期使用保守整理，建議一併查看原始證據。",
      noReports: "這個週期還沒有報告。", settings: "週期設定",
      auto: "智慧週期", manual: "固定週期", important: "僅重要時推送",
      always: "每期推送", webOnly: "僅 Web", daily: "日報",
      delivery: "推送方式", ask: "依據本簡報追問",
      askPlaceholder: "例如：這對我目前負責的工作有什麼直接影響？",
      send: "傳送", evidence: "證據",
      decision: { act: "建議行動", watch: "繼續觀察", no_action: "暫不行動", insufficient_evidence: "證據不足" },
      lifecycle: { new: "新增", persistent: "持續", intensified: "增強", faded: "消退" },
    },
    ja: {
      issue: "今回", weekly: "週報", monthly: "月報",
      conclusion: "今回の結論", why: "あなたに関係する理由",
      signals: "共通信号", next: "次のステップ", coverage: "カバレッジ",
      complete: "完全", partial: "一部の情報を取得できていないため、信号が欠ける可能性があります。",
      fallback: "保守的な要約です。根拠も確認してください。",
      noReports: "この期間のレポートはまだありません。", settings: "周期設定",
      auto: "自動周期", manual: "固定周期", important: "重要な場合のみ",
      always: "毎回", webOnly: "Web のみ", daily: "日報",
      delivery: "配信", ask: "このブリーフについて質問",
      askPlaceholder: "例：私の担当業務にどのような影響がありますか？",
      send: "送信", evidence: "根拠",
      decision: { act: "対応推奨", watch: "監視継続", no_action: "対応不要", insufficient_evidence: "根拠不足" },
      lifecycle: { new: "新規", persistent: "継続", intensified: "強化", faded: "収束" },
    },
    ko: {
      issue: "이번 호", weekly: "주간", monthly: "월간",
      conclusion: "이번 결론", why: "나와 관련된 이유",
      signals: "공통 신호", next: "다음 단계", coverage: "수집 범위",
      complete: "완전함", partial: "수집 범위가 불완전해 일부 신호가 누락될 수 있습니다.",
      fallback: "보수적으로 정리했습니다. 근거도 함께 확인하세요.",
      noReports: "이 기간의 보고서가 아직 없습니다.", settings: "주기 설정",
      auto: "스마트 주기", manual: "고정 주기", important: "중요할 때만",
      always: "매번", webOnly: "Web 전용", daily: "일간",
      delivery: "전송", ask: "이 브리프에 질문",
      askPlaceholder: "예: 현재 제 업무에 어떤 직접 영향이 있나요?",
      send: "전송", evidence: "근거",
      decision: { act: "조치 권장", watch: "계속 관찰", no_action: "조치 없음", insufficient_evidence: "근거 부족" },
      lifecycle: { new: "신규", persistent: "지속", intensified: "강화", faded: "소멸" },
    },
    es: {
      issue: "Edición", weekly: "Semanal", monthly: "Mensual",
      conclusion: "Conclusión", why: "Por qué te afecta",
      signals: "Señales comunes", next: "Próximos pasos", coverage: "Cobertura",
      complete: "Cobertura completa", partial: "La cobertura es incompleta; pueden faltar señales.",
      fallback: "Se usó un resumen conservador. Revisa las evidencias.",
      noReports: "Aún no hay informe para este periodo.", settings: "Configuración",
      auto: "Cadencia inteligente", manual: "Cadencia fija", important: "Solo importantes",
      always: "Cada informe", webOnly: "Solo Web", daily: "Diario",
      delivery: "Entrega", ask: "Pregunta sobre este informe",
      askPlaceholder: "Ejemplo: ¿Cómo afecta esto a mi trabajo?",
      send: "Enviar", evidence: "Evidencias",
      decision: { act: "Actuar", watch: "Vigilar", no_action: "Sin acción", insufficient_evidence: "Evidencia insuficiente" },
      lifecycle: { new: "Nueva", persistent: "Persistente", intensified: "Intensificada", faded: "Disipada" },
    },
    fr: {
      issue: "Édition", weekly: "Hebdo", monthly: "Mensuel",
      conclusion: "Conclusion", why: "Pourquoi cela vous concerne",
      signals: "Signaux communs", next: "Étapes suivantes", coverage: "Couverture",
      complete: "Couverture complète", partial: "La couverture est incomplète ; certains signaux peuvent manquer.",
      fallback: "Un résumé prudent a été utilisé. Consultez les preuves.",
      noReports: "Aucun rapport pour cette période.", settings: "Réglages",
      auto: "Cadence intelligente", manual: "Cadence fixe", important: "Importants seulement",
      always: "Chaque rapport", webOnly: "Web uniquement", daily: "Quotidien",
      delivery: "Diffusion", ask: "Questionner ce brief",
      askPlaceholder: "Exemple : quel impact sur mon travail ?",
      send: "Envoyer", evidence: "Preuves",
      decision: { act: "Agir", watch: "Surveiller", no_action: "Aucune action", insufficient_evidence: "Preuves insuffisantes" },
      lifecycle: { new: "Nouveau", persistent: "Persistant", intensified: "Renforcé", faded: "Atténué" },
    },
  };
  const key = locale.startsWith("zh")
    ? locale.includes("TW") || locale.includes("HK") || locale.includes("Hant")
      ? "zh-TW"
      : "zh-CN"
    : locale.split("-")[0];
  return copies[key] ?? english;
}

function ExecutivePanel({
  content,
  partial,
  fallback,
  locale,
  scheduleID,
  target,
  onAdjustTask,
  onCreateTask,
}: {
  content: ExecutiveContent;
  partial: boolean;
  fallback: boolean;
  locale: string;
  scheduleID: string;
  target: { kind: "brief" | "report"; id: number };
  onAdjustTask?: () => void;
  onCreateTask?: () => void;
}) {
  const copy = executiveCopy(locale);
  const signals = Array.isArray(content.signals) ? content.signals : [];
  const nextSteps = Array.isArray(content.next_steps)
    ? content.next_steps
    : [];
  const [question, setQuestion] = useState("");
  const [reply, setReply] = useState("");
  const [askError, setAskError] = useState("");
  const [asking, setAsking] = useState(false);
  const [grounding, setGrounding] = useState<GroundedBriefContext | null>(null);
  const [evidenceError, setEvidenceError] = useState("");
  const [loadingEvidence, setLoadingEvidence] = useState(false);

  async function loadEvidence() {
    if (grounding || loadingEvidence) return;
    setLoadingEvidence(true);
    setEvidenceError("");
    try {
      const value =
        target.kind === "brief"
          ? await api.briefGrounding(scheduleID, target.id)
          : await api.reportGrounding(scheduleID, target.id);
      setGrounding(value);
    } catch (error) {
      setEvidenceError(
        error instanceof ApiError ? error.message : "Unable to load evidence.",
      );
    } finally {
      setLoadingEvidence(false);
    }
  }

  function evidenceForRef(
    ref: ExecutiveContent["signals"][number]["evidence_refs"][number],
  ) {
    const briefID = ref.brief_id ?? target.id;
    const brief = grounding?.evidence.find((item) => item.brief_id === briefID);
    return brief?.insights.find((item) => item.id === ref.insight_id);
  }

  async function askGrounded(prefill?: string) {
    const value = (prefill ?? question).trim();
    if (!value || asking) return;
    setQuestion(value);
    setAsking(true);
    setAskError("");
    try {
      const response =
        target.kind === "brief"
          ? await api.askBrief(scheduleID, target.id, value)
          : await api.askReport(scheduleID, target.id, value);
      setReply(response.reply);
    } catch (error) {
      setAskError(
        error instanceof ApiError ? error.message : "Unable to answer.",
      );
    } finally {
      setAsking(false);
    }
  }

  async function runStep(step: ExecutiveContent["next_steps"][number]) {
    if (step.kind === "deep_dive") {
      const insightID = step.evidence_refs[0]?.insight_id;
      if (!insightID || asking) return;
      setAsking(true);
      setAskError("");
      try {
        const response =
          target.kind === "brief"
            ? await api.deepDiveBrief(scheduleID, target.id, insightID)
            : await api.deepDiveReport(scheduleID, target.id, insightID);
        setReply(response.message);
      } catch (error) {
        setAskError(
          error instanceof ApiError ? error.message : "Unable to start.",
        );
      } finally {
        setAsking(false);
      }
      return;
    }
    if (step.kind === "create_task") {
      onCreateTask?.();
      return;
    }
    onAdjustTask?.();
  }
  return (
    <section className="space-y-5 border-b bg-muted/20 px-4 py-5 sm:px-5">
      <div className="space-y-1">
        <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
          {copy.conclusion}
        </p>
        <h3 className="text-lg font-semibold leading-7">{content.headline}</h3>
        <Badge variant="secondary">
          {copy.decision[content.decision_state]}
        </Badge>
        <p className="text-sm leading-6 text-foreground/85">
          {content.executive_summary}
        </p>
      </div>
      <div className="space-y-1">
        <p className="text-xs font-medium text-muted-foreground">{copy.why}</p>
        <p className="text-sm leading-6">{content.why_for_you}</p>
      </div>
      {signals.length > 0 && (
        <div className="space-y-2">
          <p className="text-xs font-medium text-muted-foreground">
            {copy.signals}
          </p>
          <ol className="space-y-2">
            {signals.slice(0, 3).map((signal, index) => (
              <li key={`${signal.kind}-${index}`} className="text-sm">
                <span className="mr-2 font-mono text-muted-foreground">
                  {index + 1}
                </span>
                <span className="font-medium">{signal.title}</span>
                {signal.lifecycle && (
                  <Badge variant="outline" className="ml-2">
                    {copy.lifecycle[signal.lifecycle]}
                  </Badge>
                )}
                <span className="ml-2 text-muted-foreground">
                  {signal.summary}
                </span>
                <details
                  className="ml-7 mt-1 text-xs text-muted-foreground"
                  onToggle={(event) => {
                    if (event.currentTarget.open) void loadEvidence();
                  }}
                >
                  <summary className="cursor-pointer">{copy.evidence}</summary>
                  {signal.evidence_refs.map((ref) => (
                    <div
                      key={`${ref.brief_id ?? 0}-${ref.insight_id}`}
                      className="mt-2 rounded-md border bg-background p-2"
                    >
                      {(() => {
                        const insight = evidenceForRef(ref);
                        if (!insight) {
                          return (
                            <span>
                              {ref.brief_id ? `Brief ${ref.brief_id} · ` : ""}
                              Insight {ref.insight_id} · claims{" "}
                              {ref.claim_indexes
                                .map((value) => value + 1)
                                .join(", ")}
                            </span>
                          );
                        }
                        const sources = new Map(
                          insight.event_evidence?.sources.map((source) => [
                            source.ref,
                            source,
                          ]) ?? [],
                        );
                        return (
                          <div className="space-y-2">
                            <p className="font-medium text-foreground">
                              {insight.title}
                            </p>
                            {ref.claim_indexes.map((claimIndex) => {
                              const claim = insight.structured?.claims[claimIndex];
                              if (!claim) return null;
                              return (
                                <div key={claimIndex} className="space-y-1">
                                  <p>{claim.text}</p>
                                  {claim.excerpt && (
                                    <blockquote className="border-l-2 pl-2">
                                      {claim.excerpt}
                                    </blockquote>
                                  )}
                                  <div className="flex flex-wrap gap-x-2">
                                    {claim.source_refs.map((sourceRef) => {
                                      const source = sources.get(sourceRef);
                                      const href = source
                                        ? safeBriefURL(source.source_url)
                                        : null;
                                      return href ? (
                                        <a
                                          key={sourceRef}
                                          href={href}
                                          target="_blank"
                                          rel="noreferrer"
                                          className="underline"
                                        >
                                          {source?.source_title ||
                                            source?.title ||
                                            sourceRef}
                                        </a>
                                      ) : (
                                        <span key={sourceRef}>{sourceRef}</span>
                                      );
                                    })}
                                  </div>
                                </div>
                              );
                            })}
                          </div>
                        );
                      })()}
                    </div>
                  ))}
                  {loadingEvidence && (
                    <Loader2 className="mt-2 size-4 animate-spin" />
                  )}
                  {evidenceError && (
                    <p className="mt-2 text-destructive">{evidenceError}</p>
                  )}
                </details>
              </li>
            ))}
          </ol>
        </div>
      )}
      {nextSteps.length > 0 && (
        <div className="space-y-2">
          <p className="text-xs font-medium text-muted-foreground">
            {copy.next}
          </p>
          <div className="flex flex-wrap gap-2">
            {nextSteps.map((step, index) => (
              <Button
                key={`${step.kind}-${index}`}
                variant="outline"
                size="sm"
                onClick={() => void runStep(step)}
                title={step.rationale}
              >
                {step.label}
              </Button>
            ))}
          </div>
        </div>
      )}
      <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
        <span>{copy.coverage}: {partial ? copy.partial : copy.complete}</span>
        {fallback && <span>{copy.fallback}</span>}
      </div>
      <div
        data-grounded-followup
        className="space-y-2 rounded-lg border bg-background p-3"
      >
        <label className="flex items-center gap-2 text-xs font-medium">
          <MessageCircle className="size-4" />
          {copy.ask}
        </label>
        <div className="flex gap-2">
          <input
            value={question}
            onChange={(event) => setQuestion(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter") void askGrounded();
            }}
            maxLength={16000}
            placeholder={copy.askPlaceholder}
            className="h-9 min-w-0 flex-1 rounded-md border bg-background px-3 text-sm"
          />
          <Button
            size="sm"
            disabled={asking || question.trim() === ""}
            onClick={() => void askGrounded()}
          >
            {asking && <Loader2 className="mr-2 size-4 animate-spin" />}
            {copy.send}
          </Button>
        </div>
        {reply && <p className="whitespace-pre-wrap text-sm leading-6">{reply}</p>}
        {askError && <p className="text-sm text-destructive">{askError}</p>}
      </div>
    </section>
  );
}

function PeriodicReportCard({
  report,
  locale,
  scheduleID,
  onAdjustTask,
  onCreateTask,
}: {
  report: PeriodicBriefReport;
  locale: string;
  scheduleID: string;
  onAdjustTask?: () => void;
  onCreateTask?: () => void;
}) {
  const formatPeriod = (value: string) =>
    new Intl.DateTimeFormat(locale, {
      timeZone: report.timezone,
      year: "numeric",
      month: "short",
      day: "numeric",
    }).format(new Date(value));
  return (
    <Card>
      <CardContent className="p-0">
        <header className="border-b px-4 py-3 text-xs text-muted-foreground sm:px-5">
          {formatPeriod(report.period_start)} – {formatPeriod(report.period_end)}
          <span className="ml-2">[{report.timezone}; end exclusive]</span>
        </header>
        <ExecutivePanel
          content={report.content}
          locale={locale}
          partial={
            report.source_coverage === "partial" ||
            report.processing === "partial"
          }
          fallback={report.generation_mode === "deterministic_fallback"}
          scheduleID={scheduleID}
          target={{ kind: "report", id: report.id }}
          onAdjustTask={onAdjustTask}
          onCreateTask={onCreateTask}
        />
      </CardContent>
    </Card>
  );
}
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

type BriefPeriodView = "issue" | "daily" | "weekly" | "monthly";

export default function TaskBriefFeed({
  scheduleID,
  onLatestCheck,
  onHealth,
  onAdjustTask,
  onCreateTask,
}: {
  scheduleID: string;
  onLatestCheck?: (check?: TaskLatestCheck) => void;
  onHealth?: (health?: TaskHealthProjection) => void;
  onAdjustTask?: () => void;
  onCreateTask?: () => void;
}) {
  const { t, locale } = useI18n();
  const D = briefDict(locale);
  const [items, setItems] = useState<TaskBrief[]>([]);
  const [total, setTotal] = useState(0);
  const [nextToken, setNextToken] = useState("");
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [loadError, setLoadError] = useState("");
  const [view, setView] = useState<BriefPeriodView>("issue");
  const [reports, setReports] = useState<PeriodicBriefReport[]>([]);
  const [reportsLoading, setReportsLoading] = useState(false);
  const [reportsNextCursor, setReportsNextCursor] = useState("");
  const [reportsLoadingMore, setReportsLoadingMore] = useState(false);
  const [settings, setSettings] = useState<BriefReportSettings | null>(null);
  const [p2dAvailable, setP2dAvailable] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
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
    setView("issue");
    setReports([]);
    setReportsNextCursor("");
    setSettings(null);
    setP2dAvailable(false);
    api.reportSettings(scheduleID).then((value) => {
      if (isCurrent()) {
        setSettings(value);
        setP2dAvailable(true);
      }
    }).catch((error) => {
      if (isCurrent()) {
        if (error instanceof ApiError && error.status === 404) {
          setP2dAvailable(false);
          return;
        }
        setLoadError(
          error instanceof ApiError ? error.message : t.app.common.loadFailed,
        );
      }
    });
    api
      .scheduleBriefs(scheduleID, PAGE_SIZE)
      .then((page) => {
        if (!isCurrent()) return;
        setItems(page.items);
        setTotal(page.total);
        setNextToken(page.next_page_token ?? "");
        setLoadError("");
        onLatestCheck?.(page.latest_check);
        onHealth?.(page.health);
      })
      .catch((error) => {
        if (!isCurrent()) return;
        setItems([]);
        setTotal(0);
        setNextToken("");
        onHealth?.(undefined);
        setLoadError(
          error instanceof ApiError ? error.message : t.app.common.loadFailed,
        );
      })
      .finally(() => isCurrent() && setLoading(false));
    return () => {
      alive = false;
    };
    // Projection callbacks are notification sinks; refetch is task-bound only.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scheduleID]);

  useEffect(() => {
    const query = window.location.hash.split("?", 2)[1] ?? "";
    if (new URLSearchParams(query).get("brief_action") !== "deep_dive") {
      return;
    }
    requestAnimationFrame(() => {
      const input = document.querySelector<HTMLInputElement>(
        "[data-grounded-followup] input",
      );
      input?.scrollIntoView({ block: "center", behavior: "smooth" });
      input?.focus();
    });
  }, [items, reports]);

  useEffect(() => {
    if (view === "issue" || !p2dAvailable) return;
    let alive = true;
    setReportsLoading(true);
    setReportsNextCursor("");
    api.scheduleReports(scheduleID, view)
      .then((page) => {
        if (!alive) return;
        setReports(page.items);
        setReportsNextCursor(page.next_cursor ?? "");
      })
      .catch((error) => {
        if (alive) {
          setLoadError(
            error instanceof ApiError ? error.message : t.app.common.loadFailed,
          );
        }
      })
      .finally(() => alive && setReportsLoading(false));
    return () => {
      alive = false;
    };
  }, [p2dAvailable, scheduleID, view, t.app.common.loadFailed]);

  async function loadMoreReports() {
    if (
      view === "issue" ||
      !reportsNextCursor ||
      reportsLoadingMore
    ) return;
    setReportsLoadingMore(true);
    try {
      const page = await api.scheduleReports(
        scheduleID, view, PAGE_SIZE, reportsNextCursor,
      );
      setReports((current) => current.concat(page.items));
      setReportsNextCursor(page.next_cursor ?? "");
    } catch (error) {
      setLoadError(
        error instanceof ApiError ? error.message : t.app.common.loadFailed,
      );
    } finally {
      setReportsLoadingMore(false);
    }
  }

  async function updateSettings(
    patch: Partial<Pick<BriefReportSettings, "mode" | "cadence" | "delivery">>,
  ) {
    try {
      const next = await api.patchReportSettings(scheduleID, patch);
      setSettings(next);
      setLoadError("");
    } catch (error) {
      setLoadError(
        error instanceof ApiError ? error.message : t.app.common.loadFailed,
      );
    }
  }

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
      onHealth?.(page.health);
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

  const availableViews: readonly BriefPeriodView[] = p2dAvailable
    ? ["issue", "daily", "weekly", "monthly"]
    : ["issue"];

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div
          className="inline-flex rounded-lg border bg-background p-1"
          role="tablist"
          aria-label="Brief period"
        >
          {availableViews.map((value) => (
            <Button
              key={value}
              id={`brief-tab-${value}`}
              role="tab"
              aria-selected={view === value}
              aria-controls={`brief-panel-${value}`}
              tabIndex={view === value ? 0 : -1}
              variant={view === value ? "secondary" : "ghost"}
              size="sm"
              onClick={() => setView(value)}
              onKeyDown={(event) => {
                if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") {
                  return;
                }
                event.preventDefault();
                const direction = event.key === "ArrowRight" ? 1 : -1;
                const index =
                  (availableViews.indexOf(value) + direction +
                    availableViews.length) %
                  availableViews.length;
                const next = availableViews[index];
                setView(next);
                requestAnimationFrame(() =>
                  document.getElementById(`brief-tab-${next}`)?.focus(),
                );
              }}
            >
              {executiveCopy(locale)[value]}
            </Button>
          ))}
        </div>
        {p2dAvailable && (
          <Button
            variant="outline"
            size="sm"
            onClick={() => setSettingsOpen((open) => !open)}
            aria-expanded={settingsOpen}
          >
            <Settings2 className="mr-2 size-4" />
            {executiveCopy(locale).settings}
          </Button>
        )}
      </div>
      {settingsOpen && settings && (
        <Card>
          <CardContent className="grid gap-3 py-4 sm:grid-cols-3">
            <label className="space-y-1 text-xs text-muted-foreground">
              <span>{executiveCopy(locale).settings}</span>
              <select
                className="h-9 w-full rounded-md border bg-background px-2 text-sm text-foreground"
                value={settings.mode}
                onChange={(event) =>
                  void updateSettings({
                    mode: event.target.value as BriefReportSettings["mode"],
                  })
                }
              >
                <option value="auto">{executiveCopy(locale).auto}</option>
                <option value="manual">{executiveCopy(locale).manual}</option>
              </select>
            </label>
            <label className="space-y-1 text-xs text-muted-foreground">
              <span>{settings.timezone}</span>
              <select
                className="h-9 w-full rounded-md border bg-background px-2 text-sm text-foreground"
                value={settings.cadence}
                onChange={(event) =>
                  void updateSettings({
                    cadence: event.target.value as BriefReportSettings["cadence"],
                  })
                }
              >
                <option value="daily">{executiveCopy(locale).daily}</option>
                <option value="weekly">{executiveCopy(locale).weekly}</option>
                <option value="monthly">{executiveCopy(locale).monthly}</option>
              </select>
            </label>
            <label className="space-y-1 text-xs text-muted-foreground">
              <span>{executiveCopy(locale).delivery}</span>
              <select
                className="h-9 w-full rounded-md border bg-background px-2 text-sm text-foreground"
                value={settings.delivery}
                onChange={(event) =>
                  void updateSettings({
                    delivery: event.target.value as BriefReportSettings["delivery"],
                  })
                }
              >
                <option value="important">{executiveCopy(locale).important}</option>
                <option value="always">{executiveCopy(locale).always}</option>
                <option value="web_only">{executiveCopy(locale).webOnly}</option>
              </select>
            </label>
          </CardContent>
        </Card>
      )}
      {loadError && (
        <Alert variant="destructive">
          <AlertDescription>{loadError}</AlertDescription>
        </Alert>
      )}
      <div
        id={`brief-panel-${view}`}
        role="tabpanel"
        aria-labelledby={`brief-tab-${view}`}
      >
      {view !== "issue" ? (
        reportsLoading ? (
          <Card>
            <CardContent className="space-y-3 py-6">
              <Skeleton className="h-6 w-2/3" />
              <Skeleton className="h-20 w-full" />
            </CardContent>
          </Card>
        ) : reports.length === 0 ? (
          <Card>
            <CardContent className="py-12 text-center text-muted-foreground">
              {executiveCopy(locale).noReports}
            </CardContent>
          </Card>
        ) : (
          <>
            {reports.map((report) => (
              <PeriodicReportCard
                key={report.id}
                report={report}
                locale={locale}
                scheduleID={scheduleID}
                onAdjustTask={onAdjustTask}
                onCreateTask={onCreateTask}
              />
            ))}
            {reportsNextCursor && (
              <div className="flex justify-end">
                <Button
                  variant="outline"
                  size="sm"
                  disabled={reportsLoadingMore}
                  onClick={() => void loadMoreReports()}
                >
                  {reportsLoadingMore && (
                    <Loader2 className="mr-2 size-4 animate-spin" />
                  )}
                  {t.app.common.loadMore}
                </Button>
              </div>
            )}
          </>
        )
      ) : items.length === 0 ? (
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
                {brief.executive && (
                  <ExecutivePanel
                    content={brief.executive.content}
                    locale={locale}
                    partial={
                      brief.source_coverage === "partial" ||
                      brief.processing === "partial" ||
                      brief.executive.processing === "partial"
                    }
                    fallback={
                      brief.executive.generation_mode ===
                      "deterministic_fallback"
                    }
                    scheduleID={scheduleID}
                    target={{ kind: "brief", id: brief.id }}
                    onAdjustTask={onAdjustTask}
                    onCreateTask={onCreateTask}
                  />
                )}
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
    </div>
  );
}

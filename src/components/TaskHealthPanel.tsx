import { Activity, CircleDollarSign, ShieldCheck } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";

export type TaskHealthState =
  | "healthy"
  | "attention"
  | "waiting"
  | "never_run";
export type TaskHealthIssue =
  | "coverage_incomplete"
  | "sources_unavailable"
  | "quota_paused"
  | "model_temporarily_unavailable"
  | "delivery_failed"
  | "check_interrupted"
  | "check_failed";
export type TaskHealthAction =
  | "wait_for_retry"
  | "review_sources"
  | "review_usage"
  | "review_delivery"
  | "run_again"
  | "contact_support";
export type CostCoverage =
  | "none"
  | "llm_only"
  | "tools_only"
  | "llm_and_tools";
export type BudgetState =
  | "not_configured"
  | "ok"
  | "warning"
  | "exhausted"
  | "incomplete";

export interface TaskHealthProjection {
  schema_version: "vane.task-health/v1";
  state: TaskHealthState;
  issue?: TaskHealthIssue;
  recommended_action?: TaskHealthAction;
  usage?: {
    known_cost_usd: number;
    coverage: CostCoverage;
    budget_state: BudgetState;
  };
  permissions: {
    role: "owner" | "admin" | "member" | "";
    can_run: boolean;
    can_pause: boolean;
    can_edit: boolean;
    can_delete: boolean;
    can_view_usage: boolean;
  };
}

export interface TaskHealthCopy {
  title: string;
  usageTitle: string;
  accessTitle: string;
  knownCost: string;
  states: Record<TaskHealthState, string>;
  issues: Record<TaskHealthIssue, string>;
  actions: Record<TaskHealthAction, string>;
  coverage: Record<CostCoverage, string>;
  budget: Record<BudgetState, string>;
  roles: Record<"owner" | "admin" | "member" | "unknown", string>;
  allowedActions: string;
  capabilities: {
    run: string;
    pause: string;
    edit: string;
    delete: string;
    viewUsage: string;
  };
  readOnly: string;
  usageUnavailable: string;
}

function actionAllowed(
  action: TaskHealthAction,
  permissions: TaskHealthProjection["permissions"],
): boolean {
  if (action === "run_again") return permissions.can_run;
  if (action === "review_usage") return permissions.can_view_usage;
  return true;
}

export default function TaskHealthPanel({
  health,
  copy,
  locale,
  onAction,
}: {
  health: TaskHealthProjection;
  copy: TaskHealthCopy;
  locale: string;
  onAction?: (action: TaskHealthAction) => void;
}) {
  const role = health.permissions.role || "unknown";
  const allowedCapabilities = [
    health.permissions.can_run && copy.capabilities.run,
    health.permissions.can_pause && copy.capabilities.pause,
    health.permissions.can_edit && copy.capabilities.edit,
    health.permissions.can_delete && copy.capabilities.delete,
    health.permissions.can_view_usage && copy.capabilities.viewUsage,
  ].filter((value): value is string => Boolean(value));
  const cost = new Intl.NumberFormat(locale, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 4,
  }).format(health.usage?.known_cost_usd ?? 0);

  return (
    <section aria-labelledby="task-health-title" className="grid gap-3 lg:grid-cols-3">
      <h2 id="task-health-title" className="sr-only">
        {copy.title}
      </h2>
      <Card>
        <CardContent className="space-y-2 p-4">
          <div className="flex items-center justify-between gap-2">
            <span className="flex items-center gap-2 text-sm font-medium">
              <Activity className="size-4" />
              {copy.title}
            </span>
            <Badge
              variant={health.state === "attention" ? "destructive" : "outline"}
            >
              {copy.states[health.state]}
            </Badge>
          </div>
          {health.issue && (
            <p className="text-sm text-muted-foreground">
              {copy.issues[health.issue]}
            </p>
          )}
          {health.recommended_action &&
            onAction &&
            actionAllowed(health.recommended_action, health.permissions) && (
            <Button
              variant="outline"
              size="sm"
              onClick={() => onAction(health.recommended_action!)}
            >
              {copy.actions[health.recommended_action]}
            </Button>
          )}
        </CardContent>
      </Card>
      <Card>
        <CardContent className="space-y-2 p-4">
          <span className="flex items-center gap-2 text-sm font-medium">
            <CircleDollarSign className="size-4" />
            {copy.usageTitle}
          </span>
          {health.permissions.can_view_usage && health.usage ? (
            <>
              <p className="text-lg font-semibold">
                {copy.knownCost}: {cost}
              </p>
              <p className="text-xs text-muted-foreground">
                {copy.coverage[health.usage.coverage]} ·{" "}
                {copy.budget[health.usage.budget_state]}
              </p>
            </>
          ) : (
            <p className="text-sm text-muted-foreground">
              {health.permissions.can_view_usage
                ? copy.usageUnavailable
                : copy.readOnly}
            </p>
          )}
        </CardContent>
      </Card>
      <Card>
        <CardContent className="space-y-2 p-4">
          <span className="flex items-center gap-2 text-sm font-medium">
            <ShieldCheck className="size-4" />
            {copy.accessTitle}
          </span>
          <p className="text-sm">{copy.roles[role]}</p>
          <p className="text-xs text-muted-foreground">
            {allowedCapabilities.length > 0
              ? `${copy.allowedActions}: ${allowedCapabilities.join(" · ")}`
              : copy.readOnly}
          </p>
        </CardContent>
      </Card>
    </section>
  );
}

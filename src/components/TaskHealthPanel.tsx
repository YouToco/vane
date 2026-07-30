import { Activity, CircleDollarSign, ShieldCheck } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import type {
  AcquisitionFailure,
  BudgetState,
  CostCoverage,
  TaskHealthAction,
  TaskHealthIssue,
  TaskHealthProjection,
  TaskHealthState,
} from "@/api";

export type {
  BudgetState,
  CostCoverage,
  TaskHealthAction,
  TaskHealthIssue,
  TaskHealthProjection,
  TaskHealthState,
} from "@/api";

export interface TaskHealthCopy {
  title: string;
  usageTitle: string;
  accessTitle: string;
  knownCost: string;
  latestAcquisition: string;
  modelCalls: string;
  acquisitionCalls: string;
  pricedCalls: string;
  recommended: string;
  states: Record<TaskHealthState, string>;
  issues: Record<TaskHealthIssue, string>;
  actions: Record<TaskHealthAction, string>;
  coverage: Record<CostCoverage, string>;
  acquisitionFailures: Record<AcquisitionFailure, string>;
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
  if (action === "review_task") return permissions.can_edit;
  return false;
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
          {health.acquisition.failure_reason && (
            <p className="text-xs text-muted-foreground">
              {copy.latestAcquisition}:{" "}
              {copy.acquisitionFailures[
                health.acquisition.failure_reason
              ]}
            </p>
          )}
          {health.recommended_action &&
          onAction &&
          actionAllowed(health.recommended_action, health.permissions) ? (
            <Button
              variant="outline"
              size="sm"
              onClick={() => onAction(health.recommended_action!)}
            >
              {copy.actions[health.recommended_action]}
            </Button>
          ) : health.recommended_action ? (
            <p className="text-xs text-muted-foreground">
              {copy.recommended}:{" "}
              {copy.actions[health.recommended_action]}
            </p>
          ) : null}
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
              <p className="text-xs text-muted-foreground">
                {health.usage.llm_calls !== undefined &&
                  `${copy.modelCalls} ${health.usage.llm_calls}`}
                {health.usage.llm_calls !== undefined &&
                  health.usage.tool_calls !== undefined &&
                  " · "}
                {health.usage.tool_calls !== undefined &&
                  `${copy.acquisitionCalls} ${health.usage.tool_calls}`}
                {health.usage.tool_priced_calls !== undefined &&
                  health.usage.tool_calls !== undefined &&
                  health.usage.tool_priced_calls <
                    health.usage.tool_calls &&
                  ` (${copy.pricedCalls} ${health.usage.tool_priced_calls})`}
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

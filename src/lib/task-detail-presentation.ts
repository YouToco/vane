import type { TaskLatestCheck } from "@/api";

export type TaskRunOutcome =
  | "completed"
  | "no_important_change"
  | "incomplete"
  | "not_run";

export function taskRunOutcome(status: string): TaskRunOutcome {
  if (!status) return "not_run";
  if (status === "done") return "completed";
  if (status === "empty") return "no_important_change";
  return "incomplete";
}

export function canonicalCheckOutcome(
  check: TaskLatestCheck,
): TaskRunOutcome {
  if (
    check.result === "quiet" &&
    check.source_coverage === "complete" &&
    check.processing === "complete"
  ) {
    return "no_important_change";
  }
  if (
    check.result === "content" &&
    check.source_coverage === "complete" &&
    check.processing === "complete"
  ) {
    return "completed";
  }
  return "incomplete";
}

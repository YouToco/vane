import type { DeliveryHistoryItem, TaskLatestCheck } from "@/api";

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

// The task feed is ordered and paged by the row creation cursor. Showing sent_at
// here would make the visible time disagree with that ordering.
export function taskContentTimestamp(
  item: Pick<DeliveryHistoryItem, "created_at" | "sent_at">,
): string {
  return item.created_at;
}

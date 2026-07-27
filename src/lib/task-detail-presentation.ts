import type { DeliveryHistoryItem } from "@/api";

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

// The task feed is ordered and paged by the row creation cursor. Showing sent_at
// here would make the visible time disagree with that ordering.
export function taskContentTimestamp(
  item: Pick<DeliveryHistoryItem, "created_at" | "sent_at">,
): string {
  return item.created_at;
}

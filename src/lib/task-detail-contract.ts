import type { Schedule, ScheduleDetail } from "@/api";

export function taskDefinitionEditEnabled(detail: ScheduleDetail): boolean {
  return detail.capabilities.definition_edit === true;
}

export type NextRunPresentation =
  | { kind: "scheduled"; at: string }
  | { kind: "paused" | "none" | "unavailable" };

export function nextRunPresentation(
  schedule: Schedule,
): NextRunPresentation {
  if (schedule.next_run_state === "scheduled" && schedule.next_run) {
    return { kind: "scheduled", at: schedule.next_run };
  }
  if (
    schedule.next_run_state === "paused" ||
    schedule.next_run_state === "none"
  ) {
    return { kind: schedule.next_run_state };
  }
  return { kind: "unavailable" };
}

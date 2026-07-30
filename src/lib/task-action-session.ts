export const TASK_ACTION_STORAGE_PREFIX = "vane.task-action.v1";
export const TASK_EXECUTION_STORAGE_PREFIX = "vane.task-execution.v2";
export const SCHEDULE_COMMAND_STORAGE_PREFIX = "vane.schedule-command.v1";

// Logout also removes data written by the retired proposal/confirmation flow.
// No current write path may import this historical prefix.
const LEGACY_TASK_PROPOSAL_STORAGE_PREFIX = "vane.task-proposal.v1";

const TASK_MUTATION_PREFIXES = [
  TASK_ACTION_STORAGE_PREFIX,
  TASK_EXECUTION_STORAGE_PREFIX,
  SCHEDULE_COMMAND_STORAGE_PREFIX,
  LEGACY_TASK_PROPOSAL_STORAGE_PREFIX,
];

export function clearTaskMutationSessionStorage(
  storage: Pick<Storage, "key" | "length" | "removeItem"> = window.sessionStorage,
): void {
  const removals: string[] = [];
  for (let index = 0; index < storage.length; index += 1) {
    const key = storage.key(index);
    if (
      key &&
      TASK_MUTATION_PREFIXES.some((prefix) => key.startsWith(`${prefix}:`))
    ) {
      removals.push(key);
    }
  }
  for (const key of removals) storage.removeItem(key);
}

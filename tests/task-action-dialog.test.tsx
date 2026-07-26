import React from "react";
import {
  act,
  create,
  type ReactTestRenderer,
} from "react-test-renderer";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

const apiMock = vi.hoisted(() => ({
  proposeTaskAction: vi.fn(),
  confirmTaskAction: vi.fn(),
  cancelTaskAction: vi.fn(),
  taskActionStatus: vi.fn(),
}));

vi.mock("@/api", () => ({
  api: apiMock,
  ApiError: class ApiError extends Error {
    status: number;
    constructor(status: number, message: string) {
      super(message);
      this.status = status;
    }
  },
}));

vi.mock("@/components/ui/dialog", () => ({
  Dialog: ({
    open,
    onOpenChange,
    children,
  }: {
    open: boolean;
    onOpenChange: (open: boolean) => void;
    children: React.ReactNode;
  }) =>
    open ? (
      <mock-dialog>
        <button
          aria-label="dismiss"
          onClick={() => onOpenChange(false)}
        />
        {children}
      </mock-dialog>
    ) : null,
  DialogContent: ({ children }: { children: React.ReactNode }) => (
    <mock-content>{children}</mock-content>
  ),
  DialogDescription: ({ children }: { children: React.ReactNode }) => (
    <mock-description>{children}</mock-description>
  ),
  DialogFooter: ({ children }: { children: React.ReactNode }) => (
    <mock-footer>{children}</mock-footer>
  ),
  DialogHeader: ({ children }: { children: React.ReactNode }) => (
    <mock-header>{children}</mock-header>
  ),
  DialogTitle: ({ children }: { children: React.ReactNode }) => (
    <mock-title>{children}</mock-title>
  ),
}));

vi.mock("@/components/ui/alert", () => ({
  Alert: ({ children }: { children: React.ReactNode }) => (
    <mock-alert>{children}</mock-alert>
  ),
  AlertDescription: ({ children }: { children: React.ReactNode }) => (
    <mock-alert-description>{children}</mock-alert-description>
  ),
}));

vi.mock("@/components/ui/button", () => ({
  Button: (props: React.ButtonHTMLAttributes<HTMLButtonElement>) => (
    <button {...props} />
  ),
}));

vi.mock("@/components/ui/input", () => ({
  Input: (props: React.InputHTMLAttributes<HTMLInputElement>) => (
    <input {...props} />
  ),
}));

import TaskActionDialog, {
  type TaskActionDialogLabels,
} from "@/components/TaskActionDialog";
import { ApiError, type TaskActionStatus } from "@/api";
import {
  canonicalTaskActionPayload,
  normalizeTaskActionField,
  taskActionPayloadHash,
} from "@/lib/task-action-canonical";
import { clearTaskMutationSessionStorage } from "@/lib/task-action-session";

const DEFAULT_ACTOR = "1:11";
const storageKey = (actor = DEFAULT_ACTOR) =>
  `vane.task-action.v1:${encodeURIComponent(actor)}:create`;

const labels: TaskActionDialogLabels = {
  title: "New task",
  description: "Describe it",
  placeholder: "What to track",
  inputLabel: "Task request",
  draft: "Draft",
  drafting: "Drafting",
  preview: "Preview",
  confirm: "Confirm",
  confirming: "Confirming",
  cancel: "Cancel",
  close: "Done",
  waiting: "Still processing",
  checkAgain: "Check again",
  requestFailed: "Localized request failed",
  resultStatus: "Status",
  invalidProposal: "Proposal scope mismatch",
  status: {
    accepted: "Accepted",
    pending: "Awaiting confirmation",
    executing: "In progress",
    executed: "Created",
    completed: "Completed",
    cancelled: "Cancelled",
    expired: "Expired",
    blocked: "Blocked",
    failed: "Failed",
    superseded: "Superseded",
  },
};

class MemoryStorage {
  private values = new Map<string, string>();

  getItem(key: string) {
    return this.values.get(key) ?? null;
  }

  setItem(key: string, value: string) {
    this.values.set(key, value);
  }

  removeItem(key: string) {
    this.values.delete(key);
  }

  clear() {
    this.values.clear();
  }

  key(index: number) {
    return Array.from(this.values.keys())[index] ?? null;
  }

  get length() {
    return this.values.size;
  }
}

function storedPending(id: string) {
  return JSON.stringify({
    version: 1,
    id,
    kind: "create",
    status: "accepted",
    terminal: false,
    notified: false,
  });
}

function textOf(renderer: ReactTestRenderer): string {
  return JSON.stringify(renderer.toJSON());
}

async function flushEffects() {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
}

function renderDialog(
  props: {
    open?: boolean;
    actorScope?: string;
    taskID?: string;
    onClose?: (status?: TaskActionStatus) => void;
    onComplete?: (status: TaskActionStatus) => void;
  } = {},
) {
  return create(
    <TaskActionDialog
      open={props.open ?? true}
      actorScope={props.actorScope ?? DEFAULT_ACTOR}
      taskID={props.taskID}
      labels={labels}
      onClose={props.onClose ?? vi.fn()}
      onComplete={props.onComplete ?? vi.fn()}
    />,
  );
}

async function draftAction(
  renderer: ReactTestRenderer,
  text: string,
): Promise<void> {
  const input = renderer.root.findByType("input");
  await act(async () => {
    input.props.onChange({ target: { value: text } });
  });
  const draft = renderer.root
    .findAllByType("button")
    .find((node) => node.children.includes("Draft"));
  await act(async () => {
    draft?.props.onClick();
    await Promise.resolve();
    await Promise.resolve();
  });
}

describe("TaskActionDialog durable lifecycle", () => {
  let sessionStorage: MemoryStorage;

  beforeEach(() => {
    vi.useFakeTimers();
    vi.clearAllMocks();
    sessionStorage = new MemoryStorage();
    Object.defineProperty(globalThis, "window", {
      configurable: true,
      value: { sessionStorage },
    });
    Object.defineProperty(globalThis, "IS_REACT_ACT_ENVIRONMENT", {
      configurable: true,
      value: true,
    });
  });

  afterEach(() => {
    vi.useRealTimers();
    Reflect.deleteProperty(globalThis, "window");
    Reflect.deleteProperty(globalThis, "IS_REACT_ACT_ENVIRONMENT");
  });

  test("keeps a hidden terminal failure visible until acknowledgement", async () => {
    sessionStorage.setItem(storageKey(), storedPending("action-failed"));
    apiMock.taskActionStatus.mockResolvedValue({
      id: "action-failed",
      kind: "create",
      status: "failed",
      terminal: true,
      message: "The durable action failed",
    });
    const onComplete = vi.fn();

    let renderer: ReactTestRenderer;
    await act(async () => {
      renderer = renderDialog({ open: false, onComplete });
    });
    await flushEffects();

    expect(textOf(renderer!)).toContain("The durable action failed");
    expect(textOf(renderer!)).toContain("failed");
    expect(onComplete).toHaveBeenCalledTimes(1);
    expect(sessionStorage.getItem(storageKey())).not.toBeNull();

    const done = renderer!.root
      .findAllByType("button")
      .find((node) => node.children.includes("Done"));
    await act(async () => done?.props.onClick());
    expect(sessionStorage.getItem(storageKey())).toBeNull();
  });

  test("continues after the 120 second window and resumes immediately when reopened", async () => {
    sessionStorage.setItem(storageKey(), storedPending("action-slow"));
    apiMock.taskActionStatus.mockResolvedValue({
      id: "action-slow",
      kind: "create",
      status: "executing",
      terminal: false,
    });

    let renderer: ReactTestRenderer;
    await act(async () => {
      renderer = renderDialog();
    });
    for (
      let attempt = 0;
      attempt < 90 && !textOf(renderer!).includes("Still processing");
      attempt += 1
    ) {
      await act(async () => {
        await vi.advanceTimersByTimeAsync(1500);
      });
    }
    expect(apiMock.taskActionStatus.mock.calls.length).toBeGreaterThanOrEqual(80);
    expect(textOf(renderer!)).toContain("Still processing");

    await act(async () => {
      renderer!.update(
        <TaskActionDialog
          open={false}
          actorScope={DEFAULT_ACTOR}
          labels={labels}
          onClose={vi.fn()}
          onComplete={vi.fn()}
        />,
      );
    });
    apiMock.taskActionStatus.mockResolvedValue({
      id: "action-slow",
      kind: "create",
      status: "executed",
      terminal: true,
      task_id: "task-42",
      message: "Created",
    });
    await act(async () => {
      renderer!.update(
        <TaskActionDialog
          open
          actorScope={DEFAULT_ACTOR}
          labels={labels}
          onClose={vi.fn()}
          onComplete={vi.fn()}
        />,
      );
    });
    await flushEffects();

    expect(textOf(renderer!)).toContain("Created");
    expect(textOf(renderer!)).toContain(labels.status.executed);
  });

  test("recovers an accepted action after unmount and reload", async () => {
    sessionStorage.setItem(storageKey(), storedPending("action-reload"));
    apiMock.taskActionStatus.mockResolvedValueOnce({
      id: "action-reload",
      kind: "create",
      status: "executing",
      terminal: false,
    });

    let first: ReactTestRenderer;
    await act(async () => {
      first = renderDialog({ open: false });
    });
    await flushEffects();
    await act(async () => first!.unmount());

    const onComplete = vi.fn();
    apiMock.taskActionStatus.mockResolvedValue({
      id: "action-reload",
      kind: "create",
      status: "executed",
      terminal: true,
      task_id: "task-reloaded",
      message: "Recovered creation",
    });
    let reloaded: ReactTestRenderer;
    await act(async () => {
      reloaded = renderDialog({ open: false, onComplete });
    });
    await flushEffects();

    expect(textOf(reloaded!)).toContain("Recovered creation");
    expect(onComplete).toHaveBeenCalledTimes(1);
    const persisted = sessionStorage.getItem(storageKey()) ?? "";
    expect(persisted).not.toContain("Recovered creation");
    expect(persisted).not.toContain("proposal");
  });

  test("clears an actor-scoped stale action on authoritative 404", async () => {
    sessionStorage.setItem(storageKey(), storedPending("stale-action"));
    apiMock.taskActionStatus.mockRejectedValue(
      new ApiError(404, "Action not found"),
    );
    let renderer: ReactTestRenderer;
    await act(async () => {
      renderer = renderDialog();
    });
    await flushEffects();

    expect(sessionStorage.getItem(storageKey())).toBeNull();
    expect(renderer!.root.findByType("input")).toBeDefined();
    expect(textOf(renderer!)).toContain("Action not found");
  });

  test("restores proposal controls when status proves the mutation was not accepted", async () => {
    sessionStorage.setItem(storageKey(), storedPending("still-pending"));
    apiMock.taskActionStatus.mockResolvedValue({
      id: "still-pending",
      kind: "create",
      status: "pending",
      terminal: false,
      summary: "Proposal remains available",
      message: "Awaiting confirmation",
    });
    let renderer: ReactTestRenderer;
    await act(async () => {
      renderer = renderDialog();
    });
    await flushEffects();

    expect(textOf(renderer!)).toContain("Proposal remains available");
    expect(
      renderer!.root
        .findAllByType("button")
        .some((node) => node.children.includes("Confirm")),
    ).toBe(true);
    expect(sessionStorage.getItem(storageKey())).not.toBeNull();
  });

  test.each([
    {
      mutation: "confirm",
      actionID: "action-confirm-lost",
      terminalStatus: "executed",
      terminalMessage: "Recovered after lost confirm response",
    },
    {
      mutation: "cancel",
      actionID: "action-cancel-lost",
      terminalStatus: "cancelled",
      terminalMessage: "Recovered after lost cancel response",
    },
  ])(
    "persists the action before a lost $mutation response and recovers after reload",
    async ({
      mutation,
      actionID,
      terminalStatus,
      terminalMessage,
    }) => {
      apiMock.proposeTaskAction.mockResolvedValue({
        reply: "Review this proposal",
        action: {
          id: actionID,
          kind: "create",
          summary: "Durable proposal",
        },
      });
      const mutate =
        mutation === "confirm"
          ? apiMock.confirmTaskAction
          : apiMock.cancelTaskAction;
      mutate.mockRejectedValue(new Error("response lost"));
      apiMock.taskActionStatus.mockImplementationOnce(
        () => new Promise(() => {}),
      );

      let first: ReactTestRenderer;
      await act(async () => {
        first = renderDialog();
      });
      await draftAction(first!, "Track official AI updates");
      const buttonLabel = mutation === "confirm" ? "Confirm" : "Cancel";
      const mutateButton = first!.root
        .findAllByType("button")
        .find((node) => node.children.includes(buttonLabel));
      await act(async () => {
        mutateButton?.props.onClick();
        await Promise.resolve();
        await Promise.resolve();
      });

      const persistedBeforeResponse =
        sessionStorage.getItem(storageKey()) ?? "";
      expect(persistedBeforeResponse).toContain(actionID);
      expect(persistedBeforeResponse).not.toContain("Durable proposal");
      await act(async () => first!.unmount());

      apiMock.taskActionStatus.mockReset();
      apiMock.taskActionStatus.mockResolvedValue({
        id: actionID,
        kind: "create",
        status: terminalStatus,
        terminal: true,
        ...(terminalStatus === "executed" ? { task_id: "task-recovered" } : {}),
        message: terminalMessage,
      });
      const onComplete = vi.fn();
      let reloaded: ReactTestRenderer;
      await act(async () => {
        reloaded = renderDialog({ open: false, onComplete });
      });
      await flushEffects();

      expect(textOf(reloaded!)).toContain(terminalMessage);
      expect(textOf(reloaded!)).toContain(labels.status[terminalStatus]);
      expect(onComplete).toHaveBeenCalledTimes(1);
    },
  );

  test("notifies completion exactly once across rerenders and remounts", async () => {
    sessionStorage.setItem(storageKey(), storedPending("action-once"));
    apiMock.taskActionStatus.mockResolvedValue({
      id: "action-once",
      kind: "create",
      status: "cancelled",
      terminal: true,
      message: "Cancelled",
    });
    const onComplete = vi.fn();

    let first: ReactTestRenderer;
    await act(async () => {
      first = renderDialog({ onComplete });
    });
    await flushEffects();
    await act(async () => {
      first!.update(
        <TaskActionDialog
          open={false}
          actorScope={DEFAULT_ACTOR}
          labels={labels}
          onClose={vi.fn()}
          onComplete={onComplete}
        />,
      );
      first!.update(
        <TaskActionDialog
          open
          actorScope={DEFAULT_ACTOR}
          labels={labels}
          onClose={vi.fn()}
          onComplete={onComplete}
        />,
      );
    });
    expect(onComplete).toHaveBeenCalledTimes(1);
    await act(async () => first!.unmount());

    let second: ReactTestRenderer;
    await act(async () => {
      second = renderDialog({ open: false, onComplete });
    });
    await flushEffects();

    expect(textOf(second!)).toContain("Cancelled");
    expect(onComplete).toHaveBeenCalledTimes(1);
  });

  test("gives the free-text input a localized accessible name", async () => {
    let renderer: ReactTestRenderer;
    await act(async () => {
      renderer = renderDialog();
    });
    const input = renderer!.root.findByType("input");
    expect(input.props["aria-label"]).toBe("Task request");
  });

  test.each([
    {
      name: "create receives edit kind",
      taskID: undefined,
      action: {
        id: "wrong-kind",
        kind: "edit",
        summary: "Wrong kind",
      },
    },
    {
      name: "create receives a task scope",
      taskID: undefined,
      action: {
        id: "wrong-create-scope",
        kind: "create",
        task_id: "task-other",
        summary: "Wrong create scope",
      },
    },
    {
      name: "edit receives another task",
      taskID: "task-1",
      action: {
        id: "wrong-edit-scope",
        kind: "edit",
        task_id: "task-other",
        summary: "Wrong edit scope",
      },
    },
  ])("fails closed when $name", async ({ taskID, action }) => {
    apiMock.proposeTaskAction.mockResolvedValue({
      reply: "Untrusted reply",
      action,
    });
    let renderer: ReactTestRenderer;
    await act(async () => {
      renderer = renderDialog({ taskID });
    });
    await draftAction(renderer!, "Track official AI updates");

    expect(textOf(renderer!)).toContain("Proposal scope mismatch");
    expect(textOf(renderer!)).not.toContain(action.summary);
    expect(
      renderer!.root
        .findAllByType("button")
        .some((node) => node.children.includes("Confirm")),
    ).toBe(false);
    for (let index = 0; index < sessionStorage.length; index += 1) {
      expect(sessionStorage.key(index)).not.toContain("vane.task-action.v1");
    }
    expect(sessionStorage.length).toBe(0);
  });

  test("isolates recovery slots by actor and logout clears only mutation state", async () => {
    const actorA = "1:101";
    const actorB = "1:202";
    sessionStorage.setItem(storageKey(actorA), storedPending("actor-a-action"));
    sessionStorage.setItem(
      `vane.task-proposal.v1:${encodeURIComponent(actorA)}:create`,
      "{}",
    );
    sessionStorage.setItem(
      `vane.schedule-command.v1:${encodeURIComponent(actorA)}:task-1:run`,
      "command-a",
    );
    sessionStorage.setItem("vane.locale", "en");
    apiMock.taskActionStatus.mockImplementation(() => new Promise(() => {}));

    let first: ReactTestRenderer;
    await act(async () => {
      first = renderDialog({ actorScope: actorA, open: false });
    });
    await flushEffects();
    expect(apiMock.taskActionStatus).toHaveBeenCalledWith("actor-a-action");
    await act(async () => first!.unmount());
    const callsBeforeActorB = apiMock.taskActionStatus.mock.calls.length;

    let second: ReactTestRenderer;
    await act(async () => {
      second = renderDialog({ actorScope: actorB });
    });
    await flushEffects();
    expect(second!.root.findByType("input")).toBeDefined();
    expect(textOf(second!)).not.toContain("actor-a-action");
    expect(apiMock.taskActionStatus).toHaveBeenCalledTimes(callsBeforeActorB);

    clearTaskMutationSessionStorage(sessionStorage);
    expect(sessionStorage.getItem(storageKey(actorA))).toBeNull();
    expect(
      sessionStorage.getItem(
        `vane.task-proposal.v1:${encodeURIComponent(actorA)}:create`,
      ),
    ).toBeNull();
    expect(sessionStorage.getItem("vane.locale")).toBe("en");
  });

  test("maps machine status inside a polite live region", async () => {
    sessionStorage.setItem(storageKey(), storedPending("action-live"));
    apiMock.taskActionStatus.mockResolvedValue({
      id: "action-live",
      kind: "create",
      status: "executing",
      terminal: false,
    });
    let renderer: ReactTestRenderer;
    await act(async () => {
      renderer = renderDialog();
    });
    await flushEffects();

    expect(textOf(renderer!)).toContain("In progress");
    const liveRegion = renderer!.root.find(
      (node) => node.props.role === "status",
    );
    expect(liveRegion.props["aria-live"]).toBe("polite");
  });

  test("matches the backend canonical payload digest vector", async () => {
    apiMock.proposeTaskAction.mockResolvedValue({ reply: "Drafted" });
    let renderer: ReactTestRenderer;
    await act(async () => {
      renderer = renderDialog({ taskID: "task-1" });
    });
    await draftAction(
      renderer!,
      "追踪 <AI> 更新\u2028只看官方",
    );

    expect(apiMock.proposeTaskAction).toHaveBeenCalledTimes(1);
    const [, taskID, requestID] = apiMock.proposeTaskAction.mock.calls[0] as [
      string,
      string,
      string,
    ];
    expect(taskID).toBe("task-1");
    expect(requestID.split(".")[1]).toBe(
      "66ceaf7617ecc99ca6bdf53d3ef88733015eb93505505a6253e08e2195079d8d",
    );
  });

  test.each([
    {
      name: "NEL, NBSP and emoji",
      raw: "\u0085\u00a0追踪😀\u00a0\u0085",
      normalized: "追踪😀",
      mode: "create" as const,
      taskID: "",
      digest:
        "04b1b293aae035ef83a5a5a295675e89753451de3dd9a8715a79e339e75b4bb2",
    },
    {
      name: "lone surrogate",
      raw: "坏\ud800字符",
      normalized: "坏\ufffd字符",
      mode: "create" as const,
      taskID: "",
      digest:
        "7da1a59a215651d2da47d38586587dcfcf75847255f55172d752c453ebc54904",
    },
    {
      name: "paragraph separator",
      raw: "a\u2029b",
      normalized: "a\u2029b",
      mode: "edit" as const,
      taskID: "task-1",
      digest:
        "41bace1956275d0af907e09d13e97b24010fb3aa12a47ff73d233671a6fa9c9a",
    },
    {
      name: "BOM is not Go whitespace",
      raw: "\ufeff保留\ufeff",
      normalized: "\ufeff保留\ufeff",
      mode: "create" as const,
      taskID: "",
      digest:
        "f9c1aa17e4cb004db4c3e2c5d74e2d163d797efcce537b88c05a5a84259de3ec",
    },
  ])(
    "matches Go canonicalization for $name",
    async ({ raw, normalized, mode, taskID, digest }) => {
      expect(normalizeTaskActionField(raw)).toBe(normalized);
      expect(
        JSON.parse(canonicalTaskActionPayload(mode, taskID, raw)).text,
      ).toBe(normalized);
      await expect(taskActionPayloadHash(mode, taskID, raw)).resolves.toBe(
        digest,
      );
    },
  );
});

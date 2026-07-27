// @vitest-environment jsdom

import React from "react";
import {
  act,
  cleanup,
  render,
  screen,
  waitFor,
  type RenderResult,
} from "@testing-library/react";
import userEvent, { type UserEvent } from "@testing-library/user-event";
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
        <button aria-label="dismiss" onClick={() => onOpenChange(false)} />
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

function pageText(): string {
  return document.body.textContent ?? "";
}

async function flushEffects() {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
}

function setupUser(): UserEvent {
  return userEvent.setup();
}

function dialogElement(
  props: {
    open?: boolean;
    actorScope?: string;
    taskID?: string;
    onClose?: (status?: TaskActionStatus) => void;
    onComplete?: (status: TaskActionStatus) => void;
  } = {},
) {
  return (
    <TaskActionDialog
      open={props.open ?? true}
      actorScope={props.actorScope ?? DEFAULT_ACTOR}
      taskID={props.taskID}
      labels={labels}
      onClose={props.onClose ?? vi.fn()}
      onComplete={props.onComplete ?? vi.fn()}
    />
  );
}

function renderDialog(
  props: Parameters<typeof dialogElement>[0] = {},
): RenderResult {
  return render(dialogElement(props));
}

async function draftAction(user: UserEvent, text: string): Promise<void> {
  await user.type(screen.getByRole("textbox", { name: "Task request" }), text);
  await user.click(screen.getByRole("button", { name: "Draft" }));
  await waitFor(() => {
    expect(apiMock.proposeTaskAction).toHaveBeenCalledTimes(1);
  });
  await flushEffects();
}

describe("TaskActionDialog durable lifecycle", () => {
  let sessionStorage: Storage;

  beforeEach(() => {
    vi.clearAllMocks();
    sessionStorage = window.sessionStorage;
    sessionStorage.clear();
  });

  afterEach(() => {
    cleanup();
    sessionStorage.clear();
    vi.useRealTimers();
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
    const user = setupUser();

    renderDialog({ open: false, onComplete });
    await flushEffects();

    expect(pageText()).toContain("The durable action failed");
    expect(pageText()).toContain("failed");
    expect(onComplete).toHaveBeenCalledTimes(1);
    expect(sessionStorage.getItem(storageKey())).not.toBeNull();

    await user.click(screen.getByRole("button", { name: "Done" }));
    expect(sessionStorage.getItem(storageKey())).toBeNull();
  });

  test("continues after the 120 second window and resumes immediately when reopened", async () => {
    vi.useFakeTimers();
    sessionStorage.setItem(storageKey(), storedPending("action-slow"));
    apiMock.taskActionStatus.mockResolvedValue({
      id: "action-slow",
      kind: "create",
      status: "executing",
      terminal: false,
    });

    const view = renderDialog();
    for (
      let attempt = 0;
      attempt < 90 && !pageText().includes("Still processing");
      attempt += 1
    ) {
      await act(async () => {
        await vi.advanceTimersByTimeAsync(1500);
      });
    }
    expect(apiMock.taskActionStatus.mock.calls.length).toBeGreaterThanOrEqual(80);
    expect(pageText()).toContain("Still processing");

    view.rerender(dialogElement({ open: false }));
    apiMock.taskActionStatus.mockResolvedValue({
      id: "action-slow",
      kind: "create",
      status: "executed",
      terminal: true,
      task_id: "task-42",
      message: "Created",
    });
    view.rerender(dialogElement({ open: true }));
    await flushEffects();

    expect(pageText()).toContain("Created");
    expect(pageText()).toContain(labels.status.executed);
  });

  test("recovers an accepted action after unmount and reload", async () => {
    sessionStorage.setItem(storageKey(), storedPending("action-reload"));
    apiMock.taskActionStatus.mockResolvedValueOnce({
      id: "action-reload",
      kind: "create",
      status: "executing",
      terminal: false,
    });

    const first = renderDialog({ open: false });
    await flushEffects();
    first.unmount();

    const onComplete = vi.fn();
    apiMock.taskActionStatus.mockResolvedValue({
      id: "action-reload",
      kind: "create",
      status: "executed",
      terminal: true,
      task_id: "task-reloaded",
      message: "Recovered creation",
    });
    renderDialog({ open: false, onComplete });
    await flushEffects();

    expect(pageText()).toContain("Recovered creation");
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
    renderDialog();
    await flushEffects();

    expect(sessionStorage.getItem(storageKey())).toBeNull();
    expect(screen.getByRole("textbox", { name: "Task request" })).toBeDefined();
    expect(pageText()).toContain("Action not found");
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
    renderDialog();
    await flushEffects();

    expect(pageText()).toContain("Proposal remains available");
    expect(screen.getByRole("button", { name: "Confirm" })).toBeDefined();
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
      const user = setupUser();

      const first = renderDialog();
      await draftAction(user, "Track official AI updates");
      await user.click(
        screen.getByRole("button", {
          name: mutation === "confirm" ? "Confirm" : "Cancel",
        }),
      );

      await waitFor(() => {
        expect(sessionStorage.getItem(storageKey()) ?? "").toContain(actionID);
      });
      const persistedBeforeResponse = sessionStorage.getItem(storageKey()) ?? "";
      expect(persistedBeforeResponse).toContain(actionID);
      expect(persistedBeforeResponse).not.toContain("Durable proposal");
      first.unmount();

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
      renderDialog({ open: false, onComplete });
      await flushEffects();

      expect(pageText()).toContain(terminalMessage);
      expect(pageText()).toContain(
        labels.status[terminalStatus as keyof typeof labels.status],
      );
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

    const first = renderDialog({ onComplete });
    await flushEffects();
    first.rerender(dialogElement({ open: false, onComplete }));
    first.rerender(dialogElement({ open: true, onComplete }));
    await waitFor(() => {
      expect(onComplete).toHaveBeenCalledTimes(1);
    });
    first.unmount();

    renderDialog({ open: false, onComplete });
    await flushEffects();

    expect(pageText()).toContain("Cancelled");
    expect(onComplete).toHaveBeenCalledTimes(1);
  });

  test("gives the free-text input a localized accessible name", () => {
    renderDialog();
    expect(screen.getByRole("textbox", { name: "Task request" })).toBeDefined();
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
    const user = setupUser();
    renderDialog({ taskID });
    await draftAction(user, "Track official AI updates");

    expect(pageText()).toContain("Proposal scope mismatch");
    expect(pageText()).not.toContain(action.summary);
    expect(
      screen.queryByRole("button", { name: "Confirm" }),
    ).toBeNull();
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

    const first = renderDialog({ actorScope: actorA, open: false });
    await flushEffects();
    expect(apiMock.taskActionStatus).toHaveBeenCalledWith("actor-a-action");
    first.unmount();
    const callsBeforeActorB = apiMock.taskActionStatus.mock.calls.length;

    renderDialog({ actorScope: actorB });
    await flushEffects();
    expect(screen.getByRole("textbox", { name: "Task request" })).toBeDefined();
    expect(pageText()).not.toContain("actor-a-action");
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
    renderDialog();
    await flushEffects();

    expect(pageText()).toContain("In progress");
    expect(screen.getByRole("status").getAttribute("aria-live")).toBe("polite");
  });

  test("matches the backend canonical payload digest vector", async () => {
    apiMock.proposeTaskAction.mockResolvedValue({ reply: "Drafted" });
    const user = setupUser();
    renderDialog({ taskID: "task-1" });
    await draftAction(user, "追踪 <AI> 更新\u2028只看官方");

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

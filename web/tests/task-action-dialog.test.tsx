// @vitest-environment jsdom

import React from "react";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

const apiMock = vi.hoisted(() => ({
  executeTaskAction: vi.fn(),
}));

vi.mock("@/shared/api/client", () => ({
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
} from "@/features/task/TaskActionDialog";
import { ApiError } from "@/shared/api/client";

const labels: TaskActionDialogLabels = {
  title: "New task",
  description: "Describe it",
  placeholder: "What to track",
  inputLabel: "Task request",
  execute: "Execute",
  executing: "Executing",
  close: "Done",
  requestFailed: "Localized request failed",
};

function storageKey(actor = "1:11", taskID?: string): string {
  const suffix = taskID ? `edit:${encodeURIComponent(taskID)}` : "create";
  return `vane.task-execution.v2:${encodeURIComponent(actor)}:${suffix}`;
}

function renderDialog(
  props: {
    actorScope?: string;
    taskID?: string;
    onClose?: () => void;
    onComplete?: () => void;
  } = {},
) {
  return render(
    <TaskActionDialog
      open
      actorScope={props.actorScope ?? "1:11"}
      taskID={props.taskID}
      labels={labels}
      onClose={props.onClose ?? vi.fn()}
      onComplete={props.onComplete ?? vi.fn()}
    />,
  );
}

async function submit(text: string) {
  const user = userEvent.setup();
  await user.type(screen.getByRole("textbox", { name: "Task request" }), text);
  await user.click(screen.getByRole("button", { name: "Execute" }));
}

describe("TaskActionDialog direct execution", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    window.sessionStorage.clear();
  });

  afterEach(() => {
    cleanup();
    window.sessionStorage.clear();
  });

  test("creates directly with one call and shows the terminal receipt", async () => {
    const onComplete = vi.fn();
    apiMock.executeTaskAction.mockResolvedValue({ message: "Task created" });
    renderDialog({ onComplete });

    await submit("  Track official AI updates  ");

    await waitFor(() => {
      expect(apiMock.executeTaskAction).toHaveBeenCalledTimes(1);
    });
    const [text, taskID, requestID] = apiMock.executeTaskAction.mock.calls[0];
    expect(text).toBe("Track official AI updates");
    expect(taskID).toBeUndefined();
    expect(requestID).toMatch(/^[^.]+\.[0-9a-f]{64}$/);
    await waitFor(() => {
      expect(screen.getByRole("status").textContent).toContain("Task created");
    });
    expect(onComplete).toHaveBeenCalledTimes(1);
    expect(window.sessionStorage.getItem(storageKey())).toBeNull();
    expect(screen.queryByRole("button", { name: /confirm|cancel|draft/i })).toBeNull();
  });

  test("edits the selected task directly", async () => {
    apiMock.executeTaskAction.mockResolvedValue({ message: "Task updated" });
    renderDialog({ taskID: " task-42 " });

    await submit("move it to 09:00");

    await waitFor(() => {
      expect(apiMock.executeTaskAction).toHaveBeenCalledTimes(1);
    });
    expect(apiMock.executeTaskAction.mock.calls[0][1]).toBe("task-42");
    expect(window.sessionStorage.getItem(storageKey("1:11", "task-42"))).toBeNull();
  });

  test("preserves the exact idempotency key after a lost response", async () => {
    apiMock.executeTaskAction.mockRejectedValueOnce(
      new ApiError(503, "Execution continues"),
    );
    const first = renderDialog();
    await submit("Track official AI updates");
    await waitFor(() => {
      expect(document.body.textContent).toContain("Execution continues");
    });
    const firstRequestID = apiMock.executeTaskAction.mock.calls[0][2];
    expect(window.sessionStorage.getItem(storageKey())).toContain(firstRequestID);
    first.unmount();

    apiMock.executeTaskAction.mockResolvedValueOnce({ message: "Recovered" });
    renderDialog();
    await submit("Track official AI updates");
    await waitFor(() => {
      expect(apiMock.executeTaskAction).toHaveBeenCalledTimes(2);
    });
    expect(apiMock.executeTaskAction.mock.calls[1][2]).toBe(firstRequestID);
    await waitFor(() => {
      expect(window.sessionStorage.getItem(storageKey())).toBeNull();
    });
  });

  test("a changed request gets a different idempotency key", async () => {
    apiMock.executeTaskAction.mockRejectedValueOnce(new Error("lost"));
    const first = renderDialog();
    await submit("Track AI");
    await waitFor(() => expect(apiMock.executeTaskAction).toHaveBeenCalledTimes(1));
    const firstRequestID = apiMock.executeTaskAction.mock.calls[0][2];
    first.unmount();

    apiMock.executeTaskAction.mockResolvedValueOnce({ message: "Done" });
    renderDialog();
    await submit("Track AI and robotics");
    await waitFor(() => expect(apiMock.executeTaskAction).toHaveBeenCalledTimes(2));
    expect(apiMock.executeTaskAction.mock.calls[1][2]).not.toBe(firstRequestID);
  });

  test("suppresses duplicate clicks while execution is in flight", async () => {
    let resolve!: (value: { message: string }) => void;
    apiMock.executeTaskAction.mockReturnValue(
      new Promise<{ message: string }>((done) => {
        resolve = done;
      }),
    );
    renderDialog();
    const user = userEvent.setup();
    await user.type(screen.getByRole("textbox", { name: "Task request" }), "Track AI");
    await user.dblClick(screen.getByRole("button", { name: "Execute" }));

    await waitFor(() => {
      expect(apiMock.executeTaskAction).toHaveBeenCalledTimes(1);
    });
    expect(
      (screen.getByRole("button", { name: "Executing" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    resolve({ message: "Done" });
    await waitFor(() => {
      expect(screen.getByRole("status").textContent).toContain("Done");
    });
  });

  test("does not execute when Enter confirms an IME candidate", async () => {
    apiMock.executeTaskAction.mockResolvedValue({ message: "Unexpected" });
    renderDialog();
    const input = screen.getByRole("textbox", { name: "Task request" });
    fireEvent.change(input, { target: { value: "关注李飞飞" } });
    fireEvent.keyDown(input, {
      key: "Enter",
      code: "Enter",
      isComposing: true,
    });
    expect(apiMock.executeTaskAction).not.toHaveBeenCalled();
  });

  test("close is blocked in flight and available after completion", async () => {
    const onClose = vi.fn();
    let resolve!: (value: { message: string }) => void;
    apiMock.executeTaskAction.mockReturnValue(
      new Promise<{ message: string }>((done) => {
        resolve = done;
      }),
    );
    renderDialog({ onClose });
    const user = userEvent.setup();
    await user.type(screen.getByRole("textbox", { name: "Task request" }), "Track AI");
    await user.click(screen.getByRole("button", { name: "Execute" }));
    await user.click(screen.getByRole("button", { name: "dismiss" }));
    expect(onClose).not.toHaveBeenCalled();

    resolve({ message: "Done" });
    await screen.findByRole("button", { name: "Done" });
    await user.click(screen.getByRole("button", { name: "Done" }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});

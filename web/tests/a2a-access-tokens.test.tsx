// @vitest-environment jsdom

import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

const apiMock = vi.hoisted(() => ({
  listA2AAccessTokens: vi.fn(),
  reauthenticate: vi.fn(),
  issueA2AAccessToken: vi.fn(),
  revokeA2AAccessToken: vi.fn(),
}));

vi.mock("@/shared/api/client", async (importOriginal) => {
  const original = await importOriginal<typeof import("@/shared/api/client")>();
  return { ...original, api: apiMock };
});

vi.mock("sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

import { I18nProvider } from "@/i18n";
import A2AAccessTokens from "@/pages/A2AAccessTokens";
import type { A2AAccessToken, MeResponse, WorkspaceRole } from "@/shared/api/client";

const now = "2026-08-15T00:00:00Z";

function me(tenantID: number, userID: number, role: WorkspaceRole): MeResponse {
  return {
    ok: true,
    tenant_id: tenantID,
    user_id: userID,
    email: `user${userID}@example.com`,
    role,
    actor_type: "user",
    workspaces: [{
      id: tenantID,
      name: `Workspace ${tenantID}`,
      kind: "team",
      status: "active",
      plan: "team",
      seat_limit: 10,
      member_count: 3,
      role,
      created_at: now,
      updated_at: now,
    }],
  };
}

function accessToken(overrides: Partial<A2AAccessToken> = {}): A2AAccessToken {
  return {
    id: "token-1",
    tenant_id: 11,
    principal_user_id: 1,
    actor_type: "user",
    scopes: ["content.query"],
    issued_by: 1,
    expires_at: "2099-08-15T00:00:00Z",
    created_at: now,
    ...overrides,
  };
}

beforeEach(() => {
  vi.resetAllMocks();
  localStorage.setItem("vane.locale", "zh");
  vi.stubGlobal("confirm", vi.fn(() => true));
  Object.defineProperty(navigator, "clipboard", {
    configurable: true,
    value: { writeText: vi.fn().mockResolvedValue(undefined) },
  });
  apiMock.listA2AAccessTokens.mockResolvedValue([]);
  apiMock.reauthenticate.mockResolvedValue({ ok: true, proof: "recent-proof", expires_in: 600 });
  apiMock.issueA2AAccessToken.mockResolvedValue(accessToken({
    id: "issued-1",
    token: "raw-one-time-token",
  }));
  apiMock.revokeA2AAccessToken.mockResolvedValue({ ok: true });
});

afterEach(() => {
  cleanup();
  localStorage.clear();
  vi.unstubAllGlobals();
});

function renderPage(principal = me(11, 1, "member")) {
  return render(<I18nProvider><A2AAccessTokens me={principal} /></I18nProvider>);
}

describe("A2A credential management", () => {
  test("creates a least-privilege personal token after session-bound reauthentication and displays it once", async () => {
    const user = userEvent.setup();
    renderPage();
    expect(await screen.findByText("这个工作区还没有访问凭证。")).toBeTruthy();
    expect(screen.queryByText("自动化身份")).toBeNull();

    await user.type(screen.getByLabelText("确认你的登录密码"), "correct horse");
    await user.click(screen.getByRole("button", { name: "验证并创建" }));

    await waitFor(() => expect(apiMock.reauthenticate).toHaveBeenCalledWith("correct horse"));
    expect(apiMock.issueA2AAccessToken).toHaveBeenCalledWith({
      actor_type: "user",
      principal_user_id: 1,
      scopes: ["content.query"],
      expires_in_days: 30,
    }, "recent-proof");
    expect((await screen.findByTestId("one-time-a2a-token")).textContent).toContain("raw-one-time-token");
    expect((screen.getByLabelText("确认你的登录密码") as HTMLInputElement).value).toBe("");
    expect(screen.getAllByText("读取工作区情报").length).toBeGreaterThanOrEqual(2);
    const clipboardWrite = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue(undefined);
    await user.click(screen.getByRole("button", { name: "复制 Token" }));
    expect(clipboardWrite).toHaveBeenCalledWith("raw-one-time-token");
    await user.click(screen.getByRole("button", { name: "我已保存" }));
    expect(screen.queryByTestId("one-time-a2a-token")).toBeNull();
  });

  test("lets an admin create a labelled Member-level automation and never sends tenant overrides", async () => {
    const user = userEvent.setup();
    renderPage(me(22, 8, "admin"));
    await screen.findByText("这个工作区还没有访问凭证。");
    await user.click(screen.getByRole("button", { name: /自动化身份/ }));
    await user.type(screen.getByLabelText("自动化名称"), "竞品日报机器人");
    await user.type(screen.getByLabelText("确认你的登录密码"), "admin-password");
    await user.click(screen.getByRole("button", { name: "验证并创建" }));

    await waitFor(() => expect(apiMock.issueA2AAccessToken).toHaveBeenCalled());
    expect(apiMock.issueA2AAccessToken).toHaveBeenCalledWith({
      actor_type: "service_account",
      principal_user_id: 8,
      service_account_label: "竞品日报机器人",
      scopes: ["content.query"],
      expires_in_days: 30,
    }, "recent-proof");
    expect(JSON.stringify(apiMock.issueA2AAccessToken.mock.calls[0]?.[0])).not.toContain("tenant");
  });

  test("clears the one-time bearer and rejects stale results when the Principal changes", async () => {
    const user = userEvent.setup();
    let resolveOld: ((items: A2AAccessToken[]) => void) | undefined;
    apiMock.listA2AAccessTokens.mockImplementationOnce(() => new Promise<A2AAccessToken[]>((resolve) => {
      resolveOld = resolve;
    })).mockResolvedValueOnce([]);

    const view = renderPage(me(31, 4, "owner"));
    view.rerender(<I18nProvider><A2AAccessTokens me={me(32, 4, "owner")} /></I18nProvider>);
    await screen.findByText("这个工作区还没有访问凭证。");
    resolveOld?.([accessToken({ tenant_id: 31, service_account_label: "旧工作区凭证" })]);
    await Promise.resolve();
    expect(screen.queryByText("旧工作区凭证")).toBeNull();

    await user.type(screen.getByLabelText("确认你的登录密码"), "owner-password");
    await user.click(screen.getByRole("button", { name: "验证并创建" }));
    expect(await screen.findByTestId("one-time-a2a-token")).toBeTruthy();
    view.rerender(<I18nProvider><A2AAccessTokens me={me(33, 4, "owner")} /></I18nProvider>);
    await waitFor(() => expect(screen.queryByTestId("one-time-a2a-token")).toBeNull());
  });

  test("revokes an active credential without retaining the raw bearer", async () => {
    const user = userEvent.setup();
    apiMock.listA2AAccessTokens.mockResolvedValue([accessToken({ service_account_label: "日报机器人" })]);
    renderPage(me(11, 1, "owner"));
    expect(await screen.findByText("日报机器人")).toBeTruthy();
    expect(screen.queryByTestId("one-time-a2a-token")).toBeNull();
    await user.click(screen.getByRole("button", { name: "撤销" }));
    await waitFor(() => expect(apiMock.revokeA2AAccessToken).toHaveBeenCalledWith("token-1"));
    expect(screen.getByText("已撤销")).toBeTruthy();
  });
});

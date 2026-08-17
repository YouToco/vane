// @vitest-environment jsdom

import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { I18nProvider } from "@/i18n";

const apiMock = vi.hoisted(() => ({
  claimInstallation: vi.fn(),
  setupStatus: vi.fn(),
}));

vi.mock("@/shared/api/client", () => ({
  api: apiMock,
  ApiError: class ApiError extends Error {},
}));

import Setup from "@/pages/Setup";

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.setItem("vane.locale", "zh");
  apiMock.setupStatus.mockResolvedValue({
    state: "setup_required",
    setup_required: true,
  });
});

afterEach(() => {
  cleanup();
  localStorage.clear();
});

describe("first-run setup", () => {
  test("claims with the host-local token without echoing it", async () => {
    apiMock.claimInstallation.mockResolvedValue({
      ok: true,
      tenant_id: 1,
      restart_required: true,
    });
    render(
      <I18nProvider>
        <Setup />
      </I18nProvider>,
    );

    const token = screen.getByLabelText("一次性初始化令牌") as HTMLInputElement;
    expect(token.type).toBe("password");
    await userEvent.type(
      token,
      "synthetic-host-token-abcdefghijklmnopqrstuvwxyz",
    );
    await userEvent.type(
      screen.getByLabelText("平台 owner 邮箱"),
      "owner@example.com",
    );
    await userEvent.type(screen.getByLabelText("设置密码"), "secure-password");
    await userEvent.click(
      screen.getByRole("button", { name: "创建平台 owner" }),
    );

    await waitFor(() =>
      expect(apiMock.claimInstallation).toHaveBeenCalledWith(
        "synthetic-host-token-abcdefghijklmnopqrstuvwxyz",
        "owner@example.com",
        "secure-password",
      ),
    );
    expect(await screen.findByText("平台 owner 已创建")).toBeTruthy();
    expect(
      screen.queryByDisplayValue(
        "synthetic-host-token-abcdefghijklmnopqrstuvwxyz",
      ),
    ).toBeNull();
  });

  test("shows a recoverable unavailable state instead of exposing login", async () => {
    render(
      <I18nProvider>
        <Setup unavailable />
      </I18nProvider>,
    );
    expect(await screen.findByText("暂时无法读取初始化状态")).toBeTruthy();
    expect(screen.getByRole("button", { name: "重新检查" })).toBeTruthy();
    expect(screen.queryByText("登录")).toBeNull();
  });
});

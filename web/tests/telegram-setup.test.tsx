// @vitest-environment jsdom

import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

const apiMock = vi.hoisted(() => ({
  telegramStatus: vi.fn(),
  telegramLink: vi.fn(),
  telegramRouteLink: vi.fn(),
  telegramRouteUnlink: vi.fn(),
  telegramTest: vi.fn(),
  telegramUnlink: vi.fn(),
}));

vi.mock("@/shared/api/client", () => ({
  api: apiMock,
  ApiError: class ApiError extends Error {},
}));

import TelegramSetup from "@/pages/TelegramSetup";

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  cleanup();
});

describe("Telegram settings", () => {
  test("does not ask the browser for bot secrets when server is disabled", async () => {
    apiMock.telegramStatus.mockResolvedValue({
      enabled: false,
      ready: false,
      bound: false,
    });
    render(<TelegramSetup />);
    expect(await screen.findByText(/服务端尚未启用 Telegram/)).toBeTruthy();
    expect(screen.queryByRole("textbox")).toBeNull();
    expect(screen.queryByLabelText(/Bot token/i)).toBeNull();
  });

  test("issues an opaque one-time pairing link for the current session", async () => {
    apiMock.telegramStatus.mockResolvedValue({
      enabled: true,
      ready: true,
      bound: false,
      bot_id: 123,
      bot_username: "vane_bot",
    });
    apiMock.telegramLink.mockResolvedValue({
      deep_link: "https://t.me/vane_bot?start=opaque-token",
      expires_at: "2026-08-15T01:00:00Z",
    });
    render(<TelegramSetup />);
    await screen.findByText("@vane_bot");
    await userEvent.click(
      screen.getByRole("button", { name: /生成 10 分钟一次性配对链接/ }),
    );
    await waitFor(() => expect(apiMock.telegramLink).toHaveBeenCalledTimes(1));
    const link = await screen.findByRole("link", { name: /打开 Telegram 完成绑定/ });
    expect(link.getAttribute("href")).toBe(
      "https://t.me/vane_bot?start=opaque-token",
    );
    expect(screen.getByText(/链接使用一次后立即失效/)).toBeTruthy();
  });

  test("offers test and unlink only for an active binding", async () => {
    apiMock.telegramStatus
      .mockResolvedValueOnce({ enabled: true, ready: true, bound: true })
      .mockResolvedValue({ enabled: true, ready: true, bound: false });
    apiMock.telegramTest.mockResolvedValue({ ok: true });
    apiMock.telegramUnlink.mockResolvedValue({ ok: true });
    render(<TelegramSetup />);
    await screen.findByText("已绑定");
    await userEvent.click(screen.getByRole("button", { name: /发送测试消息/ }));
    await waitFor(() => expect(apiMock.telegramTest).toHaveBeenCalledTimes(1));
    expect(await screen.findByText(/测试消息已确认送达/)).toBeTruthy();
    await userEvent.click(screen.getByRole("button", { name: /解除绑定/ }));
    await waitFor(() => expect(apiMock.telegramUnlink).toHaveBeenCalledTimes(1));
  });

  test("installs and revokes explicit group or topic routes", async () => {
    apiMock.telegramStatus.mockResolvedValue({
      enabled: true,
      ready: true,
      bound: true,
      routes: [
        { id: 1, kind: "private", chat_type: "private", bound_at: "2026-08-15T01:00:00Z" },
        { id: 7, kind: "topic", chat_type: "supergroup", bound_at: "2026-08-15T01:01:00Z" },
      ],
    });
    apiMock.telegramRouteLink.mockResolvedValue({
      deep_link: "https://t.me/vane_bot?startgroup=opaque-route-token",
      command: "/connect opaque-route-token",
      expires_at: "2026-08-15T01:10:00Z",
    });
    apiMock.telegramRouteUnlink.mockResolvedValue({ ok: true });
    render(<TelegramSetup />);
    expect(await screen.findByText(/论坛话题 #7 · supergroup/)).toBeTruthy();
    await userEvent.click(screen.getByRole("button", { name: /连接群组或话题/ }));
    await waitFor(() => expect(apiMock.telegramRouteLink).toHaveBeenCalledTimes(1));
    expect((await screen.findByRole("link", { name: /添加 Bot 到群组/ })).getAttribute("href"))
      .toBe("https://t.me/vane_bot?startgroup=opaque-route-token");
    expect(screen.getByText("/connect opaque-route-token")).toBeTruthy();
    await userEvent.click(screen.getByRole("button", { name: "解除连接" }));
    await waitFor(() => expect(apiMock.telegramRouteUnlink).toHaveBeenCalledWith(7));
  });
});

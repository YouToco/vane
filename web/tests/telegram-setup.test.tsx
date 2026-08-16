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
  telegramCredentialStatus: vi.fn(),
  telegramRotateCredential: vi.fn(),
  telegramRevokeCredential: vi.fn(),
  deliveryChannelPreference: vi.fn(),
  patchDeliveryChannelPreference: vi.fn(),
  patchTaskDeliveryChannelPreference: vi.fn(),
  deleteTaskDeliveryChannelPreference: vi.fn(),
}));

vi.mock("@/shared/api/client", () => ({
  api: apiMock,
  ApiError: class ApiError extends Error {},
}));

import TelegramSetup from "@/pages/TelegramSetup";
import DeliveryChannelPreferenceCard from "@/pages/DeliveryChannelPreference";
import TaskDeliveryChannel from "@/features/task/TaskDeliveryChannel";

beforeEach(() => {
  vi.clearAllMocks();
  apiMock.telegramCredentialStatus.mockResolvedValue({
    configured: false,
    vault_ready: true,
  });
});

afterEach(() => {
  cleanup();
});

describe("Telegram settings", () => {
  test("surfaces status and credential loading failures without exposing secrets", async () => {
    apiMock.telegramStatus.mockRejectedValue(new Error("offline"));
    apiMock.telegramCredentialStatus.mockRejectedValue(new Error("vault offline"));
    render(<TelegramSetup />);
    expect(await screen.findByText(/读取 Telegram 状态失败/)).toBeTruthy();
    expect(await screen.findByText(/读取 Bot 凭证状态失败/)).toBeTruthy();
  });

  test("lets an ordinary signed-in user configure an encrypted personal bot", async () => {
    apiMock.telegramStatus.mockResolvedValue({
      enabled: false,
      ready: false,
      bound: false,
    });
    apiMock.telegramRotateCredential.mockResolvedValue({
      configured: true,
      vault_ready: true,
      generation: 1,
      fingerprint: "0123456789abcdef0123456789abcdef",
      activation: "active",
    });
    render(<TelegramSetup />);
    expect(await screen.findByText(/当前用户尚未启用 Telegram Bot/)).toBeTruthy();
    const input = screen.getByLabelText(/新的 Bot Token/i);
    expect(input.getAttribute("type")).toBe("password");
    await userEvent.type(input, "123:synthetic-token");
    await userEvent.click(screen.getByRole("button", { name: /校验、加密并启用/ }));
    await waitFor(() => expect(apiMock.telegramRotateCredential).toHaveBeenCalledWith({
      bot_token: "123:synthetic-token",
    }));
    expect(await screen.findByText(/已加密保存并启用/)).toBeTruthy();
    expect((input as HTMLInputElement).value).toBe("");
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

  test("revokes an encrypted personal bot only after confirmation", async () => {
    apiMock.telegramCredentialStatus
      .mockResolvedValueOnce({ configured: true, vault_ready: true, generation: 3 })
      .mockResolvedValueOnce({ configured: false, vault_ready: true });
    apiMock.telegramStatus
      .mockResolvedValueOnce({ enabled: true, ready: true, bound: true })
      .mockResolvedValueOnce({ enabled: false, ready: false, bound: false });
    apiMock.telegramRevokeCredential.mockResolvedValue({ ok: true });
    const confirm = vi.spyOn(window, "confirm").mockReturnValue(true);
    render(<TelegramSetup />);
    expect(await screen.findByText(/已保存 · 第 3 代/)).toBeTruthy();
    await userEvent.click(screen.getByRole("button", { name: /撤销 Bot 凭证/ }));
    await waitFor(() => expect(apiMock.telegramRevokeCredential).toHaveBeenCalledTimes(1));
    expect(await screen.findByText(/个人 Telegram Bot 凭证已撤销/)).toBeTruthy();
    confirm.mockRestore();
  });

  test("keeps every Telegram mutation failure visible and retryable", async () => {
    apiMock.telegramStatus.mockResolvedValue({
      enabled: true,
      ready: true,
      bound: true,
      routes: [{ id: 7, kind: "topic", chat_type: "supergroup", bound_at: "2026-08-15T01:01:00Z" }],
    });
    apiMock.telegramTest.mockRejectedValue(new Error("send failed"));
    apiMock.telegramRouteLink.mockRejectedValue(new Error("route failed"));
    apiMock.telegramRouteUnlink.mockRejectedValue(new Error("unlink route failed"));
    apiMock.telegramUnlink.mockRejectedValue(new Error("unlink failed"));
    render(<TelegramSetup />);
    await screen.findByText("已绑定");

    await userEvent.click(screen.getByRole("button", { name: /发送测试消息/ }));
    expect(await screen.findByText("测试消息发送失败")).toBeTruthy();
    await userEvent.click(screen.getByRole("button", { name: /连接群组或话题/ }));
    expect(await screen.findByText("生成群组连接链接失败")).toBeTruthy();
    await userEvent.click(screen.getByRole("button", { name: "解除连接" }));
    expect(await screen.findByText("解除群组连接失败")).toBeTruthy();
    await userEvent.click(screen.getByRole("button", { name: /解除绑定（全部）/ }));
    expect(await screen.findByText("解除绑定失败")).toBeTruthy();
  });

  test("reports a pairing-link failure", async () => {
    apiMock.telegramStatus.mockResolvedValue({
      enabled: true,
      ready: true,
      bound: false,
      bot_username: "vane_bot",
    });
    apiMock.telegramLink.mockRejectedValue(new Error("link failed"));
    render(<TelegramSetup />);
    await screen.findByText("@vane_bot");
    await userEvent.click(screen.getByRole("button", { name: /生成 10 分钟一次性配对链接/ }));
    expect(await screen.findByText("生成配对链接失败")).toBeTruthy();
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

describe("Task delivery channel", () => {
  test("shows the effective route and saves an exact task override", async () => {
    apiMock.telegramStatus.mockResolvedValue({
      enabled: true,
      ready: true,
      bound: true,
      routes: [
        { id: 1, kind: "private", chat_type: "private", bound_at: "2026-08-15T01:00:00Z" },
        { id: 7, kind: "topic", chat_type: "supergroup", bound_at: "2026-08-15T01:01:00Z" },
      ],
    });
    apiMock.patchTaskDeliveryChannelPreference.mockResolvedValue({
      selection: "both",
      scope: "task",
      task_id: "task-7",
      telegram_route_id: 7,
      explicit: true,
    });
    render(
      <TaskDeliveryChannel
        scheduleID="task-7"
        initial={{
          selection: "telegram",
          scope: "account",
          telegram_route_id: 1,
          explicit: true,
        }}
      />,
    );
    const destination = await screen.findByLabelText("Telegram 目的地");
    await userEvent.selectOptions(destination, "7");
    await userEvent.click(screen.getByRole("button", { name: "飞书 + Telegram" }));
    await waitFor(() => expect(
      apiMock.patchTaskDeliveryChannelPreference,
    ).toHaveBeenCalledWith("task-7", "both", 7));
    expect(await screen.findByText("任务推送渠道已保存。")).toBeTruthy();
    expect(screen.getAllByText("飞书 + Telegram").length).toBeGreaterThan(0);
  });

  test("restores the account default for an explicit task override", async () => {
    apiMock.telegramStatus.mockResolvedValue({ enabled: true, ready: true, bound: true, routes: [] });
    apiMock.deleteTaskDeliveryChannelPreference.mockResolvedValue({
      selection: "feishu",
      scope: "account",
      explicit: true,
    });
    const onChange = vi.fn();
    render(
      <TaskDeliveryChannel
        scheduleID="task-8"
        initial={{ selection: "telegram", scope: "task", explicit: true }}
        onChange={onChange}
      />,
    );
    await userEvent.click(screen.getByRole("button", { name: "使用账号默认" }));
    await waitFor(() => expect(apiMock.deleteTaskDeliveryChannelPreference).toHaveBeenCalledWith("task-8"));
    expect(await screen.findByText("已恢复使用账号默认推送渠道。")).toBeTruthy();
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ selection: "feishu", scope: "account" }));
  });
});

describe("delivery channel preference", () => {
  test("shows the compatible Feishu default and persists a dual-channel choice", async () => {
    apiMock.deliveryChannelPreference.mockResolvedValue({
      selection: "feishu",
      scope: "account",
      explicit: false,
    });
    apiMock.telegramStatus.mockResolvedValue({
      enabled: true,
      ready: true,
      bound: true,
      routes: [],
    });
    apiMock.patchDeliveryChannelPreference.mockResolvedValue({
      selection: "both",
      scope: "account",
      explicit: true,
    });

    render(<DeliveryChannelPreferenceCard />);
    expect(await screen.findByText(/当前沿用兼容默认值：仅飞书/)).toBeTruthy();
    await userEvent.click(screen.getByRole("button", { name: /两个渠道/ }));
    await waitFor(() => expect(apiMock.patchDeliveryChannelPreference).toHaveBeenCalledWith("both", undefined));
    expect(await screen.findByText(/主动推送运行时完成渠道化后会使用这个选择/)).toBeTruthy();
  });

  test("persists an exact Telegram topic route", async () => {
    apiMock.deliveryChannelPreference.mockResolvedValue({
      selection: "telegram",
      scope: "account",
      telegram_route_id: 7,
      explicit: true,
    });
    apiMock.telegramStatus.mockResolvedValue({
      enabled: true,
      ready: true,
      bound: true,
      routes: [
        { id: 7, kind: "topic", chat_type: "supergroup", bound_at: "2026-08-15T01:01:00Z" },
      ],
    });
    apiMock.patchDeliveryChannelPreference.mockResolvedValue({
      selection: "telegram",
      scope: "account",
      telegram_route_id: 7,
      explicit: true,
    });

    render(<DeliveryChannelPreferenceCard />);
    expect(await screen.findByRole("option", { name: "论坛话题 #7" })).toBeTruthy();
    await userEvent.click(screen.getByRole("button", { name: "保存目的地" }));
    await waitFor(() => expect(apiMock.patchDeliveryChannelPreference).toHaveBeenCalledWith("telegram", 7));
  });
});

// @vitest-environment jsdom

import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

const apiMock = vi.hoisted(() => ({
  adminFetchCredentialStatus: vi.fn(),
  adminRotateFetchCredential: vi.fn(),
  adminRevokeFetchCredential: vi.fn(),
}));

vi.mock("@/shared/api/client", () => ({
  api: apiMock,
  ApiError: class ApiError extends Error {},
}));

import FetchCredentials from "@/pages/FetchCredentials";

beforeEach(() => {
  vi.clearAllMocks();
  apiMock.adminFetchCredentialStatus.mockResolvedValue({
    configured: true,
    vault_ready: true,
    generation: 3,
    fingerprint: "a".repeat(64),
    metadata: { providers: ["exa", "tikhub"] },
  });
});

afterEach(cleanup);

describe("fetch credential admin", () => {
  test("rotates both provider keys without retaining or echoing them", async () => {
    apiMock.adminRotateFetchCredential.mockResolvedValue({
      configured: true,
      vault_ready: true,
      generation: 4,
      fingerprint: "b".repeat(64),
      metadata: { providers: ["exa", "tikhub"] },
      activation: "restart_required",
    });
    render(<FetchCredentials />);
    expect(await screen.findByText("第 3 代")).toBeTruthy();
    await userEvent.type(screen.getByLabelText("Exa 新 API Key"), "synthetic-exa-key");
    await userEvent.type(screen.getByLabelText("TikHub 新 API Key"), "synthetic-tikhub-key");
    await userEvent.click(screen.getByRole("button", { name: /保存并轮换/ }));
    await waitFor(() => expect(apiMock.adminRotateFetchCredential).toHaveBeenCalledWith({
      exa_api_key: "synthetic-exa-key",
      tikhub_api_key: "synthetic-tikhub-key",
    }));
    expect(await screen.findByText(/安全重启后切换/)).toBeTruthy();
    expect((screen.getByLabelText("Exa 新 API Key") as HTMLInputElement).value).toBe("");
    expect((screen.getByLabelText("TikHub 新 API Key") as HTMLInputElement).value).toBe("");
    expect(screen.queryByDisplayValue("synthetic-exa-key")).toBeNull();
  });

  test("blocks mutation when the deployment keyring is unavailable", async () => {
    apiMock.adminFetchCredentialStatus.mockResolvedValue({ configured: false, vault_ready: false });
    render(<FetchCredentials />);
    expect(await screen.findByText(/部署侧尚未配置凭证库主密钥/)).toBeTruthy();
    expect((screen.getByRole("button", { name: /保存并轮换/ }) as HTMLButtonElement).disabled).toBe(true);
  });

  test("records explicit revoke without reviving environment fallback", async () => {
    apiMock.adminRevokeFetchCredential.mockResolvedValue({ ok: true });
    const confirm = vi.spyOn(window, "confirm").mockReturnValue(true);
    render(<FetchCredentials />);
    await screen.findByText("第 3 代");
    await userEvent.click(screen.getByRole("button", { name: /撤销当前版本/ }));
    await waitFor(() => expect(apiMock.adminRevokeFetchCredential).toHaveBeenCalledTimes(1));
    expect(await screen.findByText(/fail-closed/)).toBeTruthy();
    confirm.mockRestore();
  });
});

// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

const apiMock = vi.hoisted(() => ({
  adminLLMCredentialStatus: vi.fn(),
  adminRotateLLMCredential: vi.fn(),
  adminRevokeLLMCredential: vi.fn(),
}));

vi.mock("@/shared/api/client", () => ({
  api: apiMock,
  ApiError: class ApiError extends Error {},
}));

import LLMCredentials from "@/pages/LLMCredentials";

beforeEach(() => {
  vi.clearAllMocks();
  apiMock.adminLLMCredentialStatus.mockResolvedValue({
    configured: true,
    vault_ready: true,
    generation: 4,
    fingerprint: "a".repeat(64),
    metadata: {
      provider: "deepseek",
      base_url: "https://api.deepseek.com",
      model: "pipeline-model",
      agent_model: "agent-model",
      research_model: "research-model",
      max_concurrent: 6,
    },
  });
});

afterEach(cleanup);

describe("shared LLM credential admin", () => {
  test("shows only redacted metadata and rotates with a newly entered key", async () => {
    apiMock.adminRotateLLMCredential.mockResolvedValue({
      configured: true,
      vault_ready: true,
      generation: 5,
      fingerprint: "b".repeat(64),
      metadata: { provider: "deepseek" },
      activation: "restart_required",
    });
    render(<LLMCredentials />);
    expect(await screen.findByText("第 4 代")).toBeTruthy();
    const key = screen.getByLabelText("DeepSeek 新 API Key") as HTMLInputElement;
    expect(key.value).toBe("");
    fireEvent.change(key, { target: { value: "synthetic-new-key" } });
    await userEvent.click(screen.getByRole("button", { name: /保存并轮换/ }));
    await waitFor(() => expect(apiMock.adminRotateLLMCredential).toHaveBeenCalledTimes(1));
    expect(apiMock.adminRotateLLMCredential.mock.calls[0][0]).toMatchObject({
      api_key: "synthetic-new-key",
      provider: "deepseek",
      agent_provider: "",
      agent_base_url: "",
      agent_api_key: "",
      model: "pipeline-model",
      agent_model: "agent-model",
      research_model: "research-model",
      max_concurrent: 6,
    });
    expect(await screen.findByText(/安全重启后切换/)).toBeTruthy();
    expect(key.value).toBe("");
    expect(screen.queryByDisplayValue("synthetic-new-key")).toBeNull();
  });

  test("blocks mutation UI when the deployment keyring is absent", async () => {
    apiMock.adminLLMCredentialStatus.mockResolvedValue({
      configured: false,
      vault_ready: false,
    });
    render(<LLMCredentials />);
    expect(await screen.findByText(/部署侧尚未配置凭证库主密钥/)).toBeTruthy();
    expect((screen.getByRole("button", { name: /保存并轮换/ }) as HTMLButtonElement).disabled).toBe(true);
  });
});

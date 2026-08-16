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

  test("configures a native Kimi agent route without media conversion", async () => {
    apiMock.adminRotateLLMCredential.mockResolvedValue({
      configured: true,
      vault_ready: true,
      generation: 5,
      fingerprint: "b".repeat(64),
      metadata: { provider: "deepseek", agent_provider: "kimi" },
      activation: "restart_required",
    });
    render(<LLMCredentials />);
    await screen.findByText("第 4 代");
    await userEvent.selectOptions(screen.getByLabelText("Agent Provider"), "kimi");
    expect((screen.getByLabelText("Agent 官方 API 地址") as HTMLInputElement).value)
      .toBe("https://api.moonshot.cn/v1");
    await userEvent.clear(screen.getByLabelText("流水线模型"));
    await userEvent.type(screen.getByLabelText("流水线模型"), "pipeline-v2");
    await userEvent.clear(screen.getByLabelText("研究模型"));
    await userEvent.type(screen.getByLabelText("研究模型"), "research-v2");
    await userEvent.clear(screen.getByLabelText("Agent 模型"));
    await userEvent.type(screen.getByLabelText("Agent 模型"), "kimi-k3");
    await userEvent.clear(screen.getByLabelText("最大并发"));
    await userEvent.type(screen.getByLabelText("最大并发"), "8");
    await userEvent.type(screen.getByLabelText("DeepSeek 新 API Key"), "synthetic-pipeline-key");
    await userEvent.type(screen.getByLabelText("Agent 新 API Key"), "synthetic-agent-key");
    await userEvent.click(screen.getByRole("button", { name: /保存并轮换/ }));
    await waitFor(() => expect(apiMock.adminRotateLLMCredential).toHaveBeenCalledWith(expect.objectContaining({
      model: "pipeline-v2",
      research_model: "research-v2",
      agent_provider: "kimi",
      agent_base_url: "https://api.moonshot.cn/v1",
      agent_model: "kimi-k3",
      max_concurrent: 8,
    })));
  });

  test("revokes the active database credential after explicit confirmation", async () => {
    apiMock.adminRevokeLLMCredential.mockResolvedValue({ ok: true });
    const confirm = vi.spyOn(window, "confirm").mockReturnValue(true);
    render(<LLMCredentials />);
    await screen.findByText("第 4 代");
    await userEvent.click(screen.getByRole("button", { name: /撤销当前版本/ }));
    await waitFor(() => expect(apiMock.adminRevokeLLMCredential).toHaveBeenCalledTimes(1));
    expect(await screen.findByText(/撤销已记录/)).toBeTruthy();
    confirm.mockRestore();
  });

  test("shows load and save failures as actionable errors", async () => {
    apiMock.adminLLMCredentialStatus.mockRejectedValue(new Error("database unavailable"));
    render(<LLMCredentials />);
    expect(await screen.findByText("读取 LLM 凭证状态失败")).toBeTruthy();
    cleanup();

    apiMock.adminLLMCredentialStatus.mockResolvedValue({ configured: false, vault_ready: true });
    apiMock.adminRotateLLMCredential.mockRejectedValue(new Error("rotate unavailable"));
    render(<LLMCredentials />);
    await userEvent.type(screen.getByLabelText("DeepSeek 新 API Key"), "synthetic-key");
    await userEvent.click(screen.getByRole("button", { name: /保存并轮换/ }));
    expect(await screen.findByText("保存 LLM 凭证失败")).toBeTruthy();
  });
});

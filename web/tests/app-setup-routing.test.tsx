// @vitest-environment jsdom

import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { I18nProvider } from "@/i18n";

const apiMock = vi.hoisted(() => ({
  setupStatus: vi.fn(),
  me: vi.fn(),
  claimInstallation: vi.fn(),
}));

vi.mock("@/shared/api/client", () => ({
  api: apiMock,
  ApiError: class ApiError extends Error {},
}));

import App from "@/app/App";

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.setItem("vane.locale", "zh");
  location.hash = "#/login";
});

afterEach(() => {
  cleanup();
  localStorage.clear();
});

test("setup_required preempts login and never probes an authenticated session", async () => {
  apiMock.setupStatus.mockResolvedValue({
    state: "setup_required",
    setup_required: true,
  });
  render(
    <I18nProvider>
      <App />
    </I18nProvider>,
  );
  expect(await screen.findByText("初始化 Vane")).toBeTruthy();
  await waitFor(() => expect(apiMock.setupStatus).toHaveBeenCalledTimes(1));
  expect(apiMock.me).not.toHaveBeenCalled();
  expect(screen.queryByRole("button", { name: "登 录" })).toBeNull();
});

test("active installation continues into the normal session probe", async () => {
  apiMock.setupStatus.mockResolvedValue({
    state: "active",
    setup_required: false,
  });
  apiMock.me.mockRejectedValue(new Error("not signed in"));
  render(
    <I18nProvider>
      <App />
    </I18nProvider>,
  );
  await waitFor(() => expect(apiMock.me).toHaveBeenCalledTimes(1));
  expect(apiMock.setupStatus).toHaveBeenCalledTimes(1);
  expect(screen.queryByText("初始化 Vane")).toBeNull();
});

test("setup status failure stays on a recoverable setup surface", async () => {
  apiMock.setupStatus.mockRejectedValue(new Error("database unavailable"));
  render(
    <I18nProvider>
      <App />
    </I18nProvider>,
  );
  expect(await screen.findByText("暂时无法读取初始化状态")).toBeTruthy();
  expect(apiMock.me).not.toHaveBeenCalled();
  expect(screen.getByRole("button", { name: "重新检查" })).toBeTruthy();
});

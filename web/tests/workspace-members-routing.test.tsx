// @vitest-environment jsdom

import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

vi.mock("@/pages/Profile", () => ({ default: () => <div>profile-marker</div> }));
vi.mock("@/pages/FeishuSetup", () => ({ default: () => <div>channel-marker</div> }));
vi.mock("@/pages/WorkspaceMembers", () => ({
  default: ({ me }: { me: { tenant_id: number } }) => (
    <div>workspace-members-marker-{me.tenant_id}</div>
  ),
}));

import AuthenticatedApp from "@/app/AuthenticatedApp";
import { I18nProvider } from "@/i18n";
import Settings from "@/pages/Settings";
import type { MeResponse } from "@/shared/api/client";

const me: MeResponse = {
  ok: true,
  tenant_id: 42,
  user_id: 7,
  email: "owner@example.com",
  role: "owner",
  actor_type: "user",
  workspaces: [{
    id: 42,
    name: "Signal Team",
    kind: "team",
    status: "active",
    plan: "team",
    seat_limit: 10,
    member_count: 3,
    role: "owner",
    created_at: "2026-08-14T00:00:00Z",
    updated_at: "2026-08-14T00:00:00Z",
  }],
};

beforeEach(() => {
  localStorage.setItem("vane.locale", "zh");
  Object.defineProperty(window, "matchMedia", {
    configurable: true,
    value: vi.fn().mockImplementation((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      addListener: vi.fn(),
      removeListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })),
  });
});

afterEach(() => {
  cleanup();
  localStorage.clear();
});

describe("workspace member navigation", () => {
  test("Settings selects the member tab from the canonical hash", async () => {
    const user = userEvent.setup();
    render(
      <I18nProvider>
        <Settings hash="#/settings/members" me={me} onAuthorityChanged={vi.fn()} />
      </I18nProvider>,
    );

    expect(await screen.findByText("workspace-members-marker-42")).toBeTruthy();
    expect(screen.getByRole("tab", { name: "成员与邀请" }).getAttribute("aria-selected")).toBe("true");
    await user.click(screen.getByRole("tab", { name: "推送通道" }));
    expect(location.hash).toBe("#/settings/channel");
  });

  test("authenticated sidebar and router expose the current workspace member page", async () => {
    render(
      <I18nProvider>
        <AuthenticatedApp hash="#/settings/members" me={me} />
      </I18nProvider>,
    );

    expect(await screen.findByText("workspace-members-marker-42")).toBeTruthy();
    expect(screen.getAllByText("成员与邀请").length).toBeGreaterThanOrEqual(2);
    expect(screen.getByText("Signal Team")).toBeTruthy();
  });
});

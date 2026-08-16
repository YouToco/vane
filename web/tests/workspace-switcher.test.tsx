// @vitest-environment jsdom

import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

import { WorkspaceSwitcher } from "@/app/WorkspaceSwitcher";
import { SidebarProvider } from "@/components/ui/sidebar";
import type { MeResponse } from "@/shared/api/client";

beforeEach(() => {
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

afterEach(cleanup);

const me: MeResponse = {
  ok: true,
  user_id: 7,
  tenant_id: 11,
  email: "member@example.com",
  role: "owner",
  actor_type: "user",
  workspaces: [
    {
      id: 11,
      name: "我的空间",
      kind: "personal",
      status: "active",
      plan: "free",
      seat_limit: 1,
      member_count: 1,
      role: "owner",
      created_at: "2026-08-14T00:00:00Z",
      updated_at: "2026-08-14T00:00:00Z",
    },
    {
      id: 12,
      name: "情报团队",
      kind: "team",
      status: "active",
      plan: "team",
      seat_limit: 5,
      member_count: 3,
      role: "member",
      created_at: "2026-08-14T00:00:00Z",
      updated_at: "2026-08-14T00:00:00Z",
    },
  ],
};

describe("WorkspaceSwitcher", () => {
  test("shows the exact active workspace and switches by tenant id", async () => {
    const user = userEvent.setup();
    const onSwitch = vi.fn().mockResolvedValue(undefined);
    render(
      <SidebarProvider>
        <WorkspaceSwitcher me={me} onSwitch={onSwitch} />
      </SidebarProvider>,
    );

    await user.click(screen.getByRole("button", { name: "当前工作区：我的空间" }));
    const team = await screen.findByText("情报团队");
    expect(team).toBeTruthy();
    await user.click(team);
    expect(onSwitch).toHaveBeenCalledOnce();
    expect(onSwitch).toHaveBeenCalledWith(12);
  });

  test("does not invent a selector when the backend omits workspace authority", () => {
    render(
      <SidebarProvider>
        <WorkspaceSwitcher me={{ ...me, workspaces: undefined }} onSwitch={vi.fn()} />
      </SidebarProvider>,
    );
    expect(screen.queryByRole("button", { name: /当前工作区/ })).toBeNull();
  });
});

// @vitest-environment jsdom

import { cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

const apiMock = vi.hoisted(() => ({
  listWorkspaceMembers: vi.fn(),
  listWorkspaceInvites: vi.fn(),
  issueWorkspaceInvite: vi.fn(),
  revokeWorkspaceInvite: vi.fn(),
  updateWorkspaceMemberRole: vi.fn(),
  removeWorkspaceMember: vi.fn(),
  transferWorkspaceOwnership: vi.fn(),
}));

vi.mock("@/shared/api/client", () => ({
  api: apiMock,
  ApiError: class ApiError extends Error {
    status: number;
    constructor(status: number, message: string) {
      super(message);
      this.status = status;
    }
  },
}));

vi.mock("sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

import WorkspaceMembers from "@/pages/WorkspaceMembers";
import type {
  MeResponse,
  WorkspaceInvite,
  WorkspaceMember,
  WorkspaceRole,
} from "@/shared/api/client";

const now = "2026-08-14T00:00:00Z";

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
      name: `Team ${tenantID}`,
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

function member(
  tenantID: number,
  userID: number,
  role: WorkspaceRole,
): WorkspaceMember {
  return {
    tenant_id: tenantID,
    user_id: userID,
    email: `user${userID}@example.com`,
    name: `User ${userID}`,
    role,
    joined_at: now,
  };
}

function invite(tenantID: number, token?: string): WorkspaceInvite {
  return {
    id: 91,
    tenant_id: tenantID,
    email: "new@example.com",
    role: "member",
    issued_by: 1,
    expires_at: "2099-08-21T00:00:00Z",
    created_at: now,
    token,
  };
}

beforeEach(() => {
  vi.resetAllMocks();
  vi.stubGlobal("confirm", vi.fn(() => true));
  apiMock.listWorkspaceInvites.mockResolvedValue([]);
  apiMock.issueWorkspaceInvite.mockResolvedValue(invite(11, "one-time-token"));
  apiMock.revokeWorkspaceInvite.mockResolvedValue({ ok: true });
  apiMock.updateWorkspaceMemberRole.mockResolvedValue({ ok: true });
  apiMock.removeWorkspaceMember.mockResolvedValue({ ok: true });
  apiMock.transferWorkspaceOwnership.mockResolvedValue({ ok: true });
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("WorkspaceMembers role matrix", () => {
  test("Owner can invite Admin, change roles, remove members, and transfer ownership", async () => {
    const user = userEvent.setup();
    const members = [member(11, 1, "owner"), member(11, 2, "admin"), member(11, 3, "member")];
    apiMock.listWorkspaceMembers.mockResolvedValue(members);
    const onAuthorityChanged = vi.fn();

    render(<WorkspaceMembers me={me(11, 1, "owner")} onAuthorityChanged={onAuthorityChanged} />);

    expect(await screen.findByText("User 3")).toBeTruthy();
    expect(within(screen.getByLabelText("角色")).getByRole("option", { name: "Admin" })).toBeTruthy();
    expect(screen.getAllByRole("button", { name: /转让 Owner/ })).toHaveLength(2);
    expect(screen.getAllByRole("button", { name: /移除/ })).toHaveLength(2);

    await user.selectOptions(screen.getByRole("combobox", { name: "修改 user3@example.com 的角色" }), "admin");
    await waitFor(() => {
      expect(apiMock.updateWorkspaceMemberRole).toHaveBeenCalledWith(11, 3, "admin");
    });

    await user.click(screen.getAllByRole("button", { name: /移除/ })[1]);
    await waitFor(() => {
      expect(apiMock.removeWorkspaceMember).toHaveBeenCalledWith(11, 3);
    });

    await user.click(screen.getAllByRole("button", { name: /转让 Owner/ })[1]);
    await waitFor(() => {
      expect(apiMock.transferWorkspaceOwnership).toHaveBeenCalledWith(11, 3);
      expect(onAuthorityChanged).toHaveBeenCalledOnce();
    });
  });

  test("Admin can invite and remove Members but cannot elevate roles or manage Admins", async () => {
    const user = userEvent.setup();
    apiMock.listWorkspaceMembers.mockResolvedValue([
      member(12, 1, "owner"),
      member(12, 2, "admin"),
      member(12, 3, "member"),
    ]);
    apiMock.listWorkspaceInvites.mockResolvedValue([invite(12)]);
    apiMock.issueWorkspaceInvite.mockResolvedValue(invite(12, "admin-one-time-token"));

    render(<WorkspaceMembers me={me(12, 2, "admin")} />);

    expect(await screen.findByText("User 3")).toBeTruthy();
    expect(screen.getByLabelText("邮箱")).toBeTruthy();
    expect(screen.queryByRole("option", { name: "Admin" })).toBeNull();
    expect(screen.queryByRole("combobox", { name: /修改/ })).toBeNull();
    expect(screen.queryByRole("button", { name: /转让 Owner/ })).toBeNull();
    expect(screen.getAllByRole("button", { name: /移除/ })).toHaveLength(1);

    await user.click(screen.getByRole("button", { name: "刷新" }));
    await waitFor(() => {
      expect(apiMock.listWorkspaceMembers.mock.calls.filter(([tenantID]) => tenantID === 12)).toHaveLength(2);
    });

    await user.type(screen.getByLabelText("邮箱"), "second@example.com");
    await user.click(screen.getByRole("button", { name: /签发邀请/ }));
    await waitFor(() => {
      expect(apiMock.issueWorkspaceInvite).toHaveBeenCalledWith(12, "second@example.com", "member");
    });
    expect(await screen.findByText("admin-one-time-token")).toBeTruthy();
    await user.click(screen.getByRole("button", { name: "复制" }));

    await user.click(screen.getByRole("button", { name: "撤销" }));
    await waitFor(() => {
      expect(apiMock.revokeWorkspaceInvite).toHaveBeenCalledWith(12, 91);
    });
  });

  test("Member gets a read-only roster and never requests invitation authority", async () => {
    apiMock.listWorkspaceMembers.mockResolvedValue([
      member(13, 1, "owner"),
      member(13, 3, "member"),
    ]);

    render(<WorkspaceMembers me={me(13, 3, "member")} />);

    expect(await screen.findByText("User 1")).toBeTruthy();
    expect(screen.queryByLabelText("邮箱")).toBeNull();
    expect(screen.queryByRole("button", { name: /移除|转让 Owner|撤销|签发邀请/ })).toBeNull();
    expect(screen.queryByRole("combobox", { name: /修改/ })).toBeNull();
    expect(apiMock.listWorkspaceInvites).not.toHaveBeenCalled();
  });
});

describe("WorkspaceMembers Principal changes", () => {
  test("clears one-time secrets and ignores stale responses after a workspace switch", async () => {
    const user = userEvent.setup();
    let resolveOld!: (value: WorkspaceMember[]) => void;
    const oldMembers = new Promise<WorkspaceMember[]>((resolve) => {
      resolveOld = resolve;
    });
    apiMock.listWorkspaceMembers.mockImplementation((tenantID: number) =>
      tenantID === 21 ? oldMembers : Promise.resolve([member(22, 8, "member")]),
    );

    const view = render(<WorkspaceMembers me={me(21, 7, "owner")} />);
    await user.type(screen.getByLabelText("邮箱"), "new@example.com");
    await user.click(screen.getByRole("button", { name: /签发邀请/ }));
    expect(await screen.findByText("one-time-token")).toBeTruthy();

    view.rerender(<WorkspaceMembers me={me(22, 8, "member")} />);
    expect(await screen.findByText("User 8")).toBeTruthy();
    expect(screen.queryByText("one-time-token")).toBeNull();
    expect(screen.queryByLabelText("邮箱")).toBeNull();

    resolveOld([member(21, 99, "member")]);
    await waitFor(() => expect(screen.queryByText("User 99")).toBeNull());
    expect(apiMock.listWorkspaceMembers).toHaveBeenCalledWith(22);
    expect(apiMock.listWorkspaceInvites).not.toHaveBeenCalledWith(22);
  });
});

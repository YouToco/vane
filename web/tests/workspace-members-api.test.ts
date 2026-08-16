import { afterEach, describe, expect, test, vi } from "vitest";

import { api } from "@/shared/api/client";

afterEach(() => vi.unstubAllGlobals());

describe("workspace member API contract", () => {
  test("uses exact workspace-scoped routes and mutation bodies", async () => {
    const fetchMock = vi.fn().mockImplementation((path: string, init?: RequestInit) => {
      const method = init?.method ?? "GET";
      let body: unknown = { ok: true };
      if (path.endsWith("/members") && method === "GET") {
        body = { members: [{ tenant_id: 42, user_id: 7, role: "owner" }] };
      } else if (path.endsWith("/invites") && method === "GET") {
        body = { invites: [{ id: 91, tenant_id: 42, email: "new@example.com", role: "member" }] };
      } else if (path.endsWith("/invites") && method === "POST") {
        body = { id: 92, tenant_id: 42, email: "second@example.com", role: "admin", token: "one-time" };
      }
      return Promise.resolve(new Response(JSON.stringify(body), {
        status: method === "POST" ? 201 : 200,
        headers: { "Content-Type": "application/json" },
      }));
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(api.listWorkspaceMembers(42)).resolves.toMatchObject([{ tenant_id: 42, user_id: 7 }]);
    await expect(api.listWorkspaceInvites(42)).resolves.toMatchObject([{ id: 91, tenant_id: 42 }]);
    await expect(api.issueWorkspaceInvite(42, "second@example.com", "admin")).resolves.toMatchObject({
      id: 92,
      token: "one-time",
    });
    await api.revokeWorkspaceInvite(42, 91);
    await api.updateWorkspaceMemberRole(42, 8, "member");
    await api.removeWorkspaceMember(42, 9);
    await api.transferWorkspaceOwnership(42, 8);

    expect(fetchMock).toHaveBeenNthCalledWith(1, "/api/workspaces/42/members",
      expect.objectContaining({ credentials: "include" }));
    expect(fetchMock).toHaveBeenNthCalledWith(2, "/api/workspaces/42/invites",
      expect.objectContaining({ credentials: "include" }));
    expect(fetchMock).toHaveBeenNthCalledWith(3, "/api/workspaces/42/invites", expect.objectContaining({
      method: "POST",
      body: JSON.stringify({ email: "second@example.com", role: "admin" }),
    }));
    expect(fetchMock).toHaveBeenNthCalledWith(4, "/api/workspaces/42/invites/91",
      expect.objectContaining({ method: "DELETE" }));
    expect(fetchMock).toHaveBeenNthCalledWith(5, "/api/workspaces/42/members/8", expect.objectContaining({
      method: "PATCH",
      body: JSON.stringify({ role: "member" }),
    }));
    expect(fetchMock).toHaveBeenNthCalledWith(6, "/api/workspaces/42/members/9",
      expect.objectContaining({ method: "DELETE" }));
    expect(fetchMock).toHaveBeenNthCalledWith(7, "/api/workspaces/42/transfer-ownership", expect.objectContaining({
      method: "POST",
      body: JSON.stringify({ user_id: 8 }),
    }));
  });
});

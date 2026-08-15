import { afterEach, describe, expect, test, vi } from "vitest";

import { api } from "@/shared/api/client";

afterEach(() => vi.unstubAllGlobals());

describe("A2A access token API contract", () => {
  test("uses the current session scope, binds reauth, and strips bearers from list projections", async () => {
    const fetchMock = vi.fn().mockImplementation((path: string, init?: RequestInit) => {
      const method = init?.method ?? "GET";
      let body: unknown = { ok: true };
      if (path === "/api/a2a-tokens" && method === "GET") {
        body = { tokens: [{
          id: "list-token",
          tenant_id: 42,
          principal_user_id: 7,
          actor_type: "user",
          scopes: ["content.query"],
          issued_by: 7,
          expires_at: "2099-01-01T00:00:00Z",
          created_at: "2026-08-15T00:00:00Z",
          token: "must-not-survive-list-normalization",
        }] };
      } else if (path === "/api/a2a-tokens" && method === "POST") {
        body = {
          id: "issued-token",
          tenant_id: 42,
          principal_user_id: 7,
          actor_type: "user",
          scopes: ["content.query"],
          issued_by: 7,
          expires_at: "2099-01-01T00:00:00Z",
          created_at: "2026-08-15T00:00:00Z",
          token: "one-time-bearer",
        };
      }
      return Promise.resolve(new Response(JSON.stringify(body), {
        status: method === "POST" ? 201 : 200,
        headers: { "Content-Type": "application/json" },
      }));
    });
    vi.stubGlobal("fetch", fetchMock);

    const listed = await api.listA2AAccessTokens();
    expect(listed).toHaveLength(1);
    expect(listed[0]).not.toHaveProperty("token");

    const issued = await api.issueA2AAccessToken({
      actor_type: "user",
      principal_user_id: 7,
      scopes: ["content.query"],
      expires_in_days: 30,
    }, "recent-session-proof");
    expect(issued.token).toBe("one-time-bearer");
    await api.revokeA2AAccessToken("issued-token");

    expect(fetchMock).toHaveBeenNthCalledWith(1, "/api/a2a-tokens",
      expect.objectContaining({ credentials: "include" }));
    expect(fetchMock).toHaveBeenNthCalledWith(2, "/api/a2a-tokens", expect.objectContaining({
      credentials: "include",
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Vane-Reauth-Token": "recent-session-proof",
      },
      body: JSON.stringify({
        actor_type: "user",
        principal_user_id: 7,
        scopes: ["content.query"],
        expires_in_days: 30,
      }),
    }));
    expect(fetchMock).toHaveBeenNthCalledWith(3, "/api/a2a-tokens/issued-token",
      expect.objectContaining({ credentials: "include", method: "DELETE" }));
  });
});

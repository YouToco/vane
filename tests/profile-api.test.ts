import { afterEach, describe, expect, test, vi } from "vitest";
import { api } from "@/api";

const profile = {
  industry: "AI",
  occupation: "Founder",
  tags: null,
  removed_tags: null,
  summary: "",
  created_at: "2026-07-27T00:00:00Z",
  updated_at: "2026-07-27T01:00:00Z",
};

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("profile API contract", () => {
  test("PATCH carries the CAS body and intent idempotency header", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify(profile), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const result = await api.updateProfile(
      { expected_updated_at: null, industry: "AI" },
      "profile-edit-123",
    );

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/profile",
      expect.objectContaining({
        credentials: "include",
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": "profile-edit-123",
        },
        body: JSON.stringify({
          expected_updated_at: null,
          industry: "AI",
        }),
      }),
    );
    expect(result.tags).toEqual([]);
    expect(result.removed_tags).toEqual([]);
  });

  test("reads the edits envelope and keeps server undo authority", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            edits: [
              {
                id: "rev-1",
                actor: "self",
                kind: "edit",
                created_at: "2026-07-27T01:00:00Z",
                changes: null,
                undoable: true,
              },
            ],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      ),
    );

    await expect(api.profileEdits(20)).resolves.toEqual({
      edits: [
        {
          id: "rev-1",
          actor: "self",
          kind: "edit",
          created_at: "2026-07-27T01:00:00Z",
          changes: [],
          undoable: true,
        },
      ],
    });
  });

  test("POST undo carries the current token and a new intent key", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify(profile), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await api.undoProfileEdit(
      "revision/one",
      "2026-07-27T01:00:00Z",
      "profile-undo-123",
    );

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/profile/edits/revision%2Fone/undo",
      expect.objectContaining({
        credentials: "include",
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": "profile-undo-123",
        },
        body: JSON.stringify({
          expected_updated_at: "2026-07-27T01:00:00Z",
        }),
      }),
    );
  });
});

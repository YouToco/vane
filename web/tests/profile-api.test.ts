import { afterEach, describe, expect, test, vi } from "vitest";
import { api } from "@/shared/api/client";

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

  test("reads claim and event envelopes without inventing server authority", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            version: 4,
            profile_epoch: 7,
            restore_allowed: true,
            claims: [
              {
                id: "claim-1",
                field: "tag",
                value: "Agent",
                source: { state: "evidence" },
                active: 1,
                pinned: false,
                created_at: "2026-07-27T01:00:00Z",
              },
              {
                id: "claim-2",
                field: "tag",
                value: "Future",
                source: {
                  state: "future_provenance",
                  ref_type: "internal",
                  ref: "must-not-leak",
                },
                active: true,
                pinned: true,
                created_at: "2026-07-27T01:00:00Z",
              },
              {
                id: "claim-3",
                field: "tag",
                value: "Missing",
                active: true,
                pinned: false,
                created_at: "2026-07-27T01:00:00Z",
              },
            ],
            events: [
              {
                id: "event-1",
                kind: "pin",
                target_claim_id: "claim-1",
                created_at: "2026-07-27T02:00:00Z",
                revoked: false,
                revocable: 1,
              },
            ],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      ),
    );

    const result = await api.profileClaims();
    expect(fetch).toHaveBeenCalledWith(
      "/api/profile/claims?event_limit=20",
      expect.objectContaining({ credentials: "include" }),
    );
    expect(result).toMatchObject({
      version: 4,
      profile_epoch: 7,
      restore_allowed: true,
      events: [{ id: "event-1", revoked: false, revocable: false }],
      events_has_more: false,
    });
    expect(result.events_next_cursor).toBeUndefined();
    expect(result.claims[0]).toMatchObject({
      id: "claim-1",
      active: false,
      pinned: false,
    });
    expect(result.claims.map((claim) => claim.source)).toEqual([
      { state: "evidence" },
      { state: "source_unavailable" },
      { state: "source_unavailable" },
    ]);
  });

  test("loads an older event page with an encoded opaque cursor", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          version: 4,
          claims: [],
          events: [],
          events_has_more: true,
          events_next_cursor: "v1:/older page",
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    const result = await api.profileClaims("v1:/current page");

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/profile/claims?event_limit=20&event_cursor=v1%3A%2Fcurrent%20page",
      expect.objectContaining({ credentials: "include" }),
    );
    expect(result).toMatchObject({
      events_has_more: true,
      events_next_cursor: "v1:/older page",
    });
  });

  test("claim actions carry CAS, exact body, and the supplied idempotency key", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          version: 5,
          event_id: "event-2",
          profile,
          claims_complete: false,
          claims: [
            {
              id: "claim-2",
              field: "industry",
              value: "AI",
              source: { state: "new_backend_state", ref: "private-ref" },
              active: true,
              pinned: false,
              created_at: "2026-07-27T01:00:00Z",
            },
          ],
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    const result = await api.applyProfileClaimAction(
      {
        expected_epoch: 7,
        expected_version: 4,
        action: "correct",
        claim_id: "claim/1",
        value: "Founder",
      },
      "profile-claim-123",
    );

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/profile/claims/actions",
      expect.objectContaining({
        credentials: "include",
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": "profile-claim-123",
        },
        body: JSON.stringify({
          expected_epoch: 7,
          expected_version: 4,
          action: "correct",
          claim_id: "claim/1",
          value: "Founder",
        }),
      }),
    );
    expect(result.claims[0]?.source).toEqual({
      state: "source_unavailable",
    });
    expect(result.claims_complete).toBe(false);
  });

  test("reset carries epoch CAS, fixed scope, and the supplied idempotency key", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          action: "reset",
          profile_epoch: 8,
          version: 5,
          event_id: "epoch-event-1",
          profile: { ...profile, tags: null, removed_tags: null },
          restore_allowed: true,
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    const result = await api.applyProfileEpochAction(
      {
        expected_epoch: 7,
        expected_version: 4,
        action: "reset",
        scope: "history_learning",
      },
      "profile-epoch-reset-123",
    );

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/profile/epochs/actions",
      expect.objectContaining({
        credentials: "include",
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": "profile-epoch-reset-123",
        },
        body: JSON.stringify({
          expected_epoch: 7,
          expected_version: 4,
          action: "reset",
          scope: "history_learning",
        }),
      }),
    );
    expect(result).toMatchObject({
      action: "reset",
      profile_epoch: 8,
      version: 5,
      event_id: "epoch-event-1",
      restore_allowed: true,
      profile: { tags: [], removed_tags: [] },
    });
  });

  test("restore sends no reset scope and keeps false server authority", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          action: "restore",
          profile_epoch: 9,
          version: 6,
          event_id: "epoch-event-2",
          profile,
          restore_allowed: false,
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    const result = await api.applyProfileEpochAction(
      {
        expected_epoch: 8,
        expected_version: 5,
        action: "restore",
      },
      "profile-epoch-restore-123",
    );

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/profile/epochs/actions",
      expect.objectContaining({
        body: JSON.stringify({
          expected_epoch: 8,
          expected_version: 5,
          action: "restore",
        }),
      }),
    );
    expect(JSON.parse(fetchMock.mock.calls[0]?.[1]?.body as string)).not.toHaveProperty(
      "scope",
    );
    expect(result.restore_allowed).toBe(false);
  });
});

describe("telegram API contract", () => {
  test("uses session-scoped status, link, unlink, and test routes", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ enabled: true, ready: true, bound: false }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            deep_link: "https://t.me/vane_bot?start=opaque",
            expires_at: "2026-08-15T05:00:00Z",
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      )
      .mockImplementation(() =>
        Promise.resolve(
          new Response(JSON.stringify({ ok: true }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        ),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(api.telegramStatus()).resolves.toMatchObject({ ready: true });
    await expect(api.telegramLink()).resolves.toMatchObject({
      deep_link: "https://t.me/vane_bot?start=opaque",
    });
    await expect(api.telegramUnlink()).resolves.toEqual({ ok: true });
    await expect(api.telegramTest()).resolves.toEqual({ ok: true });

    expect(fetchMock.mock.calls.map(([path]) => path)).toEqual([
      "/api/telegram/status",
      "/api/telegram/link",
      "/api/telegram/link",
      "/api/telegram/test",
    ]);
    expect(fetchMock.mock.calls[0]?.[1]).toMatchObject({ credentials: "include" });
    expect(fetchMock.mock.calls[1]?.[1]).toMatchObject({ method: "POST" });
    expect(fetchMock.mock.calls[2]?.[1]).toMatchObject({ method: "DELETE" });
    expect(fetchMock.mock.calls[3]?.[1]).toMatchObject({ method: "POST" });
  });
});

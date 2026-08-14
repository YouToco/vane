// @vitest-environment jsdom

import React from "react";
import {
  act,
  cleanup,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

const apiMock = vi.hoisted(() => ({
  profile: vi.fn(),
  profileEdits: vi.fn(),
  profileClaims: vi.fn(),
  updateProfile: vi.fn(),
  undoProfileEdit: vi.fn(),
  applyProfileClaimAction: vi.fn(),
  applyProfileEpochAction: vi.fn(),
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

const copy = {
  title: "用户画像",
  desc: "说明",
  reload: "重新加载",
  confirmReload: "确认重新加载",
  confirmUndoDirty: "确认撤销并放弃草稿",
  confirmClaimDirty: "确认操作依据并放弃草稿",
  learningResetTitle: "学习版本",
  learningResetNote: "重置说明",
  epochAuditNote: "旧记录只读审计",
  currentEpoch: "当前版本 {epoch}",
  epochUnavailable: "无法读取学习版本",
  resetLearning: "重置历史学习",
  restoreLearning: "恢复上一次重置",
  restoreAvailable: "当前可以恢复",
  restoreUnavailable: "当前不能恢复",
  resetConfirmTitle: "确认重置历史学习？",
  resetConfirmNote: "确认重置说明",
  restoreConfirmTitle: "确认恢复重置前的画像？",
  restoreConfirmNote: "确认恢复说明",
  cancelEpochAction: "取消学习版本操作",
  confirmReset: "确认重置",
  confirmRestore: "确认恢复",
  resetDone: "已清除历史学习，将仅从此后的反馈重新学习。",
  restoreDone: "已恢复重置前的画像，并进入新的学习版本。",
  epochDirty: "请先处理未保存修改",
  epochActionFailed: "学习版本操作失败",
  claimsTitle: "画像依据",
  claimsNote: "依据说明",
  loadingClaims: "加载画像依据",
  noClaims: "暂无画像依据",
  claimFieldTag: "标签",
  claimFieldSummary: "系统摘要",
  sourceManual: "人工纠正",
  sourceUnavailable: "历史数据，原始来源不可用",
  sourceEvidence: "由反馈演化生成",
  sourceEvidenceRange: "由反馈演化生成（处理范围 {range}）",
  claimActive: "生效中",
  claimInactive: "已失效",
  claimPinned: "已固定",
  claimCorrect: "纠正",
  claimSuppress: "排除",
  claimPin: "固定",
  claimCorrectionLabel: "纠正{field}",
  claimConfirmCorrection: "确认纠正",
  claimCorrectionHint: "输入一条 1–{limit} 个字符的短判断",
  claimCorrectionTooLong: "纠正不能超过 {limit}",
  claimHistory: "依据操作历史",
  noClaimEvents: "无依据操作",
  claimKindCorrect: "纠正记录",
  claimKindSuppress: "排除记录",
  claimKindPin: "固定记录",
  claimKindRevoke: "撤销操作记录",
  claimRevoked: "已撤销",
  claimRevoke: "撤销此操作",
  claimActionFailed: "依据操作失败",
  editTitle: "首次创建画像",
  industry: "行业",
  occupation: "职业",
  notGenerated: "画像尚未生成",
  tags: "兴趣标签",
  tagHint: "标签提示",
  currentTags: "当前兴趣标签",
  removeTag: "移除标签 {tag}",
  tagSeparator: "、",
  emptyValue: "未填写",
  editChange: "{field}：{before} → {after}",
  tagPlaceholder: "输入标签",
  addTag: "添加",
  updatedAtValue: "更新于 {time}",
  notSavedYet: "尚未创建画像",
  discard: "放弃修改",
  save: "保存修改",
  saving: "保存中",
  saveFailed: "保存失败",
  saved: "画像已更新",
  conflict: "画像已更新，重新加载后再改。",
  tooManyTags: "最多12个标签",
  tagTooLong: "标签 {tag} 太长",
  invalidTagControl: "标签不能包含控制字符",
  emptyTag: "标签不能为空",
  systemExplanation: "系统学习解释",
  summaryNote: "只读说明",
  noSummary: "无摘要",
  removedTags: "人工禁区",
  removedNote: "禁区只读",
  noRemovedTags: "无禁区",
  editHistory: "最近人工修改",
  legacyEditHistory: "旧版修正记录",
  legacyEditNote: "旧记录只读",
  kindEdit: "编辑",
  kindUndo: "撤销记录",
  loadingHistory: "加载记录",
  noEdits: "无修改",
  editAt: "{actor} · {time}",
  you: "你",
  noChangeDetails: "无详情",
  undo: "撤销",
  undoing: "撤销中",
  undoFailed: "撤销失败",
};

vi.mock("@/i18n", () => ({
  useI18n: () => ({
    locale: "zh",
    t: {
      app: {
        profile: copy,
        common: { loadFailed: "加载失败", loadMore: "加载更多" },
      },
    },
  }),
  fmt: (template: string, vars: Record<string, string | number>) =>
    template.replace(/\{(\w+)\}/g, (_: string, key: string) =>
      String(vars[key] ?? ""),
    ),
}));

vi.mock("@/components/ui/card", () => ({
  Card: ({ children }: { children: React.ReactNode }) => <section>{children}</section>,
  CardContent: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
    <div {...props}>{children}</div>
  ),
  CardHeader: ({ children }: { children: React.ReactNode }) => <header>{children}</header>,
  CardTitle: ({ children }: { children: React.ReactNode }) => <h3>{children}</h3>,
}));
vi.mock("@/components/ui/button", () => ({
  Button: (props: React.ButtonHTMLAttributes<HTMLButtonElement>) => <button {...props} />,
}));
vi.mock("@/components/ui/badge", () => ({
  Badge: ({ children }: { children: React.ReactNode }) => <span>{children}</span>,
}));
vi.mock("@/components/ui/alert", () => ({
  Alert: (props: React.HTMLAttributes<HTMLDivElement>) => <div {...props} />,
  AlertDescription: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
    <div {...props}>{children}</div>
  ),
}));
vi.mock("@/components/ui/skeleton", () => ({
  Skeleton: () => <span>loading</span>,
}));
vi.mock("@/components/ui/separator", () => ({
  Separator: () => <hr />,
}));
vi.mock("@/components/ui/input", () => ({
  Input: (props: React.InputHTMLAttributes<HTMLInputElement>) => <input {...props} />,
}));
vi.mock("@/components/ui/label", () => ({
  Label: (props: React.LabelHTMLAttributes<HTMLLabelElement>) => <label {...props} />,
}));
vi.mock("@/components/ui/collapsible", () => ({
  Collapsible: ({ children, open }: { children: React.ReactNode; open: boolean }) => (
    <div data-open={open}>{children}</div>
  ),
  CollapsibleTrigger: (props: React.ButtonHTMLAttributes<HTMLButtonElement>) => (
    <button {...props} />
  ),
  CollapsibleContent: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
}));

import Profile, { retireProfileClaims } from "@/pages/Profile";
import { ApiError, type ProfileClaimsResponse } from "@/shared/api/client";

const baseProfile = {
  industry: "AI",
  occupation: "独立开发者",
  tags: ["Agent"],
  removed_tags: ["旧闻"],
  summary: "系统摘要",
  created_at: "2026-07-01T00:00:00Z",
  updated_at: "2026-07-27T01:00:00Z",
};

function pageText(): string {
  return document.body.textContent ?? "";
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function renderLoaded() {
  const user = userEvent.setup();
  const view = render(<Profile />);
  await waitFor(() => expect(apiMock.profileEdits).toHaveBeenCalledTimes(1));
  return { user, ...view };
}

function button(label: string): HTMLButtonElement {
  return screen.getByRole("button", { name: label }) as HTMLButtonElement;
}

test("retires every claim and event when an epoch action succeeds", () => {
  const retired = retireProfileClaims(
    {
      profile_epoch: 7,
      version: 3,
      restore_allowed: false,
      claims: [claim({ id: "old-claim", value: "旧依据" })],
      events: [
        {
          id: "old-event",
          kind: "pin",
          target_claim_id: "old-claim",
          created_at: "2026-07-27T02:00:00Z",
          revoked: false,
          revocable: true,
        },
      ],
      events_has_more: true,
      events_next_cursor: "old-cursor",
    },
    {
      action: "reset",
      profile_epoch: 8,
      version: 4,
      event_id: "epoch-event",
      profile: baseProfile,
      restore_allowed: true,
    },
  );

  expect(retired).toEqual({
    profile_epoch: 8,
    version: 4,
    restore_allowed: true,
    claims: [],
    events: [],
    events_has_more: false,
  });
});

function claim(
  overrides: Record<string, unknown> = {},
) {
  return {
    id: "claim-1",
    field: "tag",
    value: "Agent",
    source: { state: "evidence" },
    active: true,
    pinned: false,
    created_at: "2026-07-27T01:00:00Z",
    ...overrides,
  };
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("profile claim authority UI", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    apiMock.profile.mockResolvedValue(baseProfile);
    apiMock.profileEdits.mockResolvedValue({ edits: [] });
    apiMock.profileClaims.mockResolvedValue({
      version: 3,
      profile_epoch: 7,
      restore_allowed: false,
      claims: [],
      events: [],
    });
    apiMock.updateProfile.mockResolvedValue(baseProfile);
    apiMock.applyProfileClaimAction.mockResolvedValue({
      version: 4,
      event_id: "event-1",
      profile: baseProfile,
      claims: [],
    });
    apiMock.applyProfileEpochAction.mockResolvedValue({
      action: "reset",
      profile_epoch: 8,
      version: 4,
      event_id: "epoch-event-1",
      profile: baseProfile,
      restore_allowed: true,
    });
    vi.spyOn(window, "confirm").mockReturnValue(true);
  });

  test("fails closed: existing profiles have no legacy edit or undo write path", async () => {
    apiMock.profileEdits.mockResolvedValue({
      edits: [
        {
          id: "old-1",
          actor: "self",
          kind: "edit",
          created_at: "2026-07-27T01:00:00Z",
          changes: [],
          undoable: true,
        },
      ],
    });
    await renderLoaded();

    expect(pageText()).toContain("旧版修正记录");
    expect(pageText()).toContain("旧记录只读");
    expect(screen.queryAllByRole("textbox")).toHaveLength(0);
    expect(screen.queryByRole("button", { name: "保存修改" })).toBeNull();
    expect(screen.queryByRole("button", { name: "撤销" })).toBeNull();
    expect(apiMock.updateProfile).not.toHaveBeenCalled();
    expect(apiMock.undoProfileEdit).not.toHaveBeenCalled();
  });

  test("keeps one-time onboarding only when both profile and claims are absent", async () => {
    apiMock.profile.mockRejectedValue(new ApiError(404, "missing profile"));
    apiMock.profileClaims.mockRejectedValue(new ApiError(404, "missing claims"));
    const { user } = await renderLoaded();
    expect(pageText()).toContain("画像尚未生成");
    await user.type(screen.getByLabelText("行业"), "AI");
    await user.click(button("保存修改"));

    await waitFor(() =>
      expect(apiMock.updateProfile).toHaveBeenCalledWith(
        { expected_updated_at: null, industry: "AI" },
        expect.stringMatching(/^profile-edit-/),
      ),
    );
    await waitFor(() => expect(apiMock.profileClaims).toHaveBeenCalledTimes(2));
  });

  test("treats a legacy backend without pagination fields as a complete first page", async () => {
    apiMock.profileClaims.mockResolvedValue({
      version: 2,
      claims: [claim({ value: "旧后端依据" })],
      events: [],
    });

    await renderLoaded();

    expect(pageText()).toContain("旧后端依据");
    expect(screen.queryByRole("button", { name: "加载更多" })).toBeNull();
    expect(apiMock.profileClaims).toHaveBeenCalledTimes(1);
  });

  test("fails closed when the read response has no epoch authority", async () => {
    apiMock.profileClaims.mockResolvedValue({
      version: 2,
      claims: [claim()],
      events: [
        {
          id: "event-no-epoch",
          kind: "pin",
          target_claim_id: "claim-1",
          created_at: "2026-07-27T02:00:00Z",
          revoked: false,
          revocable: true,
        },
      ],
    });

    await renderLoaded();

    await screen.findByText("暂时无法读取当前学习版本，请重新加载后再试。");
    expect(screen.queryByRole("button", { name: "重置历史学习" })).toBeNull();
    expect(screen.queryByRole("button", { name: "恢复上一次重置" })).toBeNull();
    expect(screen.queryByRole("button", { name: "纠正" })).toBeNull();
    expect(screen.queryByRole("button", { name: "排除" })).toBeNull();
    expect(screen.queryByRole("button", { name: "固定" })).toBeNull();
    expect(screen.queryByRole("button", { name: "撤销此操作" })).toBeNull();
    expect(apiMock.applyProfileEpochAction).not.toHaveBeenCalled();
    expect(apiMock.applyProfileClaimAction).not.toHaveBeenCalled();
  });

  test("requires an explicit second confirmation and cancel sends no request", async () => {
    const { user } = await renderLoaded();

    await user.click(await screen.findByRole("button", { name: "重置历史学习" }));
    expect(screen.getByRole("alertdialog", { name: "确认重置历史学习？" })).toBeTruthy();
    expect(apiMock.applyProfileEpochAction).not.toHaveBeenCalled();

    const cancel = button("取消");
    await waitFor(() => expect(document.activeElement).toBe(cancel));
    await user.click(cancel);
    expect(
      screen.queryByRole("alertdialog", { name: "确认重置历史学习？" }),
    ).toBeNull();
    await waitFor(() =>
      expect(document.activeElement).toBe(button("重置历史学习")),
    );
    expect(apiMock.applyProfileEpochAction).not.toHaveBeenCalled();
  });

  test("Escape closes the restore confirmation and returns focus to its trigger", async () => {
    apiMock.profileClaims.mockResolvedValue({
      version: 9,
      profile_epoch: 8,
      restore_allowed: true,
      claims: [],
      events: [],
    });
    const { user } = await renderLoaded();

    await user.click(await screen.findByRole("button", { name: "恢复上一次重置" }));
    await waitFor(() => expect(document.activeElement).toBe(button("取消")));
    await user.keyboard("{Escape}");

    expect(
      screen.queryByRole("alertdialog", { name: "确认恢复重置前的画像？" }),
    ).toBeNull();
    await waitFor(() =>
      expect(document.activeElement).toBe(button("恢复上一次重置")),
    );
    expect(apiMock.applyProfileEpochAction).not.toHaveBeenCalled();
  });

  test("resets with exact epoch authority and refreshes projections after success", async () => {
    apiMock.profileClaims
      .mockResolvedValueOnce({
        version: 3,
        profile_epoch: 7,
        restore_allowed: false,
        claims: [claim({ id: "old-reset-claim", value: "旧 reset 依据" })],
        events: [
          {
            id: "old-reset-event",
            kind: "pin",
            target_claim_id: "old-reset-claim",
            created_at: "2026-07-27T02:00:00Z",
            revoked: false,
            revocable: true,
          },
        ],
      })
      .mockReturnValueOnce(new Promise(() => {}));
    const { user } = await renderLoaded();

    await screen.findByText("旧 reset 依据");
    await user.click(await screen.findByRole("button", { name: "重置历史学习" }));
    await user.click(button("确认重置"));

    await waitFor(() =>
      expect(apiMock.applyProfileEpochAction).toHaveBeenCalledWith(
        {
          expected_epoch: 7,
          expected_version: 3,
          action: "reset",
          scope: "history_learning",
        },
        expect.stringMatching(/^profile-epoch-reset-/),
      ),
    );
    const resetNotice = await screen.findByText(
      "已清除历史学习，将仅从此后的反馈重新学习。",
    );
    await waitFor(() =>
      expect(document.activeElement).toBe(resetNotice.closest('[role="status"]')),
    );
    expect(screen.queryByText("旧 reset 依据")).toBeNull();
    expect(screen.queryByRole("button", { name: "排除" })).toBeNull();
    expect(screen.queryByRole("button", { name: "撤销此操作" })).toBeNull();
    expect(apiMock.applyProfileClaimAction).not.toHaveBeenCalled();
    await waitFor(() => expect(apiMock.profileClaims).toHaveBeenCalledTimes(2));
  });

  test("does not merge an in-flight old-epoch page after reset succeeds", async () => {
    const oldPage = deferred<ProfileClaimsResponse>();
    const epochResult = deferred<{
      action: "reset";
      profile_epoch: number;
      version: number;
      event_id: string;
      profile: typeof baseProfile;
      restore_allowed: boolean;
    }>();
    apiMock.applyProfileEpochAction.mockReturnValue(epochResult.promise);
    apiMock.profileClaims
      .mockResolvedValueOnce({
        version: 3,
        profile_epoch: 7,
        restore_allowed: false,
        claims: [claim({ id: "current", value: "当前 epoch 依据" })],
        events: [],
        events_has_more: true,
        events_next_cursor: "cursor/old-epoch",
      })
      .mockReturnValueOnce(oldPage.promise)
      .mockReturnValueOnce(new Promise(() => {}));
    const { user } = await renderLoaded();

    await user.click(button("加载更多"));
    await waitFor(() =>
      expect(apiMock.profileClaims).toHaveBeenNthCalledWith(
        2,
        "cursor/old-epoch",
      ),
    );
    await user.click(button("重置历史学习"));
    await user.click(button("确认重置"));
    await waitFor(() =>
      expect(apiMock.applyProfileEpochAction).toHaveBeenCalledTimes(1),
    );

    // Resolve the action continuation first, then the stale page in the same
    // React act. Passive reload effects cannot run between these resolutions,
    // so this fails if success does not invalidate pagination synchronously.
    await act(async () => {
      epochResult.resolve({
        action: "reset",
        profile_epoch: 8,
        version: 4,
        event_id: "epoch-event-race",
        profile: baseProfile,
        restore_allowed: true,
      });
      await Promise.resolve();
      oldPage.resolve({
        version: 3,
        profile_epoch: 7,
        restore_allowed: false,
        claims: [claim({ id: "retired", value: "不得回流的旧 epoch 依据" })],
        events: [
          {
            id: "retired-event",
            kind: "suppress",
            target_claim_id: "retired",
            created_at: "2026-07-01T03:00:00Z",
            revoked: false,
            revocable: true,
          },
        ],
        events_has_more: false,
      });
      await oldPage.promise;
    });

    await screen.findByText("已清除历史学习，将仅从此后的反馈重新学习。");
    expect(screen.queryByText("不得回流的旧 epoch 依据")).toBeNull();
    expect(screen.queryByText("当前 epoch 依据")).toBeNull();
    expect(screen.queryByRole("button", { name: "撤销此操作" })).toBeNull();
  });

  test("shows restore only from server authority and sends no reset scope", async () => {
    apiMock.profileClaims
      .mockResolvedValueOnce({
        version: 9,
        profile_epoch: 8,
        restore_allowed: true,
        claims: [claim({ id: "old-restore-claim", value: "旧 restore 依据" })],
        events: [
          {
            id: "old-restore-event",
            kind: "pin",
            target_claim_id: "old-restore-claim",
            created_at: "2026-07-27T02:00:00Z",
            revoked: false,
            revocable: true,
          },
        ],
      })
      .mockReturnValueOnce(new Promise(() => {}));
    apiMock.applyProfileEpochAction.mockResolvedValue({
      action: "restore",
      profile_epoch: 9,
      version: 10,
      event_id: "epoch-event-2",
      profile: baseProfile,
      restore_allowed: false,
    });
    const { user } = await renderLoaded();

    await screen.findByText("旧 restore 依据");
    await user.click(await screen.findByRole("button", { name: "恢复上一次重置" }));
    expect(apiMock.applyProfileEpochAction).not.toHaveBeenCalled();
    await user.click(button("确认恢复"));

    await waitFor(() =>
      expect(apiMock.applyProfileEpochAction).toHaveBeenCalledWith(
        {
          expected_epoch: 8,
          expected_version: 9,
          action: "restore",
        },
        expect.stringMatching(/^profile-epoch-restore-/),
      ),
    );
    expect(apiMock.applyProfileEpochAction.mock.calls[0]?.[0]).not.toHaveProperty(
      "scope",
    );
    const restoreNotice = await screen.findByText(
      "已恢复重置前的画像，并进入新的学习版本。",
    );
    await waitFor(() =>
      expect(document.activeElement).toBe(restoreNotice.closest('[role="status"]')),
    );
    expect(screen.queryByText("旧 restore 依据")).toBeNull();
    expect(screen.queryByRole("button", { name: "排除" })).toBeNull();
    expect(screen.queryByRole("button", { name: "撤销此操作" })).toBeNull();
    expect(apiMock.applyProfileClaimAction).not.toHaveBeenCalled();
  });

  test("keeps the rendered authority on 409 until the user explicitly reloads", async () => {
    apiMock.applyProfileEpochAction.mockRejectedValue(
      new ApiError(409, "stale epoch"),
    );
    const { user } = await renderLoaded();

    await user.click(await screen.findByRole("button", { name: "重置历史学习" }));
    await user.click(button("确认重置"));

    const conflictMessage = await screen.findByText(
      "画像已更新，重新加载后再改。",
    );
    const conflictReload = conflictMessage
      .closest('[role="alert"]')
      ?.querySelector("button");
    await waitFor(() => expect(document.activeElement).toBe(conflictReload));
    expect(pageText()).toContain("当前版本 7");
    expect(apiMock.profileClaims).toHaveBeenCalledTimes(1);
    expect(apiMock.applyProfileEpochAction).toHaveBeenCalledTimes(1);
    const reset = button("重置历史学习");
    expect(reset.disabled).toBe(true);
    await user.click(reset);
    expect(apiMock.applyProfileEpochAction).toHaveBeenCalledTimes(1);

    await user.click(screen.getAllByRole("button", { name: "重新加载" })[1]!);
    await waitFor(() => expect(apiMock.profileClaims).toHaveBeenCalledTimes(2));
    expect(apiMock.applyProfileEpochAction).toHaveBeenCalledTimes(1);
  });

  test("shows honest per-sentence sources and uses active as the action authority", async () => {
    apiMock.profileClaims.mockResolvedValue({
      version: 7,
      profile_epoch: 7,
      claims: [
        claim({
          id: "summary-1",
          field: "summary",
          value: "偏好短篇分析",
          source: {
            state: "evidence",
            ref_type: "feedback_range",
            ref: "feedbacks:(4,9]",
          },
        }),
        claim({
          id: "summary-2",
          field: "summary",
          value: "历史污染句",
          source: { state: "source_unavailable" },
          active: false,
        }),
        claim({
          id: "tag-1",
          source: { state: "manual" },
          active: false,
        }),
      ],
      events: [],
    });
    const { user } = await renderLoaded();

    expect(pageText()).toContain("由反馈演化生成（处理范围 5–9）");
    expect(pageText()).toContain("历史数据，原始来源不可用");
    expect(pageText()).toContain("人工纠正");
    expect(pageText()).toContain("已失效");
    expect(screen.getAllByRole("button", { name: "纠正" })).toHaveLength(1);
    await user.click(button("纠正"));
    const correction = screen.getByDisplayValue(
      "偏好短篇分析",
    ) as HTMLInputElement;
    expect(correction.maxLength).toBe(240);
    await user.clear(correction);
    await user.type(correction, "偏好带结论的短篇分析");
    await user.click(button("确认纠正"));
    await waitFor(() =>
      expect(apiMock.applyProfileClaimAction).toHaveBeenCalledWith(
        {
          expected_epoch: 7,
          expected_version: 7,
          action: "correct",
          claim_id: "summary-1",
          value: "偏好带结论的短篇分析",
        },
        expect.stringMatching(/^profile-claim-/),
      ),
    );
  });

  test("sends expected version and random idempotency key, then refreshes", async () => {
    apiMock.profileClaims.mockResolvedValue({
      version: 11,
      profile_epoch: 7,
      claims: [claim({ id: "claim/1" })],
      events: [],
    });
    const { user } = await renderLoaded();
    await user.click(button("排除"));

    await waitFor(() =>
      expect(apiMock.applyProfileClaimAction).toHaveBeenCalledWith(
        {
          expected_epoch: 7,
          expected_version: 11,
          action: "suppress",
          claim_id: "claim/1",
        },
        expect.stringMatching(/^profile-claim-/),
      ),
    );
    await waitFor(() => expect(apiMock.profileClaims).toHaveBeenCalledTimes(2));
  });

  test("keeps rendered claims on 409 and waits for explicit refresh", async () => {
    apiMock.profileClaims.mockResolvedValue({
      version: 12,
      profile_epoch: 7,
      claims: [claim({ field: "industry", value: "机器人" })],
      events: [],
    });
    apiMock.applyProfileClaimAction.mockRejectedValue(new ApiError(409, "conflict"));
    const { user } = await renderLoaded();
    await user.click(button("固定"));

    const conflictMessage = await screen.findByText(
      "画像已更新，重新加载后再改。",
    );
    const conflictReload = conflictMessage
      .closest('[role="alert"]')
      ?.querySelector("button");
    await waitFor(() => expect(document.activeElement).toBe(conflictReload));
    expect(pageText()).toContain("机器人");
    expect(apiMock.profileClaims).toHaveBeenCalledTimes(1);
    const pin = button("固定");
    expect(pin.disabled).toBe(true);
    await user.click(pin);
    expect(apiMock.applyProfileClaimAction).toHaveBeenCalledTimes(1);
    await user.click(
      screen.getAllByRole("button", { name: "重新加载" })[1],
    );
    await waitFor(() => expect(apiMock.profileClaims).toHaveBeenCalledTimes(2));
  });

  test("offers revoke only for server-revocable events and refreshes afterward", async () => {
    apiMock.profileClaims.mockResolvedValue({
      version: 13,
      profile_epoch: 7,
      claims: [claim({ id: "1", pinned: true, source: { state: "manual" } })],
      events: [
        {
          id: "event/1",
          kind: "pin",
          target_claim_id: "1",
          created_at: "2026-07-27T02:00:00Z",
          revoked: false,
          revocable: true,
        },
        {
          id: "event/0",
          kind: "correct",
          target_claim_id: "1",
          created_at: "2026-07-27T01:00:00Z",
          revoked: false,
          revocable: false,
        },
      ],
    });
    const { user } = await renderLoaded();
    expect(
      screen.getAllByRole("button", { name: "撤销此操作" }),
    ).toHaveLength(1);
    await user.click(button("撤销此操作"));

    await waitFor(() =>
      expect(apiMock.applyProfileClaimAction).toHaveBeenCalledWith(
        {
          expected_epoch: 7,
          expected_version: 13,
          action: "revoke",
          event_id: "event/1",
        },
        expect.stringMatching(/^profile-claim-/),
      ),
    );
    await waitFor(() => expect(apiMock.profileClaims).toHaveBeenCalledTimes(2));
  });

  test("appends and deduplicates an older page, then revokes its hydrated event", async () => {
    apiMock.profileClaims
      .mockResolvedValueOnce({
        version: 21,
        profile_epoch: 7,
        claims: [claim({ id: "current", value: "当前依据" })],
        events: [
          {
            id: "event-new",
            kind: "pin",
            target_claim_id: "current",
            created_at: "2026-07-27T03:00:00Z",
            revoked: false,
            revocable: false,
          },
        ],
        events_has_more: true,
        events_next_cursor: "cursor/older",
      })
      .mockResolvedValueOnce({
        version: 21,
        profile_epoch: 7,
        claims: [
          claim({ id: "current", value: "当前依据（同 ID 更新）" }),
          claim({
            id: "old-target",
            value: "很早以前的依据",
            active: false,
            source: { state: "source_unavailable" },
          }),
        ],
        events: [
          {
            id: "event-new",
            kind: "pin",
            target_claim_id: "current",
            created_at: "2026-07-27T03:00:00Z",
            revoked: false,
            revocable: false,
          },
          {
            id: "event-old",
            kind: "suppress",
            target_claim_id: "old-target",
            created_at: "2026-07-01T03:00:00Z",
            revoked: false,
            revocable: true,
          },
        ],
        events_has_more: false,
      })
      .mockResolvedValue({
        version: 22,
        profile_epoch: 7,
        claims: [claim({ id: "current", value: "撤销后依据" })],
        events: [],
        events_has_more: false,
      });
    const { user } = await renderLoaded();

    await user.click(button("加载更多"));
    await waitFor(() =>
      expect(apiMock.profileClaims).toHaveBeenNthCalledWith(2, "cursor/older"),
    );
    expect(screen.getAllByText("固定记录")).toHaveLength(1);
    expect(screen.getAllByText("排除记录")).toHaveLength(1);
    expect(screen.getAllByText("当前依据（同 ID 更新）", { exact: true })).toHaveLength(1);
    expect(pageText()).toContain("历史数据，原始来源不可用");
    expect(screen.queryByRole("button", { name: "加载更多" })).toBeNull();

    await user.click(button("撤销此操作"));
    await waitFor(() =>
      expect(apiMock.applyProfileClaimAction).toHaveBeenCalledWith(
        {
          expected_epoch: 7,
          expected_version: 21,
          action: "revoke",
          event_id: "event-old",
        },
        expect.stringMatching(/^profile-claim-/),
      ),
    );
    await waitFor(() => expect(apiMock.profileClaims).toHaveBeenCalledTimes(3));
    expect(apiMock.profileClaims).toHaveBeenNthCalledWith(3);
  });

  test("restarts the initial page on an older-page 409 without replaying an action", async () => {
    apiMock.profileClaims
      .mockResolvedValueOnce({
        version: 30,
        claims: [claim({ id: "stale", value: "旧分页依据" })],
        events: [],
        events_has_more: true,
        events_next_cursor: "stale-cursor",
      })
      .mockRejectedValueOnce(new ApiError(409, "stale cursor"))
      .mockResolvedValueOnce({
        version: 31,
        claims: [claim({ id: "fresh", value: "刷新后的依据" })],
        events: [],
        events_has_more: false,
      });
    const { user } = await renderLoaded();

    await user.click(button("加载更多"));

    await screen.findByText("画像已更新，重新加载后再改。");
    await screen.findByText("刷新后的依据");
    expect(pageText()).not.toContain("旧分页依据");
    expect(apiMock.profileClaims).toHaveBeenNthCalledWith(2, "stale-cursor");
    expect(apiMock.profileClaims).toHaveBeenNthCalledWith(3);
    expect(apiMock.applyProfileClaimAction).not.toHaveBeenCalled();
  });

  test("renders only the bounded first page of a 1000-event history", async () => {
    const fullHistoryFixture = Array.from({ length: 1000 }, (_, index) => ({
      id: `event-${1000 - index}`,
      kind: "pin",
      target_claim_id: "claim-1",
      created_at: `2026-07-${String(27 - (index % 20)).padStart(2, "0")}T02:00:00Z`,
      revoked: false,
      revocable: false,
    }));
    const firstPage = fullHistoryFixture.slice(0, 20);
    apiMock.profileClaims.mockResolvedValue({
      version: 40,
      claims: [claim()],
      events: firstPage,
      events_has_more: true,
      events_next_cursor: "event-980",
    });

    await renderLoaded();

    expect(screen.getAllByText("固定记录")).toHaveLength(20);
    expect(
      (screen.getByRole("button", { name: "加载更多" }) as HTMLButtonElement)
        .disabled,
    ).toBe(false);
    expect(apiMock.profileClaims).toHaveBeenCalledTimes(1);
  });

  test("uses narrow-screen-safe wrapping on claim and history rows", async () => {
    apiMock.profileClaims.mockResolvedValue({
      version: 1,
      claims: [claim({ value: "a".repeat(240) })],
      events: [
        {
          id: "event-1",
          kind: "pin",
          target_claim_id: "claim-1",
          created_at: "2026-07-27T02:00:00Z",
          revoked: false,
          revocable: true,
        },
      ],
      events_has_more: true,
      events_next_cursor: "narrow-cursor",
    });
    const { container } = await renderLoaded();
    const rows = [...container.querySelectorAll("[class]")].filter(
      (node) => {
        const className = node.getAttribute("class") ?? "";
        return (
          className.includes("min-w-0") &&
          className.includes("sm:flex-row")
        );
      },
    );
    expect(rows.length).toBeGreaterThanOrEqual(2);
    expect(container.querySelector(".break-words")).not.toBeNull();
    expect(button("加载更多").className).toContain("w-full");
    expect(button("加载更多").className).toContain("sm:w-auto");
  });
});

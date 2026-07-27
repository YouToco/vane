// @vitest-environment jsdom

import React from "react";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

const apiMock = vi.hoisted(() => ({
  profile: vi.fn(),
  profileEdits: vi.fn(),
  updateProfile: vi.fn(),
  undoProfileEdit: vi.fn(),
}));
const i18nState = vi.hoisted(() => ({ desc: "说明" }));

vi.mock("@/api", () => ({
  api: apiMock,
  ApiError: class ApiError extends Error {
    status: number;
    constructor(status: number, message: string) {
      super(message);
      this.status = status;
    }
  },
}));

vi.mock("@/i18n", () => {
  const profile = {
    title: "用户画像",
    desc: "说明",
    reload: "重新加载",
    confirmReload: "确认重新加载",
    confirmUndoDirty: "确认撤销并放弃草稿",
    editTitle: "人工修正",
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
  return {
    useI18n: () => ({
      t: {
        app: {
          profile: { ...profile, desc: i18nState.desc },
          common: { loadFailed: "加载失败" },
        },
      },
    }),
    fmt: (template: string, vars: Record<string, string | number>) =>
      template.replace(/\{(\w+)\}/g, (_: string, key: string) =>
        String(vars[key] ?? ""),
      ),
  };
});

vi.mock("@/components/ui/card", () => ({
  Card: ({ children }: { children: React.ReactNode }) => <section>{children}</section>,
  CardContent: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
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
  AlertDescription: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
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

import Profile from "@/pages/Profile";
import { ApiError } from "@/api";

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

function button(label: string): HTMLButtonElement {
  return screen.getByRole("button", { name: label }) as HTMLButtonElement;
}

async function renderLoaded() {
  const user = userEvent.setup();
  const view = render(<Profile />);
  await waitFor(() => expect(apiMock.profileEdits).toHaveBeenCalledTimes(1));
  return { user, ...view };
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("profile manual editing", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    i18nState.desc = "说明";
    apiMock.profile.mockResolvedValue(baseProfile);
    apiMock.profileEdits.mockResolvedValue({ edits: [] });
    apiMock.updateProfile.mockResolvedValue({
      ...baseProfile,
      industry: "AI 工具",
      updated_at: "2026-07-27T02:00:00Z",
    });
    apiMock.undoProfileEdit.mockResolvedValue(baseProfile);
    vi.spyOn(window, "confirm").mockReturnValue(true);
  });

  test("sends only changed fields with the current revision and an idempotency key", async () => {
    const { user } = await renderLoaded();
    const industry = screen.getByDisplayValue("AI");
    await user.clear(industry);
    await user.type(industry, "AI 工具");
    await user.click(button("保存修改"));

    await waitFor(() =>
      expect(apiMock.updateProfile).toHaveBeenCalledWith(
        {
          expected_updated_at: baseProfile.updated_at,
          industry: "AI 工具",
        },
        expect.stringMatching(/^profile-edit-/),
      ),
    );
    await waitFor(() => expect(apiMock.profile).toHaveBeenCalledTimes(2));
  });

  test("does not canonicalize untouched legacy fields into an unrelated edit", async () => {
    apiMock.profile.mockResolvedValue({
      ...baseProfile,
      industry: " AI ",
      occupation: "开发者",
      tags: [" Agent ", "Agent"],
    });
    const { user } = await renderLoaded();
    const occupation = screen.getByDisplayValue("开发者");
    await user.clear(occupation);
    await user.type(occupation, "创始人");
    await user.click(button("保存修改"));

    await waitFor(() =>
      expect(apiMock.updateProfile).toHaveBeenCalledWith(
        {
          expected_updated_at: baseProfile.updated_at,
          occupation: "创始人",
        },
        expect.stringMatching(/^profile-edit-/),
      ),
    );
  });

  test("treats outer whitespace around a clean scalar as no semantic change", async () => {
    const { user } = await renderLoaded();
    const industry = screen.getByDisplayValue("AI");
    await user.clear(industry);
    await user.type(industry, " AI ");

    expect(button("保存修改").disabled).toBe(true);
    expect(apiMock.updateProfile).not.toHaveBeenCalled();
  });

  test("preserves exact tag spacing and rejects pasted control characters", async () => {
    const { user } = await renderLoaded();
    const tagInput = screen.getByPlaceholderText("输入标签");
    await user.type(tagInput, "machine  learning");
    await user.tab();
    await user.click(button("保存修改"));
    await waitFor(() =>
      expect(apiMock.updateProfile).toHaveBeenCalledWith(
        {
          expected_updated_at: baseProfile.updated_at,
          tags: ["Agent", "machine  learning"],
        },
        expect.stringMatching(/^profile-edit-/),
      ),
    );

    apiMock.updateProfile.mockClear();
    for (const invalid of [
      "bad\tlabel",
      "line\u2028separator",
      "paragraph\u2029separator",
    ]) {
      const nextTagInput = screen.getByPlaceholderText(
        "输入标签",
      ) as HTMLInputElement;
      fireEvent.change(nextTagInput, { target: { value: invalid } });
      expect(pageText()).toContain("标签不能包含控制字符");
      expect(nextTagInput.getAttribute("aria-invalid")).toBe("true");
      expect(button("保存修改").disabled).toBe(true);
    }
    expect(
      screen.getByPlaceholderText("输入标签").getAttribute("aria-describedby"),
    ).toContain("-error");
    expect(apiMock.updateProfile).not.toHaveBeenCalled();
  });

  test("distinguishes edit and undo revisions", async () => {
    apiMock.profileEdits.mockResolvedValue({
      edits: [
        {
          id: "undo-1",
          actor: "self",
          kind: "undo",
          created_at: "2026-07-27T03:00:00Z",
          changes: [],
          undoable: false,
        },
        {
          id: "edit-1",
          actor: "self",
          kind: "edit",
          created_at: "2026-07-27T02:00:00Z",
          changes: [],
          undoable: false,
        },
      ],
    });
    await renderLoaded();
    expect(pageText()).toContain("撤销记录");
    expect(pageText()).toContain("编辑");
  });

  test("keeps the draft on a 409 and never reloads or overwrites automatically", async () => {
    apiMock.updateProfile.mockRejectedValue(new ApiError(409, "conflict"));
    const { user } = await renderLoaded();
    const industry = screen.getByDisplayValue("AI");
    await user.clear(industry);
    await user.type(industry, "机器人");
    await user.click(button("保存修改"));

    await screen.findByText("画像已更新，重新加载后再改。");
    expect(screen.getByDisplayValue("机器人")).toBeDefined();
    expect(apiMock.profile).toHaveBeenCalledTimes(1);
    expect(apiMock.updateProfile).toHaveBeenCalledTimes(1);
  });

  test("reuses the idempotency key when retrying the same intent after a network error", async () => {
    apiMock.updateProfile
      .mockRejectedValueOnce(new ApiError(0, "offline"))
      .mockResolvedValueOnce({ ...baseProfile, occupation: "创始人" });
    const { user } = await renderLoaded();
    const occupation = screen.getByDisplayValue("独立开发者");
    await user.clear(occupation);
    await user.type(occupation, "创始人");
    await user.click(button("保存修改"));
    await waitFor(() => expect(apiMock.updateProfile).toHaveBeenCalledTimes(1));
    const firstKey = apiMock.updateProfile.mock.calls[0]?.[1];
    await user.click(button("保存修改"));

    await waitFor(() => expect(apiMock.updateProfile).toHaveBeenCalledTimes(2));
    expect(apiMock.updateProfile.mock.calls[1]?.[1]).toBe(firstKey);
  });

  test("creates an absent profile with a null compare token", async () => {
    apiMock.profile.mockRejectedValueOnce(new ApiError(404, "missing"));
    const { user } = await renderLoaded();
    expect(pageText()).toContain("画像尚未生成");
    const industry = screen.getByLabelText("行业");
    await user.type(industry, "AI");
    await user.click(button("保存修改"));

    await waitFor(() =>
      expect(apiMock.updateProfile).toHaveBeenCalledWith(
        { expected_updated_at: null, industry: "AI" },
        expect.stringMatching(/^profile-edit-/),
      ),
    );
  });

  test("shows undo only for the server-authorized latest revision", async () => {
    apiMock.profileEdits.mockResolvedValue({
      edits: [
        {
          id: "latest",
          actor: "self",
          kind: "edit",
          created_at: "2026-07-27T02:00:00Z",
          changes: [{ field: "industry", before: "AI", after: "机器人" }],
          undoable: true,
        },
        {
          id: "older",
          actor: "self",
          kind: "edit",
          created_at: "2026-07-27T01:00:00Z",
          changes: [],
          undoable: false,
        },
      ],
    });
    const { user } = await renderLoaded();
    expect(screen.getAllByRole("button", { name: "撤销" })).toHaveLength(1);
    await user.click(button("撤销"));
    await waitFor(() =>
      expect(apiMock.undoProfileEdit).toHaveBeenCalledWith(
        "latest",
        baseProfile.updated_at,
        expect.stringMatching(/^profile-undo-/),
      ),
    );
  });

  test("does not undo or drop a pending tag when dirty-discard confirmation is cancelled", async () => {
    apiMock.profileEdits.mockResolvedValue({
      edits: [
        {
          id: "latest",
          actor: "self",
          kind: "edit",
          created_at: "2026-07-27T02:00:00Z",
          changes: [{ field: "industry", before: null, after: "AI" }],
          undoable: true,
        },
      ],
    });
    const { user } = await renderLoaded();
    const tagInput = screen.getByPlaceholderText("输入标签");
    fireEvent.change(tagInput, { target: { value: "新标签" } });
    vi.mocked(window.confirm).mockReturnValue(false);
    await user.click(button("撤销"));

    expect(window.confirm).toHaveBeenCalledWith("确认撤销并放弃草稿");
    expect(apiMock.undoProfileEdit).not.toHaveBeenCalled();
    expect(screen.getByDisplayValue("新标签")).toBeDefined();
  });

  test("asks before a reload would discard a dirty draft", async () => {
    const { user } = await renderLoaded();
    const occupation = screen.getByDisplayValue("独立开发者");
    await user.clear(occupation);
    await user.type(occupation, "创始人");
    vi.mocked(window.confirm).mockReturnValue(false);
    await user.click(button("重新加载"));

    expect(window.confirm).toHaveBeenCalledWith("确认重新加载");
    expect(apiMock.profile).toHaveBeenCalledTimes(1);
  });

  test("keeps a dirty draft when the locale rerenders the page", async () => {
    const { user, rerender } = await renderLoaded();
    const occupation = screen.getByDisplayValue("独立开发者");
    await user.clear(occupation);
    await user.type(occupation, "创始人");
    i18nState.desc = "Localized description";
    rerender(<Profile />);

    await screen.findByText("Localized description");
    expect(screen.getByDisplayValue("创始人")).toBeDefined();
    expect(apiMock.profile).toHaveBeenCalledTimes(1);
  });
});

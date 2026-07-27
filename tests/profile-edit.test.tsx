import React from "react";
import {
  act,
  create,
  type ReactTestRenderer,
} from "react-test-renderer";
import { beforeEach, describe, expect, test, vi } from "vitest";

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

function textOf(renderer: ReactTestRenderer): string {
  return JSON.stringify(renderer.toJSON());
}

async function renderLoaded(): Promise<ReactTestRenderer> {
  let renderer: ReactTestRenderer;
  await act(async () => {
    renderer = create(<Profile />);
    await Promise.resolve();
    await Promise.resolve();
  });
  return renderer!;
}

function button(renderer: ReactTestRenderer, label: string) {
  return renderer.root
    .findAllByType("button")
    .find((node) => node.children.includes(label));
}

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
    Object.defineProperty(globalThis, "window", {
      configurable: true,
      value: { confirm: vi.fn(() => true) },
    });
  });

  test("sends only changed fields with the current revision and an idempotency key", async () => {
    const renderer = await renderLoaded();
    const industry = renderer.root
      .findAllByType("input")
      .find((node) => node.props.value === "AI");
    await act(async () => {
      industry?.props.onChange({ target: { value: "AI 工具" } });
    });
    await act(async () => {
      button(renderer, "保存修改")?.props.onClick();
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(apiMock.updateProfile).toHaveBeenCalledWith(
      {
        expected_updated_at: baseProfile.updated_at,
        industry: "AI 工具",
      },
      expect.stringMatching(/^profile-edit-/),
    );
    expect(apiMock.profile).toHaveBeenCalledTimes(2);
  });

  test("does not canonicalize untouched legacy fields into an unrelated edit", async () => {
    apiMock.profile.mockResolvedValue({
      ...baseProfile,
      industry: " AI ",
      occupation: "开发者",
      tags: [" Agent ", "Agent"],
    });
    const renderer = await renderLoaded();
    const occupation = renderer.root
      .findAllByType("input")
      .find((node) => node.props.value === "开发者");
    await act(async () => {
      occupation?.props.onChange({ target: { value: "创始人" } });
    });
    await act(async () => {
      button(renderer, "保存修改")?.props.onClick();
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(apiMock.updateProfile).toHaveBeenCalledWith(
      {
        expected_updated_at: baseProfile.updated_at,
        occupation: "创始人",
      },
      expect.stringMatching(/^profile-edit-/),
    );
  });

  test("treats outer whitespace around a clean scalar as no semantic change", async () => {
    const renderer = await renderLoaded();
    const industry = renderer.root
      .findAllByType("input")
      .find((node) => node.props.value === "AI");
    await act(async () => {
      industry?.props.onChange({ target: { value: " AI " } });
    });

    expect(button(renderer, "保存修改")?.props.disabled).toBe(true);
    expect(apiMock.updateProfile).not.toHaveBeenCalled();
  });

  test("preserves exact tag spacing and rejects pasted control characters", async () => {
    const renderer = await renderLoaded();
    const tagInput = renderer.root
      .findAllByType("input")
      .find((node) => node.props.placeholder === "输入标签");
    await act(async () => {
      tagInput?.props.onChange({ target: { value: "machine  learning" } });
      tagInput?.props.onBlur();
    });
    await act(async () => {
      button(renderer, "保存修改")?.props.onClick();
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(apiMock.updateProfile).toHaveBeenCalledWith(
      {
        expected_updated_at: baseProfile.updated_at,
        tags: ["Agent", "machine  learning"],
      },
      expect.stringMatching(/^profile-edit-/),
    );

    apiMock.updateProfile.mockClear();
    const nextTagInput = renderer.root
      .findAllByType("input")
      .find((node) => node.props.placeholder === "输入标签");
    for (const invalid of ["bad\tlabel", "line\u2028separator", "paragraph\u2029separator"]) {
      await act(async () => {
        nextTagInput?.props.onChange({ target: { value: invalid } });
      });
      expect(textOf(renderer)).toContain("标签不能包含控制字符");
      expect(
        renderer.root
          .findAllByType("input")
          .find((node) => node.props.placeholder === "输入标签")
          ?.props["aria-invalid"],
      ).toBe(true);
      expect(button(renderer, "保存修改")?.props.disabled).toBe(true);
    }
    expect(
      renderer.root
        .findAllByType("input")
        .find((node) => node.props.placeholder === "输入标签")
        ?.props["aria-describedby"],
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
    const renderer = await renderLoaded();
    expect(textOf(renderer)).toContain("撤销记录");
    expect(textOf(renderer)).toContain("编辑");
  });

  test("keeps the draft on a 409 and never reloads or overwrites automatically", async () => {
    apiMock.updateProfile.mockRejectedValue(new ApiError(409, "conflict"));
    const renderer = await renderLoaded();
    const industry = renderer.root
      .findAllByType("input")
      .find((node) => node.props.value === "AI");
    await act(async () => {
      industry?.props.onChange({ target: { value: "机器人" } });
    });
    await act(async () => {
      button(renderer, "保存修改")?.props.onClick();
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(textOf(renderer)).toContain("画像已更新，重新加载后再改。");
    expect(
      renderer.root.findAllByType("input").some((node) => node.props.value === "机器人"),
    ).toBe(true);
    expect(apiMock.profile).toHaveBeenCalledTimes(1);
    expect(apiMock.updateProfile).toHaveBeenCalledTimes(1);
  });

  test("reuses the idempotency key when retrying the same intent after a network error", async () => {
    apiMock.updateProfile
      .mockRejectedValueOnce(new ApiError(0, "offline"))
      .mockResolvedValueOnce({ ...baseProfile, occupation: "创始人" });
    const renderer = await renderLoaded();
    const occupation = renderer.root
      .findAllByType("input")
      .find((node) => node.props.value === "独立开发者");
    await act(async () => {
      occupation?.props.onChange({ target: { value: "创始人" } });
    });
    await act(async () => {
      button(renderer, "保存修改")?.props.onClick();
      await Promise.resolve();
      await Promise.resolve();
    });
    const firstKey = apiMock.updateProfile.mock.calls[0]?.[1];
    await act(async () => {
      button(renderer, "保存修改")?.props.onClick();
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(apiMock.updateProfile).toHaveBeenCalledTimes(2);
    expect(apiMock.updateProfile.mock.calls[1]?.[1]).toBe(firstKey);
  });

  test("creates an absent profile with a null compare token", async () => {
    apiMock.profile.mockRejectedValueOnce(new ApiError(404, "missing"));
    const renderer = await renderLoaded();
    expect(textOf(renderer)).toContain("画像尚未生成");
    const industry = renderer.root.findAllByType("input")[0];
    await act(async () => {
      industry.props.onChange({ target: { value: "AI" } });
    });
    await act(async () => {
      button(renderer, "保存修改")?.props.onClick();
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(apiMock.updateProfile).toHaveBeenCalledWith(
      { expected_updated_at: null, industry: "AI" },
      expect.stringMatching(/^profile-edit-/),
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
    const renderer = await renderLoaded();
    expect(
      renderer.root
        .findAllByType("button")
        .filter((node) => node.children.includes("撤销")),
    ).toHaveLength(1);
    await act(async () => {
      button(renderer, "撤销")?.props.onClick();
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(apiMock.undoProfileEdit).toHaveBeenCalledWith(
      "latest",
      baseProfile.updated_at,
      expect.stringMatching(/^profile-undo-/),
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
    const renderer = await renderLoaded();
    const tagInput = renderer.root
      .findAllByType("input")
      .find((node) => node.props.placeholder === "输入标签");
    await act(async () => {
      tagInput?.props.onChange({ target: { value: "新标签" } });
    });
    vi.mocked(window.confirm).mockReturnValue(false);
    await act(async () => {
      button(renderer, "撤销")?.props.onClick();
    });

    expect(window.confirm).toHaveBeenCalledWith("确认撤销并放弃草稿");
    expect(apiMock.undoProfileEdit).not.toHaveBeenCalled();
    expect(
      renderer.root.findAllByType("input").some((node) => node.props.value === "新标签"),
    ).toBe(true);
  });

  test("asks before a reload would discard a dirty draft", async () => {
    const renderer = await renderLoaded();
    const occupation = renderer.root
      .findAllByType("input")
      .find((node) => node.props.value === "独立开发者");
    await act(async () => {
      occupation?.props.onChange({ target: { value: "创始人" } });
    });
    vi.mocked(window.confirm).mockReturnValue(false);
    await act(async () => {
      button(renderer, "重新加载")?.props.onClick();
    });
    expect(window.confirm).toHaveBeenCalledWith("确认重新加载");
    expect(apiMock.profile).toHaveBeenCalledTimes(1);
  });

  test("keeps a dirty draft when the locale rerenders the page", async () => {
    const renderer = await renderLoaded();
    const occupation = renderer.root
      .findAllByType("input")
      .find((node) => node.props.value === "独立开发者");
    await act(async () => {
      occupation?.props.onChange({ target: { value: "创始人" } });
    });
    i18nState.desc = "Localized description";
    await act(async () => {
      renderer.update(<Profile />);
      await Promise.resolve();
    });

    expect(textOf(renderer)).toContain("Localized description");
    expect(
      renderer.root.findAllByType("input").some((node) => node.props.value === "创始人"),
    ).toBe(true);
    expect(apiMock.profile).toHaveBeenCalledTimes(1);
  });
});

// @vitest-environment jsdom

import React from "react";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, test } from "vitest";
import PrototypeTaskDetail from "../prototypes/p0a-task-brief/PrototypeTaskDetail";

afterEach(cleanup);

function pageText(): string {
  return document.body.textContent ?? "";
}

describe("P0-A task brief comprehension prototype", () => {
  test("opens on a user-facing briefing rather than operations data", () => {
    const { container } = render(<PrototypeTaskDetail />);

    const text = pageText();
    expect(text).toContain("示例简报");
    expect(text).toContain("理解测试样本 · 官方事实已核验");
    expect(text).toContain("为什么与你相关");
    expect(text).toContain("对市场与销售");
    expect(text).toContain("建议下一步");
    expect(text).toContain("OpenAI 官方");
    expect(text).toContain("示例工作区");
    expect(text).not.toContain("allan_guodpl");
    expect(text).not.toContain("AI 市场与产品情报群");
    expect(
      container.querySelectorAll(".p0a-brand-mark, .p0a-mobile-logo"),
    ).toHaveLength(2);
    expect(text).not.toContain("LLM 成本");
    expect(text).not.toContain("sent");
  });

  test("distinguishes quiet, partial, and failed checks in user language", async () => {
    const user = userEvent.setup();
    render(<PrototypeTaskDetail />);

    await user.click(screen.getByRole("button", { name: "无重要变化" }));
    expect(pageText()).toContain("本次检查没有发现值得打扰你的变化");
    expect(pageText()).toContain("已查完但无需行动");

    await user.click(screen.getByRole("button", { name: "部分完成" }));
    expect(pageText()).toContain("Anthropic 官方来源暂未检查完");
    expect(pageText()).toContain("GPT-5.6 推出");
    expect(pageText()).toContain("Gemini 新增项目月度支出上限");
    expect(pageText()).toContain("补查时间：今天 16:00");

    await user.click(screen.getByRole("button", { name: "检查失败" }));
    expect(pageText()).toContain("本次不会被记成");
    expect(pageText()).toContain("自动重试：今天 16:00");
  });

  test("keeps diagnostics behind settings and changes controls locally", async () => {
    const user = userEvent.setup();
    render(<PrototypeTaskDetail />);

    await user.click(screen.getByRole("tab", { name: "任务设置" }));
    expect(pageText()).toContain("任务正在持续关注");
    expect(pageText()).not.toContain("LLM 成本");

    await user.click(screen.getByRole("button", { name: /运行与诊断/ }));
    expect(pageText()).toContain("LLM 成本");
    expect(pageText()).toContain("近 7 天运行");

    await user.click(screen.getByRole("button", { name: "暂停任务" }));
    expect(pageText()).toContain("任务已暂停");
    expect(pageText()).toContain("恢复任务");
  });

  test("records feedback without leaving the prototype", async () => {
    const user = userEvent.setup();
    render(<PrototypeTaskDetail />);

    await user.click(screen.getAllByRole("button", { name: "没帮助" })[0]);
    expect(pageText()).toContain("哪里需要改进");
    expect(pageText()).toContain("缺少证据");
    await user.click(screen.getByRole("button", { name: "缺少证据" }));
    await user.click(screen.getByRole("tab", { name: "历史简报" }));
    await user.click(screen.getByRole("tab", { name: "最新简报" }));
    expect(pageText()).toContain("已记录 · 没帮助 · 缺少证据");
  });

  test("makes prototype scope and feedback cancellation explicit", async () => {
    const user = userEvent.setup();
    render(<PrototypeTaskDetail />);

    expect(
      (screen.getByRole("button", {
        name: /返回任务列表/,
      }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(
      (screen.getByRole("button", { name: "首页" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);

    const like = screen.getAllByRole("button", { name: "有帮助" })[0];
    await user.click(like);
    expect(pageText()).toContain("有帮助 · 再次点击可撤销");
    await user.click(like);
    expect(pageText()).not.toContain("有帮助 · 再次点击可撤销");

    const dislike = screen.getAllByRole("button", { name: "没帮助" })[0];
    await user.click(dislike);
    expect(pageText()).toContain("没帮助 · 再次点击可撤销");
    await user.click(dislike);
    expect(pageText()).not.toContain("没帮助 · 再次点击可撤销");
  });
});

import React from "react";
import {
  act,
  create,
  type ReactTestInstance,
  type ReactTestRenderer,
} from "react-test-renderer";
import { beforeEach, describe, expect, test } from "vitest";
import PrototypeTaskDetail from "@/prototypes/p0a-task-brief/PrototypeTaskDetail";

function textOf(value: ReactTestRenderer | ReactTestInstance): string {
  if ("toJSON" in value) return JSON.stringify(value.toJSON());
  return value.children
    .map((child) =>
      typeof child === "string" || typeof child === "number"
        ? String(child)
        : textOf(child),
    )
    .join("");
}

function buttonNamed(
  renderer: ReactTestRenderer,
  name: string,
): ReactTestInstance {
  const button = renderer.root
    .findAllByType("button")
    .find((candidate) => textOf(candidate).includes(name));
  if (!button) throw new Error(`button not found: ${name}`);
  return button;
}

describe("P0-A task brief comprehension prototype", () => {
  beforeEach(() => {
    Object.defineProperty(globalThis, "IS_REACT_ACT_ENVIRONMENT", {
      configurable: true,
      value: true,
    });
  });

  test("opens on a user-facing briefing rather than operations data", async () => {
    let renderer: ReactTestRenderer;
    await act(async () => {
      renderer = create(<PrototypeTaskDetail />);
    });

    const text = textOf(renderer!);
    expect(text).toContain("示例简报");
    expect(text).toContain("理解测试样本 · 官方事实已核验");
    expect(text).toContain("为什么与你相关");
    expect(text).toContain("对市场与销售");
    expect(text).toContain("建议下一步");
    expect(text).toContain("OpenAI 官方");
    expect(text).toContain("示例工作区");
    expect(text).not.toContain("allan_guodpl");
    expect(text).not.toContain("AI 市场与产品情报群");
    const brandMarks = renderer!.root
      .findAllByType("svg")
      .filter((node) =>
        String(node.props.className).includes("p0a-brand-mark") ||
        String(node.props.className).includes("p0a-mobile-logo"),
      );
    expect(brandMarks).toHaveLength(2);
    expect(text).not.toContain("LLM 成本");
    expect(text).not.toContain("sent");
  });

  test("distinguishes quiet, partial, and failed checks in user language", async () => {
    let renderer: ReactTestRenderer;
    await act(async () => {
      renderer = create(<PrototypeTaskDetail />);
    });
    await act(async () => buttonNamed(renderer!, "无重要变化").props.onClick());
    expect(textOf(renderer!)).toContain("本次检查没有发现值得打扰你的变化");
    expect(textOf(renderer!)).toContain("已查完但无需行动");
    await act(async () => buttonNamed(renderer!, "部分完成").props.onClick());
    expect(textOf(renderer!)).toContain("Anthropic 官方来源暂未检查完");
    expect(textOf(renderer!)).toContain("GPT-5.6 推出");
    expect(textOf(renderer!)).toContain("Gemini 新增项目月度支出上限");
    expect(textOf(renderer!)).toContain("补查时间：今天 16:00");
    await act(async () => buttonNamed(renderer!, "检查失败").props.onClick());
    expect(textOf(renderer!)).toContain("本次不会被记成");
    expect(textOf(renderer!)).toContain("自动重试：今天 16:00");
  });

  test("keeps diagnostics behind settings and changes controls locally", async () => {
    let renderer: ReactTestRenderer;
    await act(async () => {
      renderer = create(<PrototypeTaskDetail />);
    });

    await act(async () => {
      buttonNamed(renderer!, "任务设置").props.onClick();
    });
    expect(textOf(renderer!)).toContain("任务正在持续关注");
    expect(textOf(renderer!)).not.toContain("LLM 成本");

    await act(async () => {
      buttonNamed(renderer!, "运行与诊断").props.onClick();
    });
    expect(textOf(renderer!)).toContain("LLM 成本");
    expect(textOf(renderer!)).toContain("近 7 天运行");

    await act(async () => {
      buttonNamed(renderer!, "暂停任务").props.onClick();
    });
    expect(textOf(renderer!)).toContain("任务已暂停");
    expect(textOf(renderer!)).toContain("恢复任务");
  });

  test("records feedback without leaving the prototype", async () => {
    let renderer: ReactTestRenderer;
    await act(async () => {
      renderer = create(<PrototypeTaskDetail />);
    });
    const dislike = renderer!.root.findAllByProps({ "aria-label": "没帮助" })[0];
    await act(async () => {
      dislike.props.onClick();
    });
    expect(textOf(renderer!)).toContain("哪里需要改进");
    expect(textOf(renderer!)).toContain("缺少证据");
    await act(async () => {
      buttonNamed(renderer!, "缺少证据").props.onClick();
      buttonNamed(renderer!, "历史简报").props.onClick();
      buttonNamed(renderer!, "最新简报").props.onClick();
    });
    expect(textOf(renderer!)).toContain("已记录 · 没帮助 · 缺少证据");
  });

  test("makes prototype scope and feedback cancellation explicit", async () => {
    let renderer: ReactTestRenderer;
    await act(async () => {
      renderer = create(<PrototypeTaskDetail />);
    });

    const back = buttonNamed(renderer!, "返回任务列表");
    expect(back.props.disabled).toBe(true);
    const home = buttonNamed(renderer!, "首页");
    expect(home.props.disabled).toBe(true);

    const like = renderer!.root.findAllByProps({ "aria-label": "有帮助" })[0];
    await act(async () => like.props.onClick());
    expect(textOf(renderer!)).toContain("有帮助 · 再次点击可撤销");
    await act(async () => like.props.onClick());
    expect(textOf(renderer!)).not.toContain("有帮助 · 再次点击可撤销");

    const dislike = renderer!.root.findAllByProps({ "aria-label": "没帮助" })[0];
    await act(async () => dislike.props.onClick());
    expect(textOf(renderer!)).toContain("没帮助 · 再次点击可撤销");
    await act(async () => dislike.props.onClick());
    expect(textOf(renderer!)).not.toContain("没帮助 · 再次点击可撤销");
  });
});

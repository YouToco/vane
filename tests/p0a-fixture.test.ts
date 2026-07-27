import { describe, expect, test } from "vitest";
import {
  ownerPreviewFixture,
  prototypePresentation,
} from "@/prototypes/p0a-task-brief/fixture";

describe("P0-A owner preview fixture", () => {
  test("uses explicit synthetic operational values", () => {
    expect(ownerPreviewFixture.taskTitle).toBe(
      "每周一上午 9:00 推送 AI 官方重大更新",
    );
    expect(ownerPreviewFixture.rawStats).toEqual({
      deliveries7d: 11,
      runs7d: 5,
      emptyRuns7d: 1,
      llmCostUSD: 0.0012,
    });
    expect(prototypePresentation.task.channel).toBe("飞书 · 示例情报群");
    expect(prototypePresentation.prototypePresentationCopy).toBe(true);
  });

  test("the presentation hierarchy is issue → insight → evidence", () => {
    expect(prototypePresentation.issue.headline).toBeTruthy();
    for (const insight of prototypePresentation.insights) {
      expect(insight.title).toBeTruthy();
      expect(insight.whyRelevant).toBeTruthy();
      expect(insight.goToMarketImpact).toBeTruthy();
      expect(insight.nextAction).toBeTruthy();
      expect(insight.source).toBeTruthy();
      expect(insight.evidenceTitle).toBeTruthy();
      expect(insight.evidenceUrl).toMatch(/^https:\/\//);
    }
  });
});

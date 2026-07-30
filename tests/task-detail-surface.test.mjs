import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { test } from "node:test";

const root = resolve(import.meta.dirname, "..");

test("task pages keep operational internals off the reader-facing surface", async () => {
  const [dashboard, detail, briefs, zh] = await Promise.all([
    readFile(resolve(root, "src/pages/TaskDashboard.tsx"), "utf8"),
    readFile(resolve(root, "src/pages/TaskDetail.tsx"), "utf8"),
    readFile(resolve(root, "src/components/TaskBriefFeed.tsx"), "utf8"),
    readFile(resolve(root, "src/i18n/zh.ts"), "utf8"),
  ]);

  assert.doesNotMatch(dashboard, /batches_7d|last_exit_gate/);
  assert.doesNotMatch(dashboard, /batchOutcomeLabel|batchOutcomeVariant/);
  assert.match(dashboard, /summary\.sent_pushes_7d/);

  assert.doesNotMatch(detail, /summary\.batches_7d/);
  assert.doesNotMatch(detail, /summary\.empty_batches_7d/);
  assert.doesNotMatch(detail, /detail\.cost|llm_cost_usd/);
  assert.doesNotMatch(detail, /b\.sent|b\.stage_counts|funnelText/);
  assert.doesNotMatch(detail, /batchOutcomeLabel|batchOutcomeVariant/);
  assert.match(detail, /TaskBriefFeed/);
  assert.match(detail, /TaskHealthPanel/);
  assert.match(detail, /health\?\.permissions/);
  assert.match(detail, /latestCheck\?\.finalized_at/);
  assert.match(detail, /scheduleIDRef\.current !== requestedScheduleID/);
  assert.match(detail, /window\.confirm\(D\.deleteConfirm\)/);
  assert.match(
    detail,
    /finally\s*\{\s*if \(commandRef\.current === "delete"\)/,
  );
  assert.match(zh, /这是 Agent 执行任务的完整依据/);
  assert.doesNotMatch(detail, /ExplorationPanel|value="explore"|探索视野/);

  assert.match(briefs, /ReactMarkdown/);
  assert.match(briefs, /skipHtml/);
  assert.match(briefs, /allowedElements/);
  assert.match(briefs, /BRIEF_MARKDOWN_ELEMENTS/);
  assert.match(briefs, /safeBriefMarkdownURL/);
  assert.match(briefs, /brief\.insights\.map/);
});

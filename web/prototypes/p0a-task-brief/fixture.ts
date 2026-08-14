export type Insight = {
  id: string;
  eyebrow: string;
  title: string;
  summary: string;
  whyRelevant: string;
  goToMarketImpact: string;
  decision: "现在行动" | "本周评估" | "持续观察";
  nextAction: string;
  source: string;
  publishedAt: string;
  evidenceTitle: string;
  evidenceUrl: string;
  signal: "major" | "watch";
};

// Synthetic values for the public owner-acceptance page. Do not copy account
// identifiers, private channel names, timestamps, or metrics from production.
export const ownerPreviewFixture = {
  taskTitle: "每周一上午 9:00 推送 AI 官方重大更新",
  rawStats: {
    deliveries7d: 11,
    runs7d: 5,
    emptyRuns7d: 1,
    llmCostUSD: 0.0012,
  },
} as const;

// OWNER ACCEPTANCE COPY: the page is not connected to production body_md,
// while each sample fact below is grounded in its linked official page.
export const prototypePresentation = {
  prototypePresentationCopy: true,
  task: {
    summary: "替你持续追踪三家 AI 厂商的官方更新，只在变化值得关注时解释影响。",
    scope: "OpenAI、Anthropic、Google 的官方模型与 API 更新",
    criteria: "影响产品能力、成本或路线选择的重大变化",
    cadence: "每天检查，每周一 09:00 汇总推送",
    channel: "飞书 · 示例情报群",
  },
  issue: {
    label: "示例简报",
    timeRange: "示例周期 · 7 月 20—26 日",
    headline: "三家厂商都在重画性能、成本与可控性的边界",
    overview:
      "这不是新闻列表：下面 3 项分别对应模型选型、报价与预算控制，并给出建议动作和官方原文。",
    topSignal: "优先看：OpenAI 推出 GPT-5.6 三档模型，Terra 标准价为每百万输入/输出 token $2.5/$15。",
    generatedAt: "示例时间 · 7 月 26 日 15:07",
    coverage: "已核验 3/3 个官方原文",
  },
  insights: [
    {
      id: "openai-models",
      eyebrow: "模型与价格 · OpenAI",
      title: "GPT-5.6 推出 Sol、Terra、Luna 三档；Terra 定价 $2.5/$15",
      summary:
        "7 月 9 日起三档模型全面开放。每百万输入/输出文本单位（token）标准价分别为 Sol $5/$30、Terra $2.5/$15、Luna $1/$6；缓存写入按 1.25 倍输入价计费，读取仍享 90% 折扣。",
      whyRelevant:
        "产品与研发：现有 GPT-5.5 功能场景有明确的降本候选，但缓存写入规则也改变了高复用场景的测算方式。",
      goToMarketImpact:
        "三档命名让“旗舰、均衡、经济型”的方案包装更清楚，报价单不应再只写一个 OpenAI 模型。",
      decision: "现在行动",
      nextAction: "产品：复算调用量最大的 3 个功能场景；市场/销售：更新三档报价单与客户说明。",
      source: "OpenAI 官方",
      publishedAt: "7 月 9 日",
      evidenceTitle: "GPT-5.6: Frontier intelligence that scales with your ambition",
      evidenceUrl: "https://openai.com/index/gpt-5-6/",
      signal: "major",
    },
    {
      id: "anthropic-models",
      eyebrow: "可用性与价格 · Anthropic",
      title: "Fable 5 已开放；Mythos 5 仅限 Glasswing 等受限群体；均为 $10/$50",
      summary:
        "Fable 5 已带安全保护正式开放，Mythos 5 仍是受限访问；两者每百万输入/输出 token 为 $10/$50，不到 Mythos Preview 价格的一半。",
      whyRelevant:
        "产品与研发：可把 Fable 纳入高难任务评测，但不能把 Mythos 当成所有账户都可用的默认依赖。",
      goToMarketImpact:
        "竞品正在用“更长自主工作 + 明显降价”重写旗舰叙事；比较材料需同时注明访问限制。",
      decision: "本周评估",
      nextAction: "产品：评测 Fable 并准备备用模型；市场/销售：更新竞品比较并注明访问限制。",
      source: "Anthropic 官方",
      publishedAt: "6 月 9 日",
      evidenceTitle: "Claude Fable 5 and Claude Mythos 5",
      evidenceUrl: "https://www.anthropic.com/news/claude-fable-5-mythos-5",
      signal: "major",
    },
    {
      id: "gemini-control",
      eyebrow: "预算控制 · Google",
      title: "Gemini 新增项目月度支出上限、用量层级与成本仪表盘",
      summary:
        "项目可设置月度支出上限，并新增费率、成本和用量仪表盘；但停用约有 10 分钟延迟，超出部分仍由用户承担。",
      whyRelevant:
        "产品与研发：预算护栏更容易落到单项目，但不能把支出上限当成实时强制停用。",
      goToMarketImpact:
        "可缓解客户对 Gemini 预算失控的异议，但合同和销售话术必须保留延迟与超额责任说明。",
      decision: "本周评估",
      nextAction: "产品：验证上限与停用延迟；市场/销售：更新预算异议话术和合同说明。",
      source: "Google 官方",
      publishedAt: "3 月 16 日",
      evidenceTitle: "More transparency and control over Gemini API costs",
      evidenceUrl: "https://blog.google/innovation-and-ai/technology/developers-tools/more-control-over-gemini-api-costs/",
      signal: "watch",
    },
  ] satisfies Insight[],
  history: [
    {
      date: "7 月 19 日",
      title: "一周内没有需要你立即行动的重大更新",
      summary: "检查了 3 个官方信源；2 条一般变化已归档，没有打扰团队。",
    },
    {
      date: "7 月 12 日",
      title: "两项模型上下文能力更新值得产品团队评估",
      summary: "已整理能力变化、适用场景和原始证据。",
    },
  ],
  outcomes: {
    quiet: {
      label: "无重要变化",
      title: "本次检查没有发现值得打扰你的变化",
      detail: "3 个官方来源均已完成检查；这代表“已查完但无需行动”，不是失败。",
      coverage: "检查窗口：7 月 20 日—7 月 26 日",
      lastSuccess: "最近有效简报：7 月 19 日",
      nextStep: "下次例行检查：明天 09:00",
    },
    partial: {
      label: "部分完成",
      title: "OpenAI 与 Google 已完成，Anthropic 官方来源暂未检查完",
      detail: "下方 2 条结论可用；缺失部分不作推断，补查成功后会更新本期状态。",
      coverage: "已完成：OpenAI、Google · 缺失：Anthropic",
      lastSuccess: "上次完整检查：7 月 19 日 09:04",
      nextStep: "补查时间：今天 16:00",
    },
    failed: {
      label: "检查未完成",
      title: "本次暂时无法判断是否有新变化",
      detail: "3 个官方来源都未形成可核验结论；本次不会被记成“没有变化”。",
      coverage: "失败范围：本期全部 3 个官方来源",
      lastSuccess: "最近有效简报：7 月 19 日",
      nextStep: "自动重试：今天 16:00",
    },
  },
} as const;

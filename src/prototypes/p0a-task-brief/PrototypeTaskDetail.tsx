import {
  useEffect,
  useRef,
  useState,
  type KeyboardEvent as ReactKeyboardEvent,
} from "react";
import {
  Activity,
  ArrowLeft,
  Bell,
  BookOpen,
  CalendarClock,
  Check,
  ChevronDown,
  CirclePause,
  ExternalLink,
  FileClock,
  Home,
  ListChecks,
  Menu,
  MessageCircle,
  Pencil,
  Radio,
  Settings2,
  ShieldCheck,
  Sparkles,
  ThumbsDown,
  ThumbsUp,
  X,
} from "lucide-react";
import { LogoMark } from "@/components/brand/Logo";
import {
  ownerPreviewFixture,
  prototypePresentation,
  type Insight,
} from "./fixture";

type Tab = "latest" | "history" | "settings";
type Vote = "up" | "down" | null;
type Outcome = "content" | "quiet" | "partial" | "failed";
type Feedback = { vote: Vote; reason?: string };

const navItems = [
  { icon: Home, label: "首页" },
  { icon: ListChecks, label: "任务", active: true },
  { icon: Bell, label: "推送记录" },
  { icon: Radio, label: "数据来源" },
];

const taskTabs = [
  ["latest", "最新简报", Sparkles],
  ["history", "历史简报", FileClock],
  ["settings", "任务设置", Settings2],
] as const;

function Sidebar({ open, onClose }: { open: boolean; onClose: () => void }) {
  const closeRef = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    if (!open) return;
    closeRef.current?.focus();
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [open, onClose]);
  return (
    <>
      {open && <button className="p0a-scrim" aria-label="关闭导航" onClick={onClose} />}
      <aside id="p0a-sidebar" className={`p0a-sidebar ${open ? "is-open" : ""}`}>
        <div className="p0a-brand">
          <LogoMark className="p0a-brand-mark" />
          <span><strong>见微 Vane</strong><small>AI 情报系统</small></span>
          <button ref={closeRef} className="p0a-sidebar-close" onClick={onClose} aria-label="关闭导航"><X /></button>
        </div>
        <p className="p0a-nav-label">我的情报</p>
        <nav>
          {navItems.map(({ icon: Icon, label, active }) => (
            <button
              key={label}
              className={active ? "active" : ""}
              disabled
              title={active ? "当前页面" : "本轮原型不包含此页面"}
            >
              <Icon /> <span>{label}</span>
            </button>
          ))}
        </nav>
        <p className="p0a-nav-label">账号</p>
        <nav>
          <button disabled title="本轮原型不包含此页面"><MessageCircle /> <span>推送通道</span></button>
          <button disabled title="本轮原型不包含此页面"><Settings2 /> <span>偏好设置</span></button>
        </nav>
        <div className="p0a-user">
          <span className="p0a-avatar">V</span>
          <span><strong>示例工作区</strong><small>Owner 验收</small></span>
        </div>
      </aside>
    </>
  );
}

function InsightCard({
  insight,
  feedback,
  onFeedback,
}: {
  insight: Insight;
  feedback: Feedback;
  onFeedback: (feedback: Feedback) => void;
}) {
  const [reasonOpen, setReasonOpen] = useState(false);
  return (
    <article className="p0a-insight">
      <div className="p0a-insight-rail" data-signal={insight.signal} />
      <div className="p0a-insight-body">
        <div className="p0a-insight-topline">
          <span>{insight.eyebrow}</span>
          <span>{insight.publishedAt}</span>
        </div>
        <h3>{insight.title}</h3>
        <p className="p0a-summary">{insight.summary}</p>
        <div className="p0a-relevance">
          <Sparkles />
          <div>
            <p><strong>为什么与你相关</strong>{insight.whyRelevant}</p>
            <p><strong>对市场与销售</strong>{insight.goToMarketImpact}</p>
          </div>
        </div>
        <div className="p0a-decision">
          <span>{insight.decision}</span>
          <p><strong>建议下一步</strong>{insight.nextAction}</p>
        </div>
        <div className="p0a-insight-footer">
          <div className="p0a-evidence">
            <span>{insight.source} · {insight.evidenceTitle}</span>
            <a href={insight.evidenceUrl} target="_blank" rel="noreferrer">
              核验官方原文<ExternalLink />
            </a>
          </div>
          <div className="p0a-vote" aria-label="这条情报有帮助吗">
            <span>{feedback.vote === "up" ? "已记录 · 有帮助 · 再次点击可撤销" : feedback.vote === "down" ? `已记录 · 没帮助${feedback.reason ? ` · ${feedback.reason}` : ""} · 再次点击可撤销` : "有帮助吗？"}</span>
            <button
              className={feedback.vote === "up" ? "selected" : ""}
              aria-label="有帮助"
              aria-pressed={feedback.vote === "up"}
              onClick={() => { onFeedback({ vote: feedback.vote === "up" ? null : "up" }); setReasonOpen(false); }}
            ><ThumbsUp /></button>
            <button
              className={feedback.vote === "down" ? "selected" : ""}
              aria-label="没帮助"
              aria-pressed={feedback.vote === "down"}
              onClick={() => {
                const removing = feedback.vote === "down";
                onFeedback({ vote: removing ? null : "down" });
                setReasonOpen(!removing);
              }}
            ><ThumbsDown /></button>
          </div>
        </div>
        {reasonOpen && (
          <div className="p0a-reasons">
            <span>哪里需要改进？</span>
            {["不相关", "已经知道", "信息过时", "缺少证据"].map((reason) => (
              <button
                key={reason}
                onClick={() => { onFeedback({ vote: "down", reason }); setReasonOpen(false); }}
              >{reason}</button>
            ))}
          </div>
        )}
      </div>
    </article>
  );
}

function LatestBrief({
  feedback,
  onFeedback,
}: {
  feedback: Record<string, Feedback>;
  onFeedback: (id: string, feedback: Feedback) => void;
}) {
  const [outcome, setOutcome] = useState<Outcome>("content");
  const issue = prototypePresentation.issue;
  const outcomeCopy = outcome === "content" ? null : prototypePresentation.outcomes[outcome];
  return (
    <>
      <div className="p0a-scenario" aria-label="原型结果状态">
        <span>测试主持人工具 · 切换结果样本</span>
        {([
          ["content", "有更新"],
          ["quiet", "无重要变化"],
          ["partial", "部分完成"],
          ["failed", "检查失败"],
        ] as const).map(([value, label]) => (
          <button
            key={value}
            aria-pressed={outcome === value}
            className={outcome === value ? "active" : ""}
            onClick={() => setOutcome(value)}
          >{label}</button>
        ))}
      </div>
      {outcomeCopy ? (
        <>
        <section
          id="p0a-panel-latest"
          role="tabpanel"
          aria-labelledby="p0a-tab-latest"
          className={`p0a-panel p0a-outcome p0a-outcome-${outcome}`}
        >
          <div className="p0a-outcome-icon"><Activity /></div>
          <div>
            <span>{outcomeCopy.label}</span>
            <h2>{outcomeCopy.title}</h2>
            <p>{outcomeCopy.detail}</p>
            <dl className="p0a-outcome-meta">
              <div><dt>覆盖情况</dt><dd>{outcomeCopy.coverage}</dd></div>
              <div><dt>最近成功</dt><dd>{outcomeCopy.lastSuccess}</dd></div>
              <div><dt>下一步</dt><dd>{outcomeCopy.nextStep}</dd></div>
            </dl>
          </div>
        </section>
        {outcome === "partial" && (
          <section className="p0a-panel p0a-partial-results" aria-label="已完成的部分结果">
            <div className="p0a-partial-heading">
              <span>当前可用</span>
              <h3>已完成的 2 条更新</h3>
            <p>以下内容来自已完成检查的官方来源；Anthropic 暂不作结论。</p>
            </div>
            <div className="p0a-insight-list">
              {[prototypePresentation.insights[0], prototypePresentation.insights[2]].map((insight) => (
                <InsightCard
                  key={insight.id}
                  insight={insight}
                  feedback={feedback[insight.id] ?? { vote: null }}
                  onFeedback={(next) => onFeedback(insight.id, next)}
                />
              ))}
            </div>
          </section>
        )}
        </>
      ) : (
    <section
      id="p0a-panel-latest"
      role="tabpanel"
      aria-labelledby="p0a-tab-latest"
      className="p0a-panel"
      aria-label="最新简报"
    >
      <header className="p0a-issue-head">
        <div>
          <div className="p0a-kicker"><span />{issue.label} · {issue.timeRange}</div>
          <div className="p0a-example-badge">理解测试样本 · 官方事实已核验</div>
          <h2>{issue.headline}</h2>
          <p>{issue.overview}</p>
          <p className="p0a-top-signal">{issue.topSignal}</p>
        </div>
        <div className="p0a-issue-meta">
          <span><Check />{issue.generatedAt}</span>
          <span><ShieldCheck />{issue.coverage}</span>
        </div>
      </header>
      <div className="p0a-insight-list">
        {prototypePresentation.insights.map((insight) => (
          <InsightCard
            key={insight.id}
            insight={insight}
            feedback={feedback[insight.id] ?? { vote: null }}
            onFeedback={(next) => onFeedback(insight.id, next)}
          />
        ))}
      </div>
      <div className="p0a-more"><BookOpen />本期共 5 条 · 原型展示前 3 条</div>
    </section>
      )}
    </>
  );
}

function HistoryBriefs() {
  const [expanded, setExpanded] = useState(0);
  return (
    <section id="p0a-panel-history" role="tabpanel" aria-labelledby="p0a-tab-history" className="p0a-panel p0a-history" aria-label="历史简报">
      <div className="p0a-section-heading">
        <div><p>历史简报</p><h2>按“每一期”回看，而不是翻运行日志</h2></div>
        <span>共 9 期</span>
      </div>
      {prototypePresentation.history.map((item, index) => (
        <article key={item.date} className="p0a-history-item">
          <button
            aria-expanded={expanded === index}
            aria-controls={`p0a-history-${index}`}
            onClick={() => setExpanded(expanded === index ? -1 : index)}
          >
            <span className="p0a-history-date">{item.date}</span>
            <span><strong>{item.title}</strong><small>{item.summary}</small></span>
            <ChevronDown className={expanded === index ? "rotated" : ""} />
          </button>
          {expanded === index && (
            <div id={`p0a-history-${index}`} className="p0a-history-detail">
              <span><Check />本期检查已完成</span>
              <p>这是原型中的历史摘要。正式版本将展示该期结论、来源与反馈状态。</p>
            </div>
          )}
        </article>
      ))}
    </section>
  );
}

function SettingsPanel({ paused, setPaused }: { paused: boolean; setPaused: (v: boolean) => void }) {
  const [diagnostics, setDiagnostics] = useState(false);
  return (
    <section id="p0a-panel-settings" role="tabpanel" aria-labelledby="p0a-tab-settings" className="p0a-panel p0a-settings" aria-label="任务设置">
      <div className="p0a-section-heading">
        <div><p>任务设置</p><h2>它在关注什么，什么时候检查</h2></div>
        <span className="p0a-prototype-only"><Pencil />原型暂不提供编辑</span>
      </div>
      <div className="p0a-setting-grid">
        <div><span>关注范围</span><strong>{prototypePresentation.task.scope}</strong></div>
        <div><span>判断标准</span><strong>{prototypePresentation.task.criteria}</strong></div>
        <div><span>检查节奏</span><strong>{prototypePresentation.task.cadence}</strong></div>
        <div><span>推送位置</span><strong>{prototypePresentation.task.channel}</strong></div>
      </div>
      <div className="p0a-control-row">
        <div>
          <strong>{paused ? "任务已暂停" : "任务正在持续关注"}</strong>
          <span>{paused ? "恢复后将继续检查新变化" : "下次检查：明天 09:00"}</span>
        </div>
        <button className="p0a-secondary" aria-pressed={paused} onClick={() => setPaused(!paused)}>
          <CirclePause />{paused ? "恢复任务" : "暂停任务"}
        </button>
      </div>
      <div className="p0a-diagnostics">
        <button
          aria-expanded={diagnostics}
          aria-controls="p0a-diagnostic-detail"
          onClick={() => setDiagnostics(!diagnostics)}
        >
          <span><Activity /><strong>运行与诊断</strong><small>仅在排障时查看</small></span>
          <ChevronDown className={diagnostics ? "rotated" : ""} />
        </button>
        {diagnostics && (
          <div id="p0a-diagnostic-detail" className="p0a-diagnostic-grid">
            <div><span>近 7 天运行</span><strong>{ownerPreviewFixture.rawStats.runs7d} 次</strong></div>
            <div><span>无重要变化</span><strong>{ownerPreviewFixture.rawStats.emptyRuns7d} 次</strong></div>
            <div><span>渠道投递</span><strong>{ownerPreviewFixture.rawStats.deliveries7d} 次</strong></div>
            <div><span>LLM 成本</span><strong>${ownerPreviewFixture.rawStats.llmCostUSD}</strong></div>
          </div>
        )}
      </div>
    </section>
  );
}

export default function PrototypeTaskDetail() {
  const [tab, setTab] = useState<Tab>("latest");
  const [paused, setPaused] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);
  const [feedback, setFeedback] = useState<Record<string, Feedback>>({});
  const menuButtonRef = useRef<HTMLButtonElement>(null);
  const closeMenu = () => {
    setMenuOpen(false);
    requestAnimationFrame(() => menuButtonRef.current?.focus());
  };
  const onTabKeyDown = (
    event: ReactKeyboardEvent<HTMLButtonElement>,
    index: number,
  ) => {
    if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
    event.preventDefault();
    const direction = event.key === "ArrowRight" ? 1 : -1;
    const nextIndex = (index + direction + taskTabs.length) % taskTabs.length;
    const nextTab = taskTabs[nextIndex][0];
    setTab(nextTab);
    requestAnimationFrame(() =>
      document.getElementById(`p0a-tab-${nextTab}`)?.focus(),
    );
  };
  return (
    <div className="p0a-app" data-prototype-marker="VANE_P0A_OWNER_PREVIEW">
      <Sidebar open={menuOpen} onClose={closeMenu} />
      <main className="p0a-main">
        <header className="p0a-mobile-bar">
          <button
            ref={menuButtonRef}
            aria-expanded={menuOpen}
            aria-controls="p0a-sidebar"
            onClick={() => setMenuOpen(true)}
            aria-label="打开导航"
          ><Menu /></button>
          <span className="p0a-mobile-brand">
            <LogoMark className="p0a-mobile-logo" />
            <strong>见微 Vane</strong>
          </span>
          <span />
        </header>
        <div className="p0a-content">
          <button className="p0a-back" disabled title="本轮原型不包含任务列表">
            <ArrowLeft />返回任务列表 · 本轮不测试
          </button>
          <section className="p0a-task-hero">
            <div>
              <div className="p0a-status"><span className={paused ? "paused" : ""} />{paused ? "已暂停" : "持续关注中"}</div>
              <h1>{ownerPreviewFixture.taskTitle}</h1>
              <p>{prototypePresentation.task.summary}</p>
            </div>
            <div className="p0a-next-check">
              <CalendarClock />
              <span><small>下次检查</small><strong>{paused ? "恢复任务后重新安排" : "明天 09:00"}</strong></span>
            </div>
          </section>
          <nav className="p0a-tabs" role="tablist" aria-label="任务内容">
            {taskTabs.map(([value, label, Icon], index) => (
              <button
                key={value}
                id={`p0a-tab-${value}`}
                role="tab"
                aria-selected={tab === value}
                aria-controls={`p0a-panel-${value}`}
                tabIndex={tab === value ? 0 : -1}
                className={tab === value ? "active" : ""}
                onClick={() => setTab(value)}
                onKeyDown={(event) => onTabKeyDown(event, index)}
              ><Icon />{label}</button>
            ))}
          </nav>
          {tab === "latest" && (
            <LatestBrief
              feedback={feedback}
              onFeedback={(id, next) => setFeedback((current) => ({ ...current, [id]: next }))}
            />
          )}
          {tab === "history" && <HistoryBriefs />}
          {tab === "settings" && <SettingsPanel paused={paused} setPaused={setPaused} />}
          <p className="p0a-prototype-note">
            P0-A Owner 验收页 · 仅含匿名示例数据，不接生产 API；三条示例事实可由所附官方原文核验
          </p>
        </div>
      </main>
    </div>
  );
}

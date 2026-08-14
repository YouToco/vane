import { useEffect, useRef, useState } from "react";
import { AnimatePresence, motion, useInView, useReducedMotion } from "motion/react";
import { Check, Inbox, Send, Sparkles } from "lucide-react";
import { LogoMark } from "@/shared/brand/Logo";
import { useI18n } from "@/i18n";
import { cn } from "@/shared/utils/class-names";

// source/tag 只存索引，渲染时查 i18n 字典——切语言即时生效，数据结构语言无关
interface FeedItem {
  id: number;
  sourceIdx: number;
  hot: boolean;
  w1: number;
  w2: number;
}

interface PushItem {
  id: number;
  tagIdx: number;
  w1: number;
  w2: number;
}

const SOURCE_COUNT = 5;
const TAG_COUNT = 3;
const HOT_EVERY = 6; // 每流入 6 条，1 条被判为值得推送
const MAX_FEED = 8;
const MAX_PUSHED = 3;

let nextId = 100;

function makeFeedItem(seq: number): FeedItem {
  return {
    id: nextId++,
    sourceIdx: seq % SOURCE_COUNT,
    hot: seq % HOT_EVERY === HOT_EVERY - 1,
    w1: 55 + Math.round(Math.random() * 40),
    w2: 30 + Math.round(Math.random() * 35),
  };
}

function makePushItem(seq: number): PushItem {
  return {
    id: nextId++,
    tagIdx: seq % TAG_COUNT,
    w1: 62 + Math.round(Math.random() * 30),
    w2: 38 + Math.round(Math.random() * 30),
  };
}

const SEED_FEED: FeedItem[] = Array.from({ length: 5 }, (_, i) => makeFeedItem(i));
const SEED_PUSHED: PushItem[] = Array.from({ length: MAX_PUSHED }, (_, i) => makePushItem(i));

/**
 * 「100 条里只推你 3 条」的动态演示：左列原始信息持续流入，
 * 中间 Vane 过滤器脉冲，右列偶尔弹出一张推送卡。
 * 仅在进入视口时运行；reduced-motion 下为静态快照。
 */
export function FilterShowcase() {
  const reduced = useReducedMotion();
  const { t } = useI18n();
  const L = t.landing;
  const rootRef = useRef<HTMLDivElement>(null);
  const inView = useInView(rootRef, { amount: "some" });

  const [feed, setFeed] = useState<FeedItem[]>(SEED_FEED);
  const [pushed, setPushed] = useState<PushItem[]>(SEED_PUSHED);
  const [rawCount, setRawCount] = useState(1284);
  const [pushCount, setPushCount] = useState(37);
  const [pulseKey, setPulseKey] = useState(0);
  const seqRef = useRef(SEED_FEED.length);

  useEffect(() => {
    if (reduced || !inView) return;
    const timer = setInterval(() => {
      const seq = seqRef.current++;
      const item = makeFeedItem(seq);
      setFeed((f) => [item, ...f].slice(0, MAX_FEED));
      setRawCount((c) => c + 1);
      if (item.hot) {
        // 用推送自己的序号轮换标签：seq 恒 ≡ HOT_EVERY-1 (mod HOT_EVERY)，直接取模标签会卡死在同一个
        setPushed((p) => [makePushItem(Math.floor(seq / HOT_EVERY)), ...p].slice(0, MAX_PUSHED));
        setPushCount((c) => c + 1);
        setPulseKey((k) => k + 1);
      }
    }, 820);
    return () => clearInterval(timer);
  }, [reduced, inView]);

  return (
    <div ref={rootRef} className="grid items-center gap-8 md:grid-cols-[1fr_auto_1fr]">
      {/* 左：原始信息流 */}
      <div>
        <div className="mb-3 flex items-center gap-2 text-sm text-muted-foreground">
          <Inbox className="size-4" />
          <span>{L.feedIn}</span>
          <span className="ml-auto font-semibold tabular-nums text-foreground">
            {rawCount.toLocaleString()}
          </span>
          <span className="text-xs">{L.unit}</span>
        </div>
        <div className="relative h-72 overflow-hidden [mask-image:linear-gradient(to_bottom,transparent,#000_10%,#000_78%,transparent)]">
          <div className="flex flex-col gap-2">
            <AnimatePresence initial={false}>
              {feed.map((item) => (
                <motion.div
                  key={item.id}
                  layout
                  initial={{ y: -18, opacity: 0 }}
                  animate={{ y: 0, opacity: 1 }}
                  exit={{ opacity: 0 }}
                  transition={{ duration: 0.32 }}
                  className={cn(
                    "rounded-lg border border-border/60 bg-card/60 px-3 py-2",
                    item.hot && "border-brand/40 bg-brand/5",
                  )}
                >
                  <div className="flex items-center gap-2">
                    <span className="rounded bg-muted px-1.5 py-0.5 text-[10px] text-muted-foreground">
                      {L.sources[item.sourceIdx % L.sources.length]}
                    </span>
                    {item.hot && <Sparkles className="size-3 text-brand-strong" />}
                    <div
                      className="ml-auto h-1.5 rounded-full bg-muted"
                      style={{ width: `${item.w2 * 0.5}%` }}
                    />
                  </div>
                  <div className="mt-2 h-1.5 rounded-full bg-muted" style={{ width: `${item.w1}%` }} />
                </motion.div>
              ))}
            </AnimatePresence>
          </div>
        </div>
      </div>

      {/* 中：Vane 过滤器（品牌印章） */}
      <div className="relative flex flex-col items-center gap-3 md:px-12">
        <div
          aria-hidden
          className="absolute left-0 top-[38%] hidden h-px w-11 bg-gradient-to-r from-transparent via-border to-border md:block"
        >
          <span className="absolute top-1/2 size-1.5 -translate-y-1/2 rounded-full bg-brand animate-[vane-flow_1.9s_linear_infinite]" />
        </div>
        <div
          aria-hidden
          className="absolute right-0 top-[38%] hidden h-px w-11 bg-gradient-to-l from-transparent via-border to-border md:block"
        >
          <span className="absolute top-1/2 size-1.5 -translate-y-1/2 rounded-full bg-signal animate-[vane-flow_1.9s_linear_infinite_0.9s]" />
        </div>

        <div className="relative">
          <span
            aria-hidden
            className="absolute inset-0 rounded-2xl bg-brand/25 animate-[vane-ping-slow_2.6s_ease-out_infinite]"
          />
          <motion.div
            key={pulseKey}
            animate={reduced ? {} : { scale: [1, 1.1, 1] }}
            transition={{ duration: 0.45 }}
            className="relative rounded-2xl shadow-lg shadow-brand/25"
          >
            {/* 容器圆角(16px)与印章 rect 的视觉圆角(size-16 时 rx=16px)对齐，阴影才不露直角 */}
            <LogoMark className="size-16" />
          </motion.div>
        </div>
        <div className="text-center">
          <p className="text-sm font-medium">{L.filterName}</p>
          <p className="text-xs text-muted-foreground">{L.filterDesc}</p>
        </div>
      </div>

      {/* 右：推送给你的 */}
      <div>
        <div className="mb-3 flex items-center gap-2 text-sm text-muted-foreground">
          <Send className="size-4" />
          <span>{L.feedOut}</span>
          <span className="ml-auto font-semibold tabular-nums text-foreground">{pushCount}</span>
          <span className="text-xs">{L.unit}</span>
        </div>
        <div className="flex h-72 flex-col justify-center gap-3">
          <AnimatePresence initial={false} mode="popLayout">
            {pushed.map((item) => (
              <motion.div
                key={item.id}
                layout
                initial={{ x: -30, opacity: 0, scale: 0.94 }}
                animate={{ x: 0, opacity: 1, scale: 1 }}
                exit={{ opacity: 0, y: 12, scale: 0.96 }}
                transition={{ type: "spring", stiffness: 320, damping: 26 }}
                className="rounded-xl border border-brand/25 bg-card p-3.5 shadow-sm shadow-brand/5"
              >
                <div className="mb-2.5 flex items-center gap-1.5">
                  <span className="flex size-4 items-center justify-center rounded-full bg-emerald-500/15">
                    <Check className="size-2.5 text-emerald-600 dark:text-emerald-400" />
                  </span>
                  <span className="text-[11px] font-medium text-emerald-600 dark:text-emerald-400">
                    {L.tags[item.tagIdx % L.tags.length]}
                  </span>
                  <span className="ml-auto text-[10px] text-muted-foreground">{L.justNow}</span>
                </div>
                <div
                  className="h-2 rounded-full bg-gradient-to-r from-brand/40 to-signal/30"
                  style={{ width: `${item.w1}%` }}
                />
                <div className="mt-1.5 h-2 rounded-full bg-muted" style={{ width: `${item.w2}%` }} />
              </motion.div>
            ))}
          </AnimatePresence>
        </div>
      </div>
    </div>
  );
}

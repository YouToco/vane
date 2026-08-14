import { useEffect, useState } from "react";
import { AnimatePresence, motion, useReducedMotion } from "motion/react";
import { ArrowUp, Check, Sparkles } from "lucide-react";
import { EXAMPLE_ICONS } from "@/pages/Landing";
import { useI18n } from "@/i18n";

type Phase = "typing" | "holding" | "deleting";

/**
 * 模拟「对 Vane 说一句话」的输入框：循环打字 → 停顿并弹出
 * 「已生成任务」回执 → 删除 → 下一句。reduced-motion 下退化为
 * 整句轮播（无逐字动画）。文案随 i18n 字典切换并重置轮播。
 */
// 注意：使用处以 key={locale} 挂载本组件——切语言整体重挂，内部无须处理语言切换。
export function TypewriterDemo() {
  const reduced = useReducedMotion();
  const { t } = useI18n();
  const examples = t.landing.examples;

  const [idx, setIdx] = useState(0);
  const [chars, setChars] = useState(0);
  const [phase, setPhase] = useState<Phase>("typing");

  const ex = examples[idx % examples.length] ?? examples[0];
  const full = ex?.text ?? "";

  useEffect(() => {
    if (reduced) return;
    let timer: ReturnType<typeof setTimeout>;
    if (phase === "typing") {
      timer =
        chars < full.length
          ? setTimeout(() => setChars((c) => c + 1), 46 + Math.random() * 50)
          : setTimeout(() => setPhase("holding"), 260);
    } else if (phase === "holding") {
      timer = setTimeout(() => setPhase("deleting"), 2400);
    } else {
      timer =
        chars > 0
          ? setTimeout(() => setChars((c) => c - 1), 16)
          : setTimeout(() => {
              setIdx((i) => (i + 1) % examples.length);
              setPhase("typing");
            }, 240);
    }
    return () => clearTimeout(timer);
  }, [phase, chars, full, reduced, examples.length]);

  // reduced-motion：整句直接展示，定时轮播
  useEffect(() => {
    if (!reduced) return;
    const timer = setInterval(() => setIdx((i) => (i + 1) % examples.length), 4200);
    return () => clearInterval(timer);
  }, [reduced, examples.length]);

  if (!ex) return null;
  const Icon = EXAMPLE_ICONS[idx % EXAMPLE_ICONS.length] ?? Sparkles;
  const shown = reduced ? full : full.slice(0, chars);
  const showReceipt = reduced || phase === "holding";

  return (
    <div className="relative mx-auto w-full max-w-xl">
      {/* 输入框后的破晓柔光 */}
      <div
        aria-hidden
        className="absolute -inset-2 rounded-3xl bg-gradient-to-r from-brand/15 via-brand/5 to-[var(--glow-c)] blur-xl"
      />
      <div className="relative rounded-2xl border border-border/70 bg-card/85 p-4 text-left shadow-lg shadow-brand/5 backdrop-blur-sm">
        <div className="mb-3 flex items-center gap-2 text-xs text-muted-foreground">
          <Sparkles className="size-3.5 text-brand-strong" />
          <span>{t.landing.demoPrompt}</span>
          <AnimatePresence mode="wait" initial={false}>
            <motion.span
              key={ex.label}
              initial={{ opacity: 0, y: 4 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -4 }}
              transition={{ duration: 0.2 }}
              className="ml-auto inline-flex items-center gap-1 rounded-full bg-muted px-2 py-0.5 text-[11px]"
            >
              <Icon className="size-3" />
              {ex.label}
            </motion.span>
          </AnimatePresence>
        </div>

        <div className="min-h-14 text-base leading-relaxed sm:text-lg">
          {shown}
          {!reduced && (
            <span className="ml-0.5 inline-block h-[1.1em] w-0.5 translate-y-[0.18em] rounded-full bg-brand animate-[vane-caret_1.1s_steps(1)_infinite]" />
          )}
        </div>

        <div className="mt-3 flex min-h-8 items-center justify-between gap-3">
          <AnimatePresence mode="wait">
            {showReceipt && (
              <motion.div
                key={idx}
                initial={{ opacity: 0, y: 8, scale: 0.96 }}
                animate={{ opacity: 1, y: 0, scale: 1 }}
                exit={{ opacity: 0, y: -4 }}
                transition={{ duration: 0.25 }}
                className="inline-flex items-center gap-1.5 rounded-full bg-emerald-500/10 px-2.5 py-1 text-xs font-medium text-emerald-600 dark:text-emerald-400"
              >
                <Check className="size-3" />
                {ex.result}
              </motion.div>
            )}
          </AnimatePresence>
          <motion.div
            aria-hidden
            animate={showReceipt && !reduced ? { scale: [1, 1.14, 1] } : {}}
            transition={{ duration: 0.4 }}
            className="ml-auto flex size-8 shrink-0 items-center justify-center rounded-lg bg-primary text-primary-foreground shadow-sm"
          >
            <ArrowUp className="size-4" />
          </motion.div>
        </div>
      </div>
    </div>
  );
}

import { lazy, Suspense, useEffect, useRef } from "react";
import { motion, useReducedMotion, type Variants } from "motion/react";
import {
  ArrowDown,
  ArrowRight,
  Eye,
  MessageSquare,
  Radar,
  Search,
  ThumbsUp,
  User,
  Zap,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { LocaleSwitch } from "@/components/LocaleSwitch";
import { LogoMark } from "@/components/brand/Logo";
import { TypewriterDemo } from "@/components/landing/TypewriterDemo";
import { FilterShowcase } from "@/components/landing/FilterShowcase";
import { useI18n } from "@/i18n";

export const EXAMPLE_ICONS: readonly LucideIcon[] = [User, Zap, Search, Eye] as const;
const STEP_ICONS: readonly LucideIcon[] = [MessageSquare, Radar, ThumbsUp] as const;

// 3D 吉祥物按需加载（three.js 独立 chunk，不拖累首屏）
const VaneMascot = lazy(() => import("@/components/landing/VaneMascot"));

const fadeUp: Variants = {
  hidden: { opacity: 0, y: 26, filter: "blur(6px)" },
  show: {
    opacity: 1,
    y: 0,
    filter: "blur(0px)",
    transition: { duration: 0.6, ease: [0.22, 1, 0.36, 1] },
  },
};

const stagger: Variants = {
  hidden: {},
  show: { transition: { staggerChildren: 0.1 } },
};

function goLogin() {
  location.hash = "#/login";
}

/** 鼠标光斑：跟随指针的天青柔光。触屏与 reduced-motion 下不启用。 */
function MouseGlow() {
  const reduced = useReducedMotion();
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (reduced || matchMedia("(pointer: coarse)").matches) return;
    const node = ref.current;
    if (!node) return;
    let raf = 0;
    function onMove(e: MouseEvent) {
      cancelAnimationFrame(raf);
      raf = requestAnimationFrame(() => {
        node!.style.background = `radial-gradient(640px at ${e.clientX}px ${e.clientY}px, var(--glow-spot), transparent 68%)`;
      });
    }
    window.addEventListener("mousemove", onMove);
    return () => {
      window.removeEventListener("mousemove", onMove);
      cancelAnimationFrame(raf);
    };
  }, [reduced]);

  return <div ref={ref} aria-hidden className="pointer-events-none fixed inset-0 -z-10" />;
}

export default function Landing() {
  const reduced = useReducedMotion();
  const { t, locale } = useI18n();
  const L = t.landing;

  return (
    <div className="landing-root relative min-h-screen overflow-x-clip bg-background">
      {/* 背景：网格线（工程图纸感）+ 天青彩雾三团 + 鼠标光斑 */}
      <div aria-hidden className="pointer-events-none fixed inset-0 -z-10">
        <div className="absolute inset-0 bg-[linear-gradient(to_right,var(--grid-line)_1px,transparent_1px),linear-gradient(to_bottom,var(--grid-line)_1px,transparent_1px)] bg-[size:56px_56px] [mask-image:linear-gradient(to_bottom,#000,#000_42%,transparent_82%)]" />
        <div className="absolute -top-32 left-[8%] size-[34rem] rounded-full bg-[var(--glow-a)] blur-[110px] animate-[vane-drift-a_26s_ease-in-out_infinite]" />
        <div className="absolute top-[30%] right-[-6%] size-[30rem] rounded-full bg-[var(--glow-b)] blur-[110px] animate-[vane-drift-b_32s_ease-in-out_infinite]" />
        <div className="absolute bottom-[-10%] left-[28%] size-[32rem] rounded-full bg-[var(--glow-c)] blur-[120px] animate-[vane-drift-a_38s_ease-in-out_infinite_reverse]" />
      </div>
      <MouseGlow />

      {/* Header */}
      <motion.header
        initial={reduced ? false : { y: -16, opacity: 0 }}
        animate={{ y: 0, opacity: 1 }}
        transition={{ duration: 0.5 }}
        className="sticky top-0 z-40 border-b border-border/40 bg-background/70 backdrop-blur-md"
      >
        <div className="mx-auto flex max-w-5xl items-center justify-between px-6 py-3">
          <div className="flex items-center gap-2.5">
            <LogoMark />
            <span className="text-base font-semibold">{t.brandName}</span>
          </div>
          <div className="flex items-center gap-3">
            <LocaleSwitch />
            <Button variant="outline" size="sm" onClick={goLogin}>
              {L.login}
            </Button>
          </div>
        </div>
      </motion.header>

      <main className="mx-auto flex max-w-5xl flex-col items-center px-6">
        {/* Hero */}
        <motion.section
          variants={stagger}
          initial={reduced ? false : "hidden"}
          animate="show"
          className="flex w-full flex-col items-center pb-20 pt-16 text-center sm:pt-24"
        >
          <motion.div
            variants={fadeUp}
            className="mb-6 inline-flex items-center gap-2 rounded-full border border-border/70 bg-card/60 px-3.5 py-1.5 text-xs text-muted-foreground backdrop-blur-sm"
          >
            <span className="relative flex size-2">
              <span className="absolute inline-flex size-full rounded-full bg-signal opacity-60 animate-ping" />
              <span className="relative inline-flex size-2 rounded-full bg-signal" />
            </span>
            {L.badge}
          </motion.div>

          <motion.h1
            variants={fadeUp}
            className="mb-5 font-heading text-4xl font-bold leading-[1.2] tracking-tight sm:text-5xl md:text-6xl"
          >
            {L.heroL1}
            <br />
            {L.heroL2Pre}
            <span className="bg-gradient-to-r from-[var(--grad-a)] via-[var(--grad-b)] to-[var(--grad-c)] bg-clip-text text-transparent">
              {L.heroL2Brand}
            </span>
          </motion.h1>

          <motion.p
            variants={fadeUp}
            className="mb-10 max-w-md text-base leading-relaxed text-muted-foreground sm:text-lg"
          >
            {L.heroSub}
          </motion.p>

          <motion.div variants={fadeUp} className="relative w-full">
            {/* 3D 吉祥物坐在输入框右上沿（鼠标是风，点击有随机小动作）；小屏/reduced-motion 不挂载 */}
            {!reduced && (
              <div className="absolute -top-[6.6rem] right-0 z-10 hidden h-28 w-28 sm:block md:right-[8%]">
                <Suspense fallback={null}>
                  <VaneMascot />
                </Suspense>
              </div>
            )}
            {/* key=locale：切语言整体重挂载，避免打字机状态/AnimatePresence 卡在旧语言的过渡边缘态 */}
            <TypewriterDemo key={locale} />
          </motion.div>

          <motion.div variants={fadeUp} className="mt-10 flex flex-wrap items-center justify-center gap-3">
            <div className="group relative">
              <div
                aria-hidden
                className="absolute -inset-1 rounded-xl bg-gradient-to-r from-brand/50 to-signal/50 opacity-40 blur-md transition-opacity duration-300 group-hover:opacity-70"
              />
              <Button size="lg" className="relative px-5" onClick={goLogin}>
                {L.ctaStart}
                <ArrowRight data-icon="inline-end" />
              </Button>
            </div>
            <Button
              variant="ghost"
              size="lg"
              className="text-muted-foreground"
              onClick={() =>
                document.getElementById("showcase")?.scrollIntoView({ behavior: "smooth" })
              }
            >
              {L.ctaHow}
              <ArrowDown data-icon="inline-end" />
            </Button>
          </motion.div>
        </motion.section>

        {/* 过滤演示 */}
        <motion.section
          id="showcase"
          variants={stagger}
          initial={reduced ? false : "hidden"}
          whileInView="show"
          viewport={{ once: true, margin: "-80px" }}
          className="w-full scroll-mt-20 pb-24"
        >
          <motion.div variants={fadeUp} className="mb-10 text-center">
            <h2 className="mb-3 font-heading text-2xl font-bold tracking-tight sm:text-3xl">
              {L.showcaseTitlePre}
              <span className="bg-gradient-to-r from-[var(--grad-b)] to-[var(--grad-c)] bg-clip-text text-transparent">
                {L.showcaseTitleBrand}
              </span>
              {L.showcaseTitlePost}
            </h2>
            <p className="mx-auto max-w-md text-sm leading-relaxed text-muted-foreground sm:text-base">
              {L.showcaseSub}
            </p>
          </motion.div>
          <motion.div variants={fadeUp}>
            <FilterShowcase />
          </motion.div>
        </motion.section>

        {/* 场景示例 */}
        <motion.section
          variants={stagger}
          initial={reduced ? false : "hidden"}
          whileInView="show"
          viewport={{ once: true, margin: "-80px" }}
          className="w-full pb-24"
        >
          <motion.div variants={fadeUp} className="mb-8 text-center">
            <h2 className="mb-3 font-heading text-2xl font-bold tracking-tight sm:text-3xl">{L.scenesTitle}</h2>
            <p className="text-sm text-muted-foreground sm:text-base">{L.scenesSub}</p>
          </motion.div>
          <div className="grid max-w-3xl grid-cols-1 gap-3 sm:mx-auto sm:grid-cols-2">
            {L.examples.map((ex, i) => {
              const Icon = EXAMPLE_ICONS[i % EXAMPLE_ICONS.length] ?? User;
              return (
                <motion.a
                  key={ex.label}
                  href="#/login"
                  variants={fadeUp}
                  className="group rounded-xl border border-border/60 bg-card/70 p-4 backdrop-blur-sm transition-all duration-300 hover:-translate-y-1 hover:border-brand/40 hover:shadow-lg hover:shadow-brand/5"
                >
                  <div className="mb-2 flex items-center gap-1.5 text-xs text-muted-foreground">
                    <Icon className="size-3.5 transition-colors group-hover:text-brand-strong" />
                    <span>{ex.label}</span>
                    <ArrowRight className="ml-auto size-3.5 -translate-x-1 opacity-0 transition-all duration-300 group-hover:translate-x-0 group-hover:opacity-60" />
                  </div>
                  <p className="text-sm leading-relaxed">“{ex.text}”</p>
                </motion.a>
              );
            })}
          </div>
        </motion.section>

        {/* 三步工作流 */}
        <motion.section
          variants={stagger}
          initial={reduced ? false : "hidden"}
          whileInView="show"
          viewport={{ once: true, margin: "-80px" }}
          className="w-full pb-24"
        >
          <motion.h2
            variants={fadeUp}
            className="mb-8 text-center font-heading text-2xl font-bold tracking-tight sm:text-3xl"
          >
            {L.stepsTitle}
          </motion.h2>
          <div className="grid gap-4 sm:grid-cols-3">
            {L.steps.map((step, i) => {
              const Icon = STEP_ICONS[i % STEP_ICONS.length] ?? MessageSquare;
              return (
                <motion.div
                  key={step.title}
                  variants={fadeUp}
                  className="relative overflow-hidden rounded-2xl border border-border/60 bg-card/50 p-6 backdrop-blur-sm"
                >
                  <span
                    aria-hidden
                    className="absolute right-4 top-2 text-5xl font-bold text-foreground/[0.05]"
                  >
                    {String(i + 1).padStart(2, "0")}
                  </span>
                  <div className="mb-4 flex size-10 items-center justify-center rounded-xl bg-brand/12 text-brand-strong">
                    <Icon className="size-5" />
                  </div>
                  <p className="mb-1.5 font-semibold">{step.title}</p>
                  <p className="text-sm leading-relaxed text-muted-foreground">{step.desc}</p>
                </motion.div>
              );
            })}
          </div>
        </motion.section>

        {/* 底部 CTA */}
        <motion.section
          variants={fadeUp}
          initial={reduced ? false : "hidden"}
          whileInView="show"
          viewport={{ once: true, margin: "-60px" }}
          className="w-full pb-24"
        >
          <div className="relative overflow-hidden rounded-3xl border border-border/60 bg-card px-6 py-14 text-center">
            <div
              aria-hidden
              className="absolute inset-0 bg-gradient-to-br from-brand/8 via-transparent to-[var(--glow-c)]"
            />
            <div
              aria-hidden
              className="absolute -top-28 left-1/2 size-72 -translate-x-1/2 rounded-full bg-brand/10 blur-3xl"
            />
            <div className="relative">
              <h2 className="mb-3 font-heading text-2xl font-bold tracking-tight sm:text-3xl">{L.ctaCardTitle}</h2>
              <p className="mx-auto mb-8 max-w-sm text-sm leading-relaxed text-muted-foreground sm:text-base">
                {L.ctaCardSub}
              </p>
              <div className="group relative inline-block">
                <div
                  aria-hidden
                  className="absolute -inset-1 rounded-xl bg-gradient-to-r from-brand/50 to-signal/50 opacity-40 blur-md transition-opacity duration-300 group-hover:opacity-70"
                />
                <Button size="lg" className="relative px-6" onClick={goLogin}>
                  {L.ctaStart}
                  <ArrowRight data-icon="inline-end" />
                </Button>
              </div>
            </div>
          </div>
        </motion.section>
      </main>

      <footer className="border-t border-border/40 py-8 text-center text-xs leading-relaxed text-muted-foreground">
        <p className="font-heading">{L.footerLine1}</p>
        <p className="mt-1">{L.footerLine2}</p>
      </footer>
    </div>
  );
}

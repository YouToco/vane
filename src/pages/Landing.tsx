import { Eye, Filter, MessageSquare, Search, ThumbsUp, User, Zap } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";

const EXAMPLES = [
  { icon: User, label: "博主追踪", text: "帮我盯着 XX 的小红书，有新内容就推给我" },
  { icon: Zap, label: "行业日报", text: "每天早上 AI 行业动态，过滤掉营销软文" },
  { icon: Search, label: "一次性调研", text: "帮我找 5 个做 AI 开发工具评测的博主" },
  { icon: Eye, label: "竞品监控", text: "竞品 A/B/C 有新动态时提醒我，一周一汇总" },
] as const;

const FEATURES = [
  { icon: MessageSquare, title: "AI 理解意图", desc: "说人话，不用懂技术" },
  { icon: Filter, title: "智能过滤", desc: "100 条里只留 3 条重要的" },
  { icon: ThumbsUp, title: "越用越准", desc: "反馈驱动，自动进化" },
] as const;

export default function Landing() {
  return (
    <div className="min-h-screen flex flex-col bg-gradient-to-b from-background to-muted/30">
      <header className="flex items-center justify-between px-6 py-4 max-w-5xl mx-auto w-full">
        <div className="flex items-center gap-2">
          <div className="flex size-8 items-center justify-center rounded-lg bg-primary text-primary-foreground">
            <Zap className="size-4" />
          </div>
          <span className="text-base font-semibold">见微 Vane</span>
        </div>
        <Button variant="outline" size="sm" onClick={() => (location.hash = "#/login")}>
          登录
        </Button>
      </header>

      <main className="flex-1 flex flex-col items-center px-6">
        <section className="text-center mt-16 mb-12 max-w-lg">
          <h1 className="text-3xl font-bold tracking-tight mb-3 leading-tight">
            说一句话，
            <br />
            建你的情报系统
          </h1>
          <p className="text-muted-foreground text-base leading-relaxed mb-6">
            告诉 AI 你想追踪什么，它帮你盯着、过滤噪音、只推真正重要的。
          </p>
          <Button size="lg" onClick={() => (location.hash = "#/login")}>
            开始使用
          </Button>
        </section>

        <section className="grid grid-cols-1 sm:grid-cols-2 gap-3 max-w-2xl w-full mb-16">
          {EXAMPLES.map((ex) => (
            <Card key={ex.label} className="border-border/60">
              <CardContent className="p-4">
                <div className="flex items-center gap-1.5 text-xs text-muted-foreground mb-1.5">
                  <ex.icon className="size-3.5" />
                  <span>{ex.label}</span>
                </div>
                <p className="text-sm leading-relaxed">"{ex.text}"</p>
              </CardContent>
            </Card>
          ))}
        </section>

        <section className="grid grid-cols-3 gap-8 max-w-lg w-full text-center mb-20">
          {FEATURES.map((f) => (
            <div key={f.title}>
              <div className="flex justify-center mb-2 text-primary">
                <f.icon className="size-5" />
              </div>
              <p className="text-sm font-medium mb-0.5">{f.title}</p>
              <p className="text-xs text-muted-foreground">{f.desc}</p>
            </div>
          ))}
        </section>
      </main>

      <footer className="text-center py-6 text-xs text-muted-foreground">
        见微 Vane · AI 个性化情报推送
      </footer>
    </div>
  );
}

import { useEffect, useState } from "react";
import { Plus, Loader2, Send, AlertCircle } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "../api";
import type { DeliveryHistoryItem } from "../api";

const BEIJING_TZ = "Asia/Shanghai";
// 后端 parseHistoryQuery 规定 page_size ∈ [1,100]，越界直接 400。
// 这里取满 100：既是「今日推送」计数能拿到的最大窗口，也是合法上限。
const RECENT_WINDOW = 100;

function fmtBeijing(iso?: string | null): string {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleString("zh-CN", { timeZone: BEIJING_TZ, hour12: false });
}

// 判定「今天」用北京时区的日历日，而不是 UTC 或浏览器本地时区：
// 推送记录页展示的就是北京时间，两处口径必须一致，否则用户会看到
// 「今日推送 0」但列表里明明有今天的记录。
function isTodayBeijing(iso?: string | null): boolean {
  if (!iso) return false;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return false;
  const fmt = (x: Date) => x.toLocaleDateString("zh-CN", { timeZone: BEIJING_TZ });
  return fmt(d) === fmt(new Date());
}

interface Stats {
  running: number;
  todayPush: number;
}

export default function Home() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [items, setItems] = useState<DeliveryHistoryItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    let alive = true;
    // 取 RECENT_WINDOW 条来数「今日推送」：后端没有按日计数的接口，只能从最近一页里数。
    // 刻意不 catch 成空数组——曾经 page_size 越界拿了 400，被吞成 items:[] 后页面显示
    // 「还没有推送记录」，和真的没数据长得一模一样，线上错了一整天没人发现。
    // 加载失败必须和「确实没有」分开呈现。
    Promise.all([api.listSchedules(), api.listDeliveries(RECENT_WINDOW)])
      .then(([schedules, deliveries]) => {
        if (!alive) return;
        setItems(deliveries.items.slice(0, 5));
        setStats({
          running: schedules.filter((s) => s.status === "active").length,
          todayPush: deliveries.items.filter((d) => isTodayBeijing(d.sent_at ?? d.created_at))
            .length,
        });
      })
      .catch(() => alive && setFailed(true))
      .finally(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
  }, []);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold">首页</h1>
        <Button size="sm" onClick={() => (location.hash = "#/tasks")}>
          <Plus className="size-4 mr-1" />
          新建任务
        </Button>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="rounded-lg bg-muted/50 p-4">
          <p className="text-xs text-muted-foreground mb-1">运行中任务</p>
          <p className="text-2xl font-semibold">{stats ? stats.running : "—"}</p>
        </div>
        <div className="rounded-lg bg-muted/50 p-4">
          <p className="text-xs text-muted-foreground mb-1">今日推送</p>
          <p className="text-2xl font-semibold">{stats ? stats.todayPush : "—"}</p>
        </div>
      </div>

      <div>
        <div className="flex items-center gap-2 mb-3">
          <Send className="size-4 text-muted-foreground" />
          <h2 className="text-base font-medium">最近推送</h2>
        </div>

        {loading ? (
          <div className="flex items-center justify-center py-8 text-muted-foreground">
            <Loader2 className="size-4 animate-spin mr-2" />
            <span className="text-sm">加载中…</span>
          </div>
        ) : failed ? (
          <div className="flex items-center justify-center gap-2 py-6 text-sm text-destructive">
            <AlertCircle className="size-4" />
            <span>加载失败，请刷新重试</span>
          </div>
        ) : items.length === 0 ? (
          <p className="text-sm text-muted-foreground text-center py-6">还没有推送记录。</p>
        ) : (
          <div className="space-y-2">
            {items.map((it) => (
              <div key={it.id} className="flex items-center gap-3 py-2 text-sm">
                <span className="text-xs text-muted-foreground whitespace-nowrap min-w-[120px]">
                  {fmtBeijing(it.sent_at ?? it.created_at)}
                </span>
                <span className="flex-1 truncate">
                  {it.url ? (
                    <a
                      href={it.url}
                      target="_blank"
                      rel="noreferrer"
                      className="text-primary hover:underline"
                    >
                      {it.title || "(无标题)"}
                    </a>
                  ) : (
                    <span className="text-muted-foreground">{it.title || "(内容已删除)"}</span>
                  )}
                </span>
                <span className="text-xs text-muted-foreground font-mono">{it.score}分</span>
                {it.feedbacks.length > 0 && (
                  <Badge variant="secondary" className="text-xs">
                    {it.feedbacks[0].action === "interested"
                      ? "👍"
                      : it.feedbacks[0].action === "not_interested"
                        ? "👎"
                        : "💬"}
                  </Badge>
                )}
              </div>
            ))}
            <div className="pt-2">
              <Button
                variant="link"
                size="sm"
                className="text-xs px-0"
                onClick={() => (location.hash = "#/history")}
              >
                查看全部推送记录 →
              </Button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

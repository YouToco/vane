import { useEffect, useState } from "react";
import { Plus, Clock, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { api } from "../api";
import type { Schedule } from "../api";
import { describeSpec } from "./Schedules";

// 任务卡读的是真实 schedules：schedules.nl_description 本来就是用户当初
// 用自然语言描述的意图，schedule 就是「任务」在当前后端的载体。
// 不另造 mock 任务——首页的「运行中任务」数也来自同一份数据，两处口径必须一致。
function StatusBadge({ status }: { status: string }) {
  if (status === "active") {
    return (
      <Badge
        variant="outline"
        className="text-emerald-600 border-emerald-200 bg-emerald-50 dark:bg-emerald-950/30 dark:border-emerald-800"
      >
        运行中
      </Badge>
    );
  }
  return (
    <Badge
      variant="outline"
      className="text-amber-600 border-amber-200 bg-amber-50 dark:bg-amber-950/30 dark:border-amber-800"
    >
      已暂停
    </Badge>
  );
}

function TaskCard({ task }: { task: Schedule }) {
  return (
    <Card className="hover:border-border transition-colors">
      <CardContent className="p-5">
        <div className="flex items-start justify-between gap-3 mb-2">
          <div className="min-w-0 flex-1">
            <h3 className="font-medium text-sm">{task.nl_description || describeSpec(task.spec)}</h3>
            <p className="text-xs text-muted-foreground mt-0.5">{describeSpec(task.spec)}</p>
          </div>
          <StatusBadge status={task.status} />
        </div>
        {task.next_run && (
          <div className="flex items-center gap-1 mt-3 text-xs text-muted-foreground">
            <Clock className="size-3" />
            下次 {new Date(task.next_run).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" })}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

// 新建任务对话框目前只有输入框，「发送」尚未接后端：
// 自然语言 → 任务手册的编译能力在后端已存在（agent/playbook_translate.go），
// 但入口是飞书 agent 工具（create_schedule），没有 HTTP 出口。
// 接线属于 P2，这里先不给假的成功反馈。
function CreateTaskDialog({ open, onClose }: { open: boolean; onClose: () => void }) {
  const [input, setInput] = useState("");

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>新建任务</DialogTitle>
        </DialogHeader>
        <div className="space-y-4 pt-2">
          <p className="text-sm text-muted-foreground">
            用一句话告诉我你想追踪什么，AI 会帮你生成任务手册。
          </p>
          <div className="flex gap-2">
            <Input
              placeholder="例：帮我盯着 AI 圈大佬的动态，有重要观点就推给我"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              className="flex-1"
              autoFocus
            />
            <Button disabled size="sm" title="待接后端 interpret 接口（P2）">
              发送
            </Button>
          </div>
          <p className="text-xs text-muted-foreground">
            网页端建任务还没接通，目前请在飞书里对机器人说这句话。
          </p>
        </div>
      </DialogContent>
    </Dialog>
  );
}

export default function TaskDashboard() {
  const [showCreate, setShowCreate] = useState(false);
  const [tasks, setTasks] = useState<Schedule[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let alive = true;
    api
      .listSchedules()
      .then((rows) => alive && setTasks(rows))
      .catch(() => {})
      .finally(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
  }, []);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold">我的任务</h1>
        <Button size="sm" onClick={() => setShowCreate(true)}>
          <Plus className="size-4 mr-1" />
          新建任务
        </Button>
      </div>

      {loading ? (
        <div className="flex items-center justify-center py-12 text-muted-foreground">
          <Loader2 className="size-4 animate-spin mr-2" />
          <span className="text-sm">加载中…</span>
        </div>
      ) : tasks.length === 0 ? (
        <div className="text-center py-16">
          <Clock className="size-10 mx-auto text-muted-foreground/50 mb-3" />
          <p className="text-sm text-muted-foreground mb-4">还没有任务，说一句话就能创建</p>
          <Button onClick={() => setShowCreate(true)}>
            <Plus className="size-4 mr-1" />
            创建第一个任务
          </Button>
        </div>
      ) : (
        <div className="space-y-3">
          {tasks.map((task) => (
            <TaskCard key={task.id} task={task} />
          ))}
        </div>
      )}

      <CreateTaskDialog open={showCreate} onClose={() => setShowCreate(false)} />
    </div>
  );
}

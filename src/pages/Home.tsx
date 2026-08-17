import { useEffect, useState } from "react";
import { api, ApiError } from "../api";
import type { FeishuStatus } from "../api";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Bot, ArrowRight, Wifi, WifiOff } from "lucide-react";

export default function Home() {
  const [status, setStatus] = useState<FeishuStatus | null>(null);
  const [error, setError] = useState("");

  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const s = await api.feishuStatus();
        if (alive) {
          setStatus(s);
          setError("");
        }
      } catch (err) {
        if (alive) setError(err instanceof ApiError ? err.message : "加载失败");
      }
    };
    load();
    const timer = setInterval(load, 5000);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, []);

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle className="text-lg">飞书连接状态</CardTitle>
        </CardHeader>
        <CardContent>
          {status === null && !error && (
            <div className="space-y-3">
              <Skeleton className="h-4 w-48" />
              <Skeleton className="h-4 w-64" />
              <Skeleton className="h-4 w-32" />
            </div>
          )}
          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}
          {status !== null && !status.configured && (
            <div className="flex flex-col items-center py-8 text-center">
              <div className="flex size-16 items-center justify-center rounded-full bg-muted mb-4">
                <Bot className="size-8 text-muted-foreground" />
              </div>
              <h3 className="text-lg font-semibold mb-1">尚未接入飞书</h3>
              <p className="text-sm text-muted-foreground mb-6 max-w-sm">
                完成飞书机器人接入后，即可在飞书里直接与 见微 Vane 对话。
              </p>
              <Button asChild>
                <a href="#/setup">
                  前往接入向导
                  <ArrowRight className="ml-2 size-4" />
                </a>
              </Button>
            </div>
          )}
          {status !== null && status.configured && (
            <div className="space-y-4">
              <div className="flex items-center gap-3">
                {status.connected ? (
                  <>
                    <span className="flex size-3 rounded-full bg-emerald-500" />
                    <div className="flex items-center gap-2">
                      <Wifi className="size-4 text-emerald-600" />
                      <span className="font-medium">飞书已连接</span>
                    </div>
                    <Badge variant="secondary" className="ml-auto">在线</Badge>
                  </>
                ) : (
                  <>
                    <span className="flex size-3 rounded-full bg-red-500" />
                    <div className="flex items-center gap-2">
                      <WifiOff className="size-4 text-red-600" />
                      <span className="font-medium">飞书未连接</span>
                    </div>
                    <Badge variant="destructive" className="ml-auto">离线</Badge>
                  </>
                )}
              </div>
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 rounded-lg border p-4">
                <div>
                  <p className="text-xs text-muted-foreground mb-1">机器人</p>
                  <p className="text-sm font-medium">{status.bot_name || "—"}</p>
                </div>
                <div>
                  <p className="text-xs text-muted-foreground mb-1">Owner</p>
                  <p className="text-sm font-medium">
                    {status.owner_name || (status.owner_open_id ? status.owner_open_id : "未捕获")}
                  </p>
                </div>
                <div>
                  <p className="text-xs text-muted-foreground mb-1">连接时间</p>
                  <p className="text-sm font-medium">
                    {status.connected_at
                      ? new Date(status.connected_at).toLocaleString("zh-CN")
                      : "—"}
                  </p>
                </div>
              </div>
              {!status.connected && status.last_error && (
                <Alert variant="destructive">
                  <AlertDescription>最近错误：{status.last_error}</AlertDescription>
                </Alert>
              )}
              {!status.connected && (
                <Button variant="outline" asChild>
                  <a href="#/setup">
                    去接入向导排查
                    <ArrowRight className="ml-2 size-4" />
                  </a>
                </Button>
              )}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

import { useEffect, useState } from "react";
import { ExternalLink, Link2, Loader2, Send, Unlink } from "lucide-react";
import { api, ApiError } from "@/shared/api/client";
import type { TelegramLink, TelegramStatus } from "@/shared/api/client";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function TelegramSetup() {
  const [status, setStatus] = useState<TelegramStatus | null>(null);
  const [link, setLink] = useState<TelegramLink | null>(null);
  const [busy, setBusy] = useState<"link" | "test" | "unlink" | "">("");
  const [message, setMessage] = useState("");
  const [statusError, setStatusError] = useState("");

  async function refresh() {
    try {
      setStatus(await api.telegramStatus());
      setStatusError("");
    } catch (error) {
      setStatusError(error instanceof ApiError ? error.message : "读取 Telegram 状态失败");
    }
  }

  useEffect(() => {
    let alive = true;
    let inFlight = false;
    const load = async () => {
      if (inFlight) return;
      inFlight = true;
      try {
        const next = await api.telegramStatus();
        if (alive) {
          setStatus(next);
          setStatusError("");
        }
      } catch (error) {
        if (alive) {
          setStatusError(error instanceof ApiError ? error.message : "读取 Telegram 状态失败");
        }
      } finally {
        inFlight = false;
      }
    };
    void load();
    const timer = setInterval(load, 4000);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, []);

  async function issueLink() {
    setBusy("link");
    setMessage("");
    try {
      setLink(await api.telegramLink());
    } catch (error) {
      setMessage(error instanceof ApiError ? error.message : "生成配对链接失败");
    } finally {
      setBusy("");
    }
  }

  async function testConnection() {
    setBusy("test");
    setMessage("");
    try {
      await api.telegramTest();
      setMessage("测试消息已确认送达 Telegram。");
    } catch (error) {
      setMessage(error instanceof ApiError ? error.message : "测试消息发送失败");
    } finally {
      setBusy("");
    }
  }

  async function unlink() {
    setBusy("unlink");
    setMessage("");
    try {
      await api.telegramUnlink();
      setLink(null);
      await refresh();
      setMessage("Telegram 绑定已解除。旧会话将不再获得 Vane 权限。");
    } catch (error) {
      setMessage(error instanceof ApiError ? error.message : "解除绑定失败");
    } finally {
      setBusy("");
    }
  }

  return (
    <Card>
      <CardHeader className="space-y-2">
        <div className="flex items-center justify-between gap-3">
          <CardTitle className="text-base">Telegram Bot</CardTitle>
          <Badge variant={status?.bound ? "default" : "secondary"}>
            {status?.bound ? "已绑定" : status?.ready ? "待绑定" : "未启用"}
          </Badge>
        </div>
        <p className="text-sm text-muted-foreground">
          通过一次性配对链接，把当前 Vane 账号绑定到 Telegram 私聊。群聊和未绑定用户不会进入 Agent。
        </p>
      </CardHeader>
      <CardContent className="space-y-4">
        {status === null && !statusError && (
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="size-4 animate-spin" /> 正在读取状态
          </div>
        )}

        {statusError && (
          <Alert variant="destructive">
            <AlertDescription>
              {statusError}。请稍后重试或刷新页面。
            </AlertDescription>
          </Alert>
        )}

        {status && !status.enabled && (
          <Alert>
            <AlertDescription>
              服务端尚未启用 Telegram。Bot token 与 webhook secret 只由部署凭证提供，不会在网页中填写或回显。
            </AlertDescription>
          </Alert>
        )}

        {status?.enabled && !status.ready && (
          <Alert variant="destructive">
            <AlertDescription>
              Telegram 初始化未通过{status.last_error_code ? `（${status.last_error_code}）` : ""}。在 getMe、webhook 安装与状态核对全部成功前不会接收消息。
            </AlertDescription>
          </Alert>
        )}

        {!!status?.blocked_reply_count && (
          <Alert variant="destructive">
            <AlertDescription>
              有 {status.blocked_reply_count} 条回复发送失败或已跨过 Telegram 边界但无法证明最终状态，系统不会自动盲重发；请由运维核对后处理。
            </AlertDescription>
          </Alert>
        )}

        {status?.ready && !status.bound && (
          <div className="space-y-3">
            <div className="text-sm">
              当前机器人：<span className="font-medium">@{status.bot_username}</span>
            </div>
            <Button onClick={() => void issueLink()} disabled={busy !== ""}>
              {busy === "link" ? <Loader2 className="mr-2 size-4 animate-spin" /> : <Link2 className="mr-2 size-4" />}
              生成 10 分钟一次性配对链接
            </Button>
            {link && (
              <div className="rounded-lg border bg-muted/30 p-3 space-y-2">
                <p className="text-xs text-muted-foreground">
                  仅在自己的 Telegram 客户端打开；链接使用一次后立即失效。
                </p>
                <a
                  className={buttonVariants({ variant: "outline", size: "sm" })}
                  href={link.deep_link}
                  target="_blank"
                  rel="noreferrer"
                >
                  打开 Telegram 完成绑定 <ExternalLink className="ml-2 size-3" />
                </a>
              </div>
            )}
          </div>
        )}

        {status?.ready && status.bound && (
          <div className="flex flex-wrap gap-2">
            <Button variant="outline" onClick={() => void testConnection()} disabled={busy !== ""}>
              {busy === "test" ? <Loader2 className="mr-2 size-4 animate-spin" /> : <Send className="mr-2 size-4" />}
              发送测试消息
            </Button>
            <Button variant="destructive" onClick={() => void unlink()} disabled={busy !== ""}>
              {busy === "unlink" ? <Loader2 className="mr-2 size-4 animate-spin" /> : <Unlink className="mr-2 size-4" />}
              解除绑定
            </Button>
          </div>
        )}

        {message && <p className="text-sm text-muted-foreground">{message}</p>}
      </CardContent>
    </Card>
  );
}

import { useEffect, useState } from "react";
import { ExternalLink, KeyRound, Link2, Loader2, MessagesSquare, RotateCcw, Send, ShieldCheck, Trash2, Unlink } from "lucide-react";
import { api, ApiError } from "@/shared/api/client";
import type { CredentialStatus, TelegramLink, TelegramStatus } from "@/shared/api/client";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

export default function TelegramSetup() {
  const [status, setStatus] = useState<TelegramStatus | null>(null);
  const [credential, setCredential] = useState<CredentialStatus | null>(null);
  const [botToken, setBotToken] = useState("");
  const [link, setLink] = useState<TelegramLink | null>(null);
  const [routeLink, setRouteLink] = useState<TelegramLink | null>(null);
  const [busy, setBusy] = useState<"credential" | "credential-revoke" | "link" | "route" | "test" | "unlink" | `route-${number}` | "">("");
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

  useEffect(() => {
    api.telegramCredentialStatus()
      .then(setCredential)
      .catch((error) => setMessage(error instanceof ApiError ? error.message : "读取 Bot 凭证状态失败"));
  }, []);

  async function saveCredential() {
    setBusy("credential");
    setMessage("");
    try {
      const next = await api.telegramRotateCredential({ bot_token: botToken });
      setCredential(next);
      setBotToken("");
      await refresh();
      setMessage(`第 ${next.generation} 代个人 Telegram Bot 凭证已加密保存并启用。`);
    } catch (error) {
      setMessage(error instanceof ApiError ? error.message : "保存 Telegram Bot 凭证失败");
    } finally {
      setBusy("");
    }
  }

  async function revokeCredential() {
    if (!window.confirm("撤销后，这个 Bot 将立即停止接收和发送 Vane 消息。确定继续？")) return;
    setBusy("credential-revoke");
    setMessage("");
    try {
      await api.telegramRevokeCredential();
      setCredential(await api.telegramCredentialStatus());
      setStatus(await api.telegramStatus());
      setLink(null);
      setRouteLink(null);
      setMessage("个人 Telegram Bot 凭证已撤销；密文历史仅保留审计。");
    } catch (error) {
      setMessage(error instanceof ApiError ? error.message : "撤销 Telegram Bot 凭证失败");
    } finally {
      setBusy("");
    }
  }

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

  async function issueRouteLink() {
    setBusy("route");
    setMessage("");
    try {
      setRouteLink(await api.telegramRouteLink());
    } catch (error) {
      setMessage(error instanceof ApiError ? error.message : "生成群组连接链接失败");
    } finally {
      setBusy("");
    }
  }

  async function unlinkRoute(id: number) {
    setBusy(`route-${id}`);
    setMessage("");
    try {
      await api.telegramRouteUnlink(id);
      await refresh();
      setMessage("群组或话题连接已解除。");
    } catch (error) {
      setMessage(error instanceof ApiError ? error.message : "解除群组连接失败");
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
      setRouteLink(null);
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
          私聊完成身份绑定后，可把自己的 Bot 安全连接到群组或论坛话题。群里仅响应已绑定用户的命令、@提及和对 Bot 的回复。
        </p>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="space-y-3 rounded-lg border p-4">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <p className="flex items-center gap-2 text-sm font-medium">
              <KeyRound className="size-4" />我的 Bot 凭证
            </p>
            <Badge variant={credential?.configured ? "default" : "secondary"}>
              {credential?.configured ? `已保存 · 第 ${credential.generation} 代` : "未配置"}
            </Badge>
          </div>
          <p className="text-xs text-muted-foreground">
            每位用户配置自己的 Bot，用于私聊、群组、话题和后续推送。Token 以 AES-GCM 密文保存，旧值不会返回浏览器。
          </p>
          {credential && !credential.vault_ready && (
            <Alert variant="destructive"><AlertDescription>部署侧尚未配置凭证库主密钥，暂不能保存。</AlertDescription></Alert>
          )}
          <Alert>
            <ShieldCheck className="size-4" />
            <AlertDescription>数据库只保存密文、版本、指纹与 Bot ID；加密主密钥仍由部署凭证持有。</AlertDescription>
          </Alert>
          <div className="space-y-2">
            <Label htmlFor="telegram-bot-token">新的 Bot Token</Label>
            <Input
              id="telegram-bot-token"
              type="password"
              autoComplete="new-password"
              value={botToken}
              onChange={(event) => setBotToken(event.target.value)}
              placeholder="从 BotFather 获取；保存后不会回显"
            />
          </div>
          {credential?.fingerprint && (
            <p className="text-xs text-muted-foreground">当前指纹：<code>{credential.fingerprint.slice(0, 16)}…</code></p>
          )}
          <div className="flex flex-wrap gap-2">
            <Button onClick={() => void saveCredential()} disabled={busy !== "" || !botToken || credential?.vault_ready === false}>
              {busy === "credential" ? <Loader2 className="mr-2 size-4 animate-spin" /> : <RotateCcw className="mr-2 size-4" />}
              校验、加密并启用
            </Button>
            {credential?.configured && (
              <Button variant="destructive" onClick={() => void revokeCredential()} disabled={busy !== ""}>
                {busy === "credential-revoke" ? <Loader2 className="mr-2 size-4 animate-spin" /> : <Trash2 className="mr-2 size-4" />}
                撤销 Bot 凭证
              </Button>
            )}
          </div>
        </div>

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
              当前用户尚未启用 Telegram Bot。请在上方填写自己的 Bot Token；保存成功后运行时会立即安装独立 webhook。
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
          <div className="space-y-4">
            <div className="flex flex-wrap gap-2">
              <Button variant="outline" onClick={() => void testConnection()} disabled={busy !== ""}>
                {busy === "test" ? <Loader2 className="mr-2 size-4 animate-spin" /> : <Send className="mr-2 size-4" />}
                发送测试消息
              </Button>
              <Button variant="outline" onClick={() => void issueRouteLink()} disabled={busy !== ""}>
                {busy === "route" ? <Loader2 className="mr-2 size-4 animate-spin" /> : <MessagesSquare className="mr-2 size-4" />}
                连接群组或话题
              </Button>
              <Button variant="destructive" onClick={() => void unlink()} disabled={busy !== ""}>
                {busy === "unlink" ? <Loader2 className="mr-2 size-4 animate-spin" /> : <Unlink className="mr-2 size-4" />}
                解除绑定（全部）
              </Button>
            </div>

            {routeLink && (
              <div className="rounded-lg border bg-muted/30 p-3 space-y-2">
                <p className="text-sm font-medium">在 10 分钟内完成连接</p>
                <p className="text-xs text-muted-foreground">
                  打开链接把 Bot 加入目标群；若目标是论坛话题，请再把下面的命令发送到该话题。执行者必须是群管理员和当前已绑定的 Vane 用户。
                </p>
                <a className={buttonVariants({ variant: "outline", size: "sm" })} href={routeLink.deep_link} target="_blank" rel="noreferrer">
                  添加 Bot 到群组 <ExternalLink className="ml-2 size-3" />
                </a>
                {routeLink.command && (
                  <code className="block overflow-x-auto rounded bg-background px-3 py-2 text-xs select-all">
                    {routeLink.command}
                  </code>
                )}
              </div>
            )}

            {!!status.routes?.filter((route) => route.kind !== "private").length && (
              <div className="space-y-2">
                <p className="text-sm font-medium">已连接的群组与话题</p>
                {status.routes?.filter((route) => route.kind !== "private").map((route) => (
                  <div key={route.id} className="flex items-center justify-between rounded border px-3 py-2 text-sm">
                    <span>{route.kind === "topic" ? "论坛话题" : "群组"} #{route.id} · {route.chat_type}</span>
                    <Button size="sm" variant="ghost" onClick={() => void unlinkRoute(route.id)} disabled={busy !== ""}>
                      {busy === `route-${route.id}` ? <Loader2 className="size-4 animate-spin" /> : <Trash2 className="size-4" />}
                      <span className="sr-only">解除连接</span>
                    </Button>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {message && <p className="text-sm text-muted-foreground">{message}</p>}
      </CardContent>
    </Card>
  );
}

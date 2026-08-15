import { useState, type FormEvent } from "react";
import { Loader2, LockKeyhole, MailCheck } from "lucide-react";

import { LocaleSwitch } from "@/app/LocaleSwitch";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useI18n } from "@/i18n";
import { ApiError, api } from "@/shared/api/client";
import { LogoMark } from "@/shared/brand/Logo";

type SecurityRoute = "forgot" | "verify" | "reset";

function routeFromHash(hash: string): SecurityRoute {
  if (hash.startsWith("#/verify-email")) return "verify";
  if (hash.startsWith("#/reset-password")) return "reset";
  return "forgot";
}

function tokenFromHash(hash: string): string {
  const query = hash.includes("?") ? hash.slice(hash.indexOf("?") + 1) : "";
  return new URLSearchParams(query).get("token")?.trim() ?? "";
}

export default function AccountSecurity({ hash }: { hash: string }) {
  const { locale, t } = useI18n();
  const zh = locale === "zh" || locale === "zh-Hant";
  const route = routeFromHash(hash);
  const token = tokenFromHash(hash);
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [confirmation, setConfirmation] = useState("");
  const [loading, setLoading] = useState(false);
  const [done, setDone] = useState(false);
  const [message, setMessage] = useState("");

  const title = route === "forgot"
    ? (zh ? "重置密码" : "Reset password")
    : route === "verify"
      ? (zh ? "验证邮箱" : "Verify email")
      : (zh ? "设置新密码" : "Set a new password");
  const description = route === "forgot"
    ? (zh ? "我们会向已注册邮箱发送一次性重置链接。" : "We will send a one-time reset link to a registered email.")
    : route === "verify"
      ? (zh ? "确认后将消费这枚一次性验证令牌。" : "Confirm to consume this one-time verification token.")
      : (zh ? "重置成功后，所有已有登录会话都会失效。" : "All existing sessions are revoked after a successful reset.");

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (loading || done) return;
    if (route === "reset" && password !== confirmation) {
      setMessage(zh ? "两次输入的密码不一致" : "Passwords do not match");
      return;
    }
    setLoading(true);
    setMessage("");
    try {
      if (route === "forgot") {
        const result = await api.requestPasswordReset(email.trim());
        setMessage(result.message);
      } else if (route === "verify") {
        if (!token) throw new Error(zh ? "验证链接缺少令牌" : "The verification link has no token");
        await api.verifyEmail(token);
        setMessage(zh ? "邮箱验证成功" : "Email verified");
      } else {
        if (!token) throw new Error(zh ? "重置链接缺少令牌" : "The reset link has no token");
        await api.completePasswordReset(token, password);
        setMessage(zh ? "密码已更新，请重新登录" : "Password updated. Please sign in again.");
      }
      setDone(true);
    } catch (error) {
      setMessage(error instanceof ApiError || error instanceof Error
        ? error.message
        : (zh ? "操作失败，请重试" : "The operation failed. Please try again."));
    } finally {
      setLoading(false);
    }
  }

  const Icon = route === "verify" ? MailCheck : LockKeyhole;
  return (
    <div className="flex min-h-dvh items-center justify-center bg-background px-5 py-16">
      <div className="absolute right-4 top-4"><LocaleSwitch /></div>
      <form onSubmit={submit} className="w-full max-w-md">
        <Card className="rounded-[1.5rem]">
          <CardHeader>
            <div className="mb-3 flex items-center gap-3">
              <LogoMark className="size-10 rounded-xl" />
              <Icon className="size-5 text-brand-strong" />
            </div>
            <CardTitle>{title}</CardTitle>
            <CardDescription>{description}</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {route === "forgot" && (
              <div className="space-y-2">
                <Label htmlFor="security-email">{zh ? "邮箱" : "Email"}</Label>
                <Input id="security-email" type="email" autoComplete="email" required
                  value={email} onChange={(event) => setEmail(event.target.value)} />
              </div>
            )}
            {route === "reset" && (
              <>
                <div className="space-y-2">
                  <Label htmlFor="new-password">{zh ? "新密码" : "New password"}</Label>
                  <Input id="new-password" type="password" autoComplete="new-password" required
                    value={password} onChange={(event) => setPassword(event.target.value)} />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="confirm-password">{zh ? "再次输入" : "Confirm password"}</Label>
                  <Input id="confirm-password" type="password" autoComplete="new-password" required
                    value={confirmation} onChange={(event) => setConfirmation(event.target.value)} />
                </div>
              </>
            )}
            {message && (
              <Alert variant={done ? "default" : "destructive"}>
                <AlertDescription>{message}</AlertDescription>
              </Alert>
            )}
          </CardContent>
          <CardFooter className="flex flex-col gap-3">
            {!done && (
              <Button type="submit" className="w-full" disabled={loading}>
                {loading && <Loader2 className="mr-2 size-4 animate-spin" />}
                {route === "forgot"
                  ? (zh ? "发送重置邮件" : "Send reset email")
                  : route === "verify"
                    ? (zh ? "确认验证" : "Verify")
                    : (zh ? "更新密码" : "Update password")}
              </Button>
            )}
            <Button type="button" variant="link" onClick={() => { location.hash = "#/login"; }}>
              {zh ? "返回登录" : "Back to sign in"}
            </Button>
          </CardFooter>
        </Card>
        <p className="mt-4 text-center text-xs text-muted-foreground">{t.brandName}</p>
      </form>
    </div>
  );
}

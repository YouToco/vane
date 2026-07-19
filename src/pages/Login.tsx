import { useState } from "react";
import type { FormEvent } from "react";
import { Loader2, Zap, Lock, Mail, Ticket } from "lucide-react";
import { api, ApiError } from "../api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Alert, AlertDescription } from "@/components/ui/alert";

type Mode = "login" | "register";

export default function Login() {
  const [mode, setMode] = useState<Mode>("login");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [inviteCode, setInviteCode] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const isRegister = mode === "register";
  const canSubmit =
    email.trim() !== "" && password !== "" && (!isRegister || inviteCode.trim() !== "");

  function switchMode(next: Mode) {
    setMode(next);
    setError("");
    setPassword("");
    setInviteCode("");
  }

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    if (!canSubmit || loading) return;
    setLoading(true);
    setError("");
    try {
      if (isRegister) {
        await api.register(email.trim(), password, inviteCode.trim());
      } else {
        await api.login(email.trim(), password);
      }
      location.hash = "#/";
      location.reload();
    } catch (err) {
      setError(err instanceof ApiError ? err.message : isRegister ? "注册失败，请重试" : "登录失败，请重试");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-slate-50 via-white to-slate-100 dark:from-slate-950 dark:via-slate-900 dark:to-slate-800 px-4">
      <div className="w-full max-w-sm">
        <div className="flex flex-col items-center mb-8 gap-3">
          <div className="flex items-center justify-center w-12 h-12 rounded-2xl bg-primary shadow-lg">
            <Zap className="w-6 h-6 text-primary-foreground" strokeWidth={2.5} />
          </div>
          <div className="text-center">
            <h1 className="text-2xl font-bold tracking-tight text-foreground">见微 Vane</h1>
            <p className="text-sm text-muted-foreground mt-0.5">AI 情报系统</p>
          </div>
        </div>

        <Card className="shadow-xl border-border/50">
          <CardHeader className="space-y-1 pb-4">
            <CardTitle className="text-lg font-semibold">
              {isRegister ? "注册账号" : "登录"}
            </CardTitle>
            <CardDescription>
              {isRegister ? "输入邀请码创建新账号" : "登录你的情报系统"}
            </CardDescription>
          </CardHeader>

          <form onSubmit={onSubmit}>
            <CardContent className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="email">邮箱</Label>
                <div className="relative">
                  <Mail className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground pointer-events-none" />
                  <Input
                    id="email"
                    type="email"
                    autoComplete="email"
                    placeholder="name@example.com"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    className="pl-9"
                    autoFocus
                  />
                </div>
              </div>

              <div className="space-y-2">
                <Label htmlFor="password">{isRegister ? "设置密码" : "密码"}</Label>
                <div className="relative">
                  <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground pointer-events-none" />
                  <Input
                    id="password"
                    type="password"
                    autoComplete={isRegister ? "new-password" : "current-password"}
                    placeholder={isRegister ? "至少 8 位" : "请输入密码"}
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    className="pl-9"
                  />
                </div>
              </div>

              {isRegister && (
                <div className="space-y-2">
                  <Label htmlFor="inviteCode">邀请码</Label>
                  <div className="relative">
                    <Ticket className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground pointer-events-none" />
                    <Input
                      id="inviteCode"
                      type="text"
                      placeholder="请输入邀请码"
                      value={inviteCode}
                      onChange={(e) => setInviteCode(e.target.value)}
                      className="pl-9"
                    />
                  </div>
                </div>
              )}

              {error && (
                <Alert variant="destructive" className="py-3">
                  <AlertDescription>{error}</AlertDescription>
                </Alert>
              )}
            </CardContent>

            <CardFooter className="flex flex-col gap-3">
              <Button
                type="submit"
                className="w-full"
                disabled={loading || !canSubmit}
              >
                {loading ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    {isRegister ? "注册中…" : "登录中…"}
                  </>
                ) : (
                  isRegister ? "注 册" : "登 录"
                )}
              </Button>
              <Button
                type="button"
                variant="link"
                className="text-sm text-muted-foreground"
                onClick={() => switchMode(isRegister ? "login" : "register")}
                disabled={loading}
              >
                {isRegister ? "已有账号？去登录" : "有邀请码？去注册"}
              </Button>
            </CardFooter>
          </form>
        </Card>
      </div>
    </div>
  );
}

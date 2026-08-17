import { useState } from "react";
import type { FormEvent } from "react";
import { Loader2, Zap, Lock } from "lucide-react";
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

// 登录页：单密码认证（后端 HMAC cookie 会话）。
// 登录成功后跳回首页；失败时把后端的人话错误展示出来（如"密码错误"）。
export default function Login() {
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    if (!password || loading) return;
    setLoading(true);
    setError("");
    try {
      await api.login(password);
      location.hash = "#/";
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "登录失败，请重试");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-slate-50 via-white to-slate-100 dark:from-slate-950 dark:via-slate-900 dark:to-slate-800 px-4">
      <div className="w-full max-w-sm">
        {/* Brand mark above the card */}
        <div className="flex flex-col items-center mb-8 gap-3">
          <div className="flex items-center justify-center w-12 h-12 rounded-2xl bg-primary shadow-lg">
            <Zap className="w-6 h-6 text-primary-foreground" strokeWidth={2.5} />
          </div>
          <div className="text-center">
            <h1 className="text-2xl font-bold tracking-tight text-foreground">见微 Vane</h1>
            <p className="text-sm text-muted-foreground mt-0.5">AI 个性化信息推送</p>
          </div>
        </div>

        <Card className="shadow-xl border-border/50">
          <CardHeader className="space-y-1 pb-4">
            <CardTitle className="text-lg font-semibold">控制台登录</CardTitle>
            <CardDescription>输入访问密码以进入控制台</CardDescription>
          </CardHeader>

          <form onSubmit={onSubmit}>
            <CardContent className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="password">访问密码</Label>
                <div className="relative">
                  <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground pointer-events-none" />
                  <Input
                    id="password"
                    type="password"
                    placeholder="请输入访问密码"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    className="pl-9"
                    autoFocus
                  />
                </div>
              </div>

              {error && (
                <Alert variant="destructive" className="py-3">
                  <AlertDescription>{error}</AlertDescription>
                </Alert>
              )}
            </CardContent>

            <CardFooter>
              <Button
                type="submit"
                className="w-full"
                disabled={loading || !password}
              >
                {loading ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    登录中…
                  </>
                ) : (
                  "登 录"
                )}
              </Button>
            </CardFooter>
          </form>
        </Card>
      </div>
    </div>
  );
}

import { useEffect, useState, type FormEvent, type ReactNode } from "react";
import { KeyRound, Loader2, Lock, Mail, RefreshCw } from "lucide-react";
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
import { LogoMark } from "@/shared/brand/Logo";
import { api, ApiError } from "@/shared/api/client";

export default function Setup({
  unavailable = false,
}: {
  unavailable?: boolean;
}) {
  const { locale } = useI18n();
  const zh = locale === "zh" || locale === "zh-Hant";
  const [token, setToken] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [claimed, setClaimed] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!claimed) return;
    const timer = window.setInterval(() => {
      api
        .setupStatus()
        .then((status) => {
          if (!status.setup_required) location.reload();
        })
        .catch(() => {});
    }, 1500);
    return () => window.clearInterval(timer);
  }, [claimed]);

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (loading || !token.trim() || !email.trim() || !password) return;
    setLoading(true);
    setError("");
    try {
      await api.claimInstallation(token.trim(), email.trim(), password);
      setClaimed(true);
    } catch (cause) {
      setError(
        cause instanceof ApiError
          ? cause.message
          : zh
            ? "初始化失败，请重试"
            : "Setup failed. Please try again.",
      );
    } finally {
      setLoading(false);
    }
  }

  if (unavailable) {
    return (
      <SetupShell>
        <Card className="w-full max-w-lg rounded-3xl">
          <CardHeader>
            <CardTitle>
              {zh ? "暂时无法读取初始化状态" : "Setup status unavailable"}
            </CardTitle>
            <CardDescription>
              {zh
                ? "请确认 Vane 服务和数据库已启动，然后重试。"
                : "Make sure Vane and its database are running, then retry."}
            </CardDescription>
          </CardHeader>
          <CardFooter>
            <Button className="w-full" onClick={() => location.reload()}>
              <RefreshCw className="mr-2 size-4" />
              {zh ? "重新检查" : "Check again"}
            </Button>
          </CardFooter>
        </Card>
      </SetupShell>
    );
  }

  return (
    <SetupShell>
      <form className="w-full max-w-lg" onSubmit={submit}>
        <Card className="rounded-3xl border-brand/20 bg-card/90 shadow-2xl backdrop-blur-xl">
          <CardHeader>
            <CardTitle className="font-heading text-2xl">
              {claimed
                ? zh
                  ? "平台 owner 已创建"
                  : "Platform owner created"
                : zh
                  ? "初始化 Vane"
                  : "Set up Vane"}
            </CardTitle>
            <CardDescription>
              {claimed
                ? zh
                  ? "服务正在安全重启，完成后会自动进入 Vane。"
                  : "Vane is restarting safely. This page will continue automatically."
                : zh
                  ? "只有持有服务器本地一次性令牌的人可以创建首个平台 owner。"
                  : "Only the holder of the host-local one-time token can create the first platform owner."}
            </CardDescription>
          </CardHeader>
          {claimed ? (
            <CardContent className="flex items-center gap-3 pb-8 text-sm text-muted-foreground">
              <Loader2 className="size-5 animate-spin text-brand" />
              {zh ? "等待完整运行时就绪…" : "Waiting for the full runtime…"}
            </CardContent>
          ) : (
            <>
              <CardContent className="space-y-5">
                <Alert>
                  <AlertDescription>
                    {zh ? "在服务器本地运行 " : "Run "}
                    <code className="rounded bg-muted px-1.5 py-0.5">
                      vane setup-token
                    </code>
                    {zh
                      ? " 获取 30 分钟有效的一次性令牌。令牌不会存入数据库明文。"
                      : " on the host to get the 30-minute one-time token. Its plaintext is never stored in the database."}
                  </AlertDescription>
                </Alert>
                <SetupField
                  icon={<KeyRound className="size-4" />}
                  id="setup-token"
                  label={zh ? "一次性初始化令牌" : "One-time setup token"}
                >
                  <Input
                    id="setup-token"
                    type="password"
                    autoComplete="off"
                    value={token}
                    onChange={(e) => setToken(e.target.value)}
                    autoFocus
                  />
                </SetupField>
                <SetupField
                  icon={<Mail className="size-4" />}
                  id="setup-email"
                  label={zh ? "平台 owner 邮箱" : "Platform owner email"}
                >
                  <Input
                    id="setup-email"
                    type="email"
                    autoComplete="email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                  />
                </SetupField>
                <SetupField
                  icon={<Lock className="size-4" />}
                  id="setup-password"
                  label={zh ? "设置密码" : "Set a password"}
                >
                  <Input
                    id="setup-password"
                    type="password"
                    autoComplete="new-password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    placeholder={zh ? "至少 8 位" : "At least 8 characters"}
                  />
                </SetupField>
                {error && (
                  <Alert variant="destructive">
                    <AlertDescription>{error}</AlertDescription>
                  </Alert>
                )}
              </CardContent>
              <CardFooter>
                <Button
                  type="submit"
                  className="w-full"
                  size="lg"
                  disabled={
                    loading || !token.trim() || !email.trim() || !password
                  }
                >
                  {loading && <Loader2 className="mr-2 size-4 animate-spin" />}
                  {loading
                    ? zh
                      ? "正在初始化…"
                      : "Setting up…"
                    : zh
                      ? "创建平台 owner"
                      : "Create platform owner"}
                </Button>
              </CardFooter>
            </>
          )}
        </Card>
      </form>
    </SetupShell>
  );
}

function SetupShell({ children }: { children: ReactNode }) {
  const { t } = useI18n();
  return (
    <main className="relative flex min-h-dvh items-center justify-center overflow-hidden bg-background px-5 py-16">
      <div
        aria-hidden
        className="absolute inset-0 bg-[radial-gradient(circle_at_20%_10%,var(--glow-b),transparent_42%),radial-gradient(circle_at_80%_85%,var(--glow-c),transparent_45%)]"
      />
      <div className="absolute left-5 top-5 flex items-center gap-2">
        <LogoMark />
        <span className="font-semibold">{t.brandName}</span>
      </div>
      <div className="absolute right-5 top-5">
        <LocaleSwitch />
      </div>
      <div className="relative z-10 flex w-full justify-center">{children}</div>
    </main>
  );
}

function SetupField({
  icon,
  id,
  label,
  children,
}: {
  icon: ReactNode;
  id: string;
  label: string;
  children: ReactNode;
}) {
  return (
    <div className="space-y-2">
      <Label htmlFor={id} className="flex items-center gap-2">
        {icon}
        {label}
      </Label>
      {children}
    </div>
  );
}

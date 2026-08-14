import { useState } from "react";
import type { FormEvent } from "react";
import { Loader2, Lock, Mail, Ticket } from "lucide-react";
import { LogoMark } from "@/shared/brand/Logo";
import { LocaleSwitch } from "@/app/LocaleSwitch";
import { api, ApiError } from "@/shared/api/client";
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
import { useI18n } from "@/i18n";

type Mode = "login" | "register";

/**
 * 品牌面板：跨主题恒定的深夜青底 + 天青 aurora 彩雾（纯 CSS，12s+ 慢漂）。
 * 色值写死——它和印章一样是品牌资产，不随 light/dark 变。
 */
function BrandPanel() {
  const { t } = useI18n();
  return (
    <div className="relative hidden w-[44%] shrink-0 flex-col justify-between overflow-hidden bg-[oklch(0.15_0.012_250)] p-10 lg:flex">
      {/* aurora 彩雾：三团 blur 圆盘缓慢漂移（雨过天青） */}
      <div aria-hidden className="pointer-events-none absolute inset-0">
        <div className="absolute -top-[15%] left-[5%] size-[26rem] rounded-full bg-[oklch(0.62_0.13_245/0.32)] blur-[90px] animate-[vane-aurora_13s_ease-in-out_infinite]" />
        <div className="absolute top-[35%] right-[-12%] size-[24rem] rounded-full bg-[oklch(0.72_0.13_198/0.3)] blur-[90px] animate-[vane-aurora_17s_ease-in-out_infinite_reverse]" />
        <div className="absolute bottom-[-14%] left-[22%] size-[24rem] rounded-full bg-[oklch(0.78_0.13_158/0.26)] blur-[100px] animate-[vane-drift-b_21s_ease-in-out_infinite]" />
        {/* 细网格，工程图纸感 */}
        <div className="absolute inset-0 bg-[linear-gradient(to_right,oklch(0.9_0.01_240/0.05)_1px,transparent_1px),linear-gradient(to_bottom,oklch(0.9_0.01_240/0.05)_1px,transparent_1px)] bg-[size:52px_52px] [mask-image:linear-gradient(to_bottom,#000,transparent_75%)]" />
      </div>

      <div className="relative flex items-center gap-2.5">
        <LogoMark />
        <span className="text-base font-semibold text-[oklch(0.96_0.005_240)]">{t.brandName}</span>
      </div>

      <div className="relative">
        <h2 className="font-heading text-3xl font-bold leading-snug text-[oklch(0.97_0.005_240)] xl:text-4xl">
          {t.auth.slogan}
        </h2>
        <p className="mt-4 max-w-sm text-sm leading-relaxed text-[oklch(0.78_0.015_235)]">
          {t.auth.sloganSub}
        </p>
      </div>

      <p className="relative text-xs text-[oklch(0.62_0.015_235)]">{t.landing.footerLine1}</p>
    </div>
  );
}

export default function Login() {
  const { t } = useI18n();
  const A = t.auth;
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
      setError(err instanceof ApiError ? err.message : isRegister ? A.failedRegister : A.failedLogin);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="auth-root flex min-h-dvh bg-background">
      <BrandPanel />

      {/* 表单区 */}
      <div className="relative flex min-w-0 flex-1 items-center justify-center overflow-x-clip px-5 py-16 sm:px-8">
        {/* 右区淡彩雾（移动端也可见，给单栏留一点氛围） */}
        <div aria-hidden className="pointer-events-none absolute inset-0 overflow-hidden">
          <div className="absolute -top-24 right-[-10%] size-[22rem] rounded-full bg-[var(--glow-b)] blur-[100px] animate-[vane-aurora_19s_ease-in-out_infinite]" />
          <div className="absolute bottom-[-12%] left-[-8%] size-[20rem] rounded-full bg-[var(--glow-c)] blur-[110px] animate-[vane-drift-b_23s_ease-in-out_infinite_reverse]" />
          <div className="absolute inset-0 bg-[linear-gradient(to_right,var(--grid-line)_1px,transparent_1px),linear-gradient(to_bottom,var(--grid-line)_1px,transparent_1px)] bg-[size:56px_56px] opacity-55 [mask-image:radial-gradient(ellipse_at_center,#000_0%,transparent_72%)]" />
        </div>

        <div className="absolute right-4 top-4 rounded-full border border-border/60 bg-background/65 p-0.5 shadow-sm backdrop-blur-xl sm:right-6 sm:top-6">
          <LocaleSwitch />
        </div>

        <div className="relative isolate w-full max-w-[26rem]">
          <div
            aria-hidden
            className="absolute -inset-8 -z-10 rounded-[2.75rem] bg-[radial-gradient(circle_at_50%_18%,var(--glow-b),transparent_68%)] blur-2xl"
          />

          {/* 移动端品牌头（lg 起由左面板承担） */}
          <div className="mb-8 flex flex-col items-center gap-3 lg:hidden">
            <LogoMark className="size-12 rounded-xl shadow-lg" />
            <div className="text-center">
              <h1 className="font-heading text-2xl font-bold tracking-tight text-foreground">
                {t.brandName}
              </h1>
              <p className="mt-0.5 text-sm text-muted-foreground">{A.tagline}</p>
            </div>
          </div>

          <form onSubmit={onSubmit}>
            <Card className="relative gap-0 overflow-hidden rounded-[1.75rem] border border-white/80 bg-card/88 py-0 shadow-[0_28px_80px_-32px_var(--glow-a)] ring-1 ring-foreground/[0.08] backdrop-blur-2xl dark:border-white/10 dark:bg-card/82 dark:ring-white/10">
              <div
                aria-hidden
                className="absolute inset-x-10 top-0 h-px bg-gradient-to-r from-transparent via-brand/60 to-transparent"
              />

              <CardHeader className="relative space-y-2 px-7 pb-6 pt-8 sm:px-8">
                <CardTitle className="font-heading text-2xl font-semibold tracking-tight">
                  {isRegister ? A.register : A.login}
                </CardTitle>
                <CardDescription className="leading-relaxed">
                  {isRegister ? A.registerDesc : A.loginDesc}
                </CardDescription>
              </CardHeader>

              <CardContent className="relative space-y-5 px-7 sm:px-8">
                <div className="space-y-2">
                  <Label htmlFor="email" className="text-[0.8125rem] font-medium">
                    {A.email}
                  </Label>
                  <div className="relative">
                    <Mail className="pointer-events-none absolute left-3.5 top-1/2 z-10 size-4 -translate-y-1/2 text-muted-foreground" />
                    <Input
                      id="email"
                      type="email"
                      autoComplete="email"
                      placeholder="name@example.com"
                      value={email}
                      onChange={(e) => setEmail(e.target.value)}
                      className="auth-input h-11 rounded-xl border-border/80 bg-background/75 pl-10 pr-3 shadow-xs transition-[border-color,box-shadow,background-color] hover:border-foreground/20 focus-visible:border-brand-strong/90 focus-visible:ring-brand/25 dark:bg-background/40 dark:hover:border-foreground/20"
                      autoFocus
                    />
                  </div>
                </div>

                <div className="space-y-2">
                  <Label htmlFor="password" className="text-[0.8125rem] font-medium">
                    {isRegister ? A.passwordNew : A.password}
                  </Label>
                  <div className="relative">
                    <Lock className="pointer-events-none absolute left-3.5 top-1/2 z-10 size-4 -translate-y-1/2 text-muted-foreground" />
                    <Input
                      id="password"
                      type="password"
                      autoComplete={isRegister ? "new-password" : "current-password"}
                      placeholder={isRegister ? A.passwordNewPlaceholder : A.passwordPlaceholder}
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      className="auth-input h-11 rounded-xl border-border/80 bg-background/75 pl-10 pr-3 shadow-xs transition-[border-color,box-shadow,background-color] hover:border-foreground/20 focus-visible:border-brand-strong/90 focus-visible:ring-brand/25 dark:bg-background/40 dark:hover:border-foreground/20"
                    />
                  </div>
                </div>

                {isRegister && (
                  <div className="space-y-2">
                    <Label htmlFor="inviteCode" className="text-[0.8125rem] font-medium">
                      {A.inviteCode}
                    </Label>
                    <div className="relative">
                      <Ticket className="pointer-events-none absolute left-3.5 top-1/2 z-10 size-4 -translate-y-1/2 text-muted-foreground" />
                      <Input
                        id="inviteCode"
                        type="text"
                        placeholder={A.invitePlaceholder}
                        value={inviteCode}
                        onChange={(e) => setInviteCode(e.target.value)}
                        className="auth-input h-11 rounded-xl border-border/80 bg-background/75 pl-10 pr-3 shadow-xs transition-[border-color,box-shadow,background-color] hover:border-foreground/20 focus-visible:border-brand-strong/90 focus-visible:ring-brand/25 dark:bg-background/40 dark:hover:border-foreground/20"
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

              <CardFooter className="relative mt-7 flex flex-col gap-3 border-t-0 bg-transparent px-7 pb-7 pt-0 sm:px-8 sm:pb-8">
                <Button
                  type="submit"
                  size="lg"
                  className="h-11 w-full rounded-xl shadow-lg shadow-primary/10 transition-[transform,box-shadow,background-color] hover:-translate-y-px hover:bg-primary/90 hover:shadow-xl hover:shadow-primary/15"
                  disabled={loading || !canSubmit}
                >
                  {loading ? (
                    <>
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                      {isRegister ? A.submittingRegister : A.submittingLogin}
                    </>
                  ) : isRegister ? (
                    A.submitRegister
                  ) : (
                    A.submitLogin
                  )}
                </Button>
                <Button
                  type="button"
                  variant="link"
                  className="h-auto min-h-8 px-2 py-1 text-sm text-muted-foreground hover:text-foreground hover:no-underline"
                  onClick={() => switchMode(isRegister ? "login" : "register")}
                  disabled={loading}
                >
                  {isRegister ? A.toLogin : A.toRegister}
                </Button>
              </CardFooter>
            </Card>
          </form>
        </div>
      </div>
    </div>
  );
}

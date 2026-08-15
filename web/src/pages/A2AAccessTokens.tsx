import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import {
  Bot,
  Check,
  Clipboard,
  KeyRound,
  Loader2,
  RefreshCw,
  ShieldCheck,
  Trash2,
  UserRound,
} from "lucide-react";
import { toast } from "sonner";

import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useI18n } from "@/i18n";
import {
  ApiError,
  api,
  getA2AEndpoint,
  type A2AAccessToken,
  type A2AActorType,
  type A2AScope,
  type MeResponse,
} from "@/shared/api/client";

const scopeLabels: Record<A2AScope, { zh: string; en: string; detailZh: string; detailEn: string }> = {
  "content.query": {
    zh: "读取工作区情报",
    en: "Read workspace intelligence",
    detailZh: "检索当前工作区可见的内容，不发起 Agent 对话。",
    detailEn: "Query content visible in this workspace without starting an Agent conversation.",
  },
  "assistant.chat": {
    zh: "调用 Agent 对话",
    en: "Use Agent chat",
    detailZh: "允许触发模型调用，可能产生用量和费用。",
    detailEn: "Allows model calls and may incur usage and cost.",
  },
};

function safeDate(value: string, locale: string): string {
  const date = new Date(value);
  return Number.isFinite(date.getTime()) ? date.toLocaleString(locale) : "—";
}

function tokenStatus(item: A2AAccessToken): "active" | "revoked" | "expired" {
  if (item.revoked_at) return "revoked";
  return new Date(item.expires_at).getTime() <= Date.now() ? "expired" : "active";
}

function publicToken(item: A2AAccessToken): A2AAccessToken {
  const { token: _rawToken, ...safe } = item;
  return safe;
}

export default function A2AAccessTokens({ me }: { me: MeResponse }) {
  const { locale } = useI18n();
  const zh = locale === "zh" || locale === "zh-Hant";
  const scopeKey = `${me.tenant_id}:${me.user_id}`;
  const scopeRef = useRef(scopeKey);
  scopeRef.current = scopeKey;
  const canCreateService = me.role === "owner" || me.role === "admin";
  const endpoint = getA2AEndpoint();

  const [tokens, setTokens] = useState<A2AAccessToken[]>([]);
  const [issued, setIssued] = useState<A2AAccessToken | null>(null);
  const [actorType, setActorType] = useState<A2AActorType>("user");
  const [label, setLabel] = useState("");
  const [scopes, setScopes] = useState<A2AScope[]>(["content.query"]);
  const [expiresInDays, setExpiresInDays] = useState(30);
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");

  const activeCount = useMemo(
    () => tokens.filter((item) => tokenStatus(item) === "active").length,
    [tokens],
  );

  useEffect(() => {
    let current = true;
    setTokens([]);
    setIssued(null);
    setActorType("user");
    setLabel("");
    setScopes(["content.query"]);
    setExpiresInDays(30);
    setPassword("");
    setBusy("");
    setError("");
    setLoading(true);
    api.listA2AAccessTokens()
      .then((items) => {
        if (current) setTokens(items.map(publicToken));
      })
      .catch((cause) => {
        if (current) setError(cause instanceof Error ? cause.message : (zh ? "加载凭证失败" : "Failed to load credentials"));
      })
      .finally(() => {
        if (current) setLoading(false);
      });
    return () => {
      current = false;
    };
  }, [scopeKey, zh]);

  function toggleScope(scope: A2AScope) {
    setScopes((current) => current.includes(scope)
      ? current.filter((item) => item !== scope)
      : [...current, scope]);
  }

  async function issueToken(event: FormEvent) {
    event.preventDefault();
    if (busy || scopes.length === 0 || !password) return;
    if (actorType === "service_account" && (!canCreateService || !label.trim())) return;
    const requestScope = scopeRef.current;
    setBusy("issue");
    setError("");
    setIssued(null);
    try {
      const reauth = await api.reauthenticate(password);
      setPassword("");
      if (requestScope !== scopeRef.current) return;
      const item = await api.issueA2AAccessToken({
        actor_type: actorType,
        principal_user_id: me.user_id,
        ...(actorType === "service_account" ? { service_account_label: label.trim() } : {}),
        scopes,
        expires_in_days: expiresInDays,
      }, reauth.proof);
      if (requestScope !== scopeRef.current) return;
      if (!item.token) {
        setTokens((current) => [publicToken(item), ...current]);
        throw new Error(zh
          ? "凭证已创建，但服务器没有返回一次性 Token。请撤销后重试。"
          : "The credential was created without a one-time token. Revoke it and try again.");
      }
      setIssued(item);
      setTokens((current) => [publicToken(item), ...current]);
      setLabel("");
      toast.success(zh ? "凭证已创建；Token 只显示这一次" : "Credential created; the token is shown once");
    } catch (cause) {
      if (requestScope === scopeRef.current) {
        setPassword("");
        setError(cause instanceof ApiError || cause instanceof Error
          ? cause.message
          : (zh ? "创建凭证失败" : "Failed to create credential"));
      }
    } finally {
      if (requestScope === scopeRef.current) setBusy("");
    }
  }

  async function revoke(item: A2AAccessToken) {
    if (busy || tokenStatus(item) !== "active") return;
    const name = item.service_account_label || (zh ? "个人凭证" : "Personal credential");
    if (!confirm(zh ? `立即撤销“${name}”？使用它的客户端会立刻失效。` : `Revoke “${name}” now? Clients using it will immediately lose access.`)) return;
    const requestScope = scopeRef.current;
    setBusy(`revoke:${item.id}`);
    setError("");
    try {
      await api.revokeA2AAccessToken(item.id);
      if (requestScope !== scopeRef.current) return;
      const revokedAt = new Date().toISOString();
      setTokens((current) => current.map((token) => token.id === item.id
        ? { ...token, revoked_at: revokedAt }
        : token));
      if (issued?.id === item.id) setIssued(null);
      toast.success(zh ? "凭证已撤销" : "Credential revoked");
    } catch (cause) {
      if (requestScope === scopeRef.current) {
        setError(cause instanceof Error ? cause.message : (zh ? "撤销失败" : "Revocation failed"));
      }
    } finally {
      if (requestScope === scopeRef.current) setBusy("");
    }
  }

  async function copyIssuedToken() {
    if (!issued?.token) return;
    try {
      await navigator.clipboard.writeText(issued.token);
      toast.success(zh ? "Token 已复制" : "Token copied");
    } catch {
      toast.error(zh ? "复制失败，请手动复制" : "Copy failed; copy it manually");
    }
  }

  return (
    <div className="space-y-5">
      <div className="grid gap-4 lg:grid-cols-[minmax(0,1.15fr)_minmax(18rem,.85fr)]">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2"><KeyRound className="size-4" />{zh ? "创建访问凭证" : "Create access credential"}</CardTitle>
            <CardDescription>
              {zh ? "用于从其他 Agent 或自动化安全访问当前工作区。凭证不能跨工作区使用。" : "Use another Agent or automation to access this workspace securely. Credentials never cross workspaces."}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <form className="space-y-5" onSubmit={issueToken}>
              <fieldset className="space-y-2">
                <legend className="text-sm font-medium">{zh ? "使用方式" : "Identity"}</legend>
                <div className="grid gap-2 sm:grid-cols-2">
                  <button type="button" onClick={() => setActorType("user")}
                    className={`rounded-xl border p-3 text-left transition-colors ${actorType === "user" ? "border-primary bg-primary/5" : "border-border hover:bg-muted/50"}`}>
                    <span className="flex items-center gap-2 font-medium"><UserRound className="size-4" />{zh ? "以我的身份" : "As me"}</span>
                    <span className="mt-1 block text-xs text-muted-foreground">{zh ? "权限随你在当前工作区的实时角色变化。" : "Follows your live role in this workspace."}</span>
                  </button>
                  {canCreateService && (
                    <button type="button" onClick={() => setActorType("service_account")}
                      className={`rounded-xl border p-3 text-left transition-colors ${actorType === "service_account" ? "border-primary bg-primary/5" : "border-border hover:bg-muted/50"}`}>
                      <span className="flex items-center gap-2 font-medium"><Bot className="size-4" />{zh ? "自动化身份" : "Automation identity"}</span>
                      <span className="mt-1 block text-xs text-muted-foreground">{zh ? "运行权限固定为 Member，不能代替管理员。" : "Runtime role is fixed to Member and cannot impersonate an admin."}</span>
                    </button>
                  )}
                </div>
              </fieldset>

              {actorType === "service_account" && canCreateService && (
                <div className="space-y-2">
                  <Label htmlFor="a2a-label">{zh ? "自动化名称" : "Automation name"}</Label>
                  <Input id="a2a-label" maxLength={128} required autoComplete="off"
                    placeholder={zh ? "例如：竞品日报机器人" : "e.g. Competitor brief bot"}
                    value={label} onChange={(event) => setLabel(event.target.value)} />
                </div>
              )}

              <fieldset className="space-y-2">
                <legend className="text-sm font-medium">{zh ? "允许做什么" : "Allowed actions"}</legend>
                {(Object.keys(scopeLabels) as A2AScope[]).map((scope) => {
                  const checked = scopes.includes(scope);
                  const labels = scopeLabels[scope];
                  return (
                    <label key={scope} className="flex cursor-pointer gap-3 rounded-xl border p-3 hover:bg-muted/40">
                      <input type="checkbox" className="mt-0.5 size-4 accent-primary" checked={checked}
                        onChange={() => toggleScope(scope)} />
                      <span>
                        <span className="block font-medium">{zh ? labels.zh : labels.en}</span>
                        <span className="mt-0.5 block text-xs text-muted-foreground">{zh ? labels.detailZh : labels.detailEn}</span>
                      </span>
                    </label>
                  );
                })}
                {scopes.length === 0 && <p className="text-xs text-destructive">{zh ? "至少选择一项权限" : "Select at least one permission"}</p>}
              </fieldset>

              <div className="grid gap-4 sm:grid-cols-2">
                <div className="space-y-2">
                  <Label htmlFor="a2a-expiry">{zh ? "有效期" : "Expires in"}</Label>
                  <select id="a2a-expiry" className="h-8 w-full rounded-lg border border-input bg-background px-2.5 text-sm"
                    value={expiresInDays} onChange={(event) => setExpiresInDays(Number(event.target.value))}>
                    <option value={7}>{zh ? "7 天" : "7 days"}</option>
                    <option value={30}>{zh ? "30 天" : "30 days"}</option>
                    <option value={90}>{zh ? "90 天" : "90 days"}</option>
                  </select>
                </div>
                <div className="space-y-2">
                  <Label htmlFor="a2a-password">{zh ? "确认你的登录密码" : "Confirm your password"}</Label>
                  <Input id="a2a-password" type="password" autoComplete="current-password" required
                    value={password} onChange={(event) => setPassword(event.target.value)} />
                </div>
              </div>

              <Button type="submit" disabled={Boolean(busy) || scopes.length === 0 || !password || (actorType === "service_account" && !label.trim())}>
                {busy === "issue" ? <Loader2 className="animate-spin" /> : <ShieldCheck />}
                {zh ? "验证并创建" : "Verify and create"}
              </Button>
            </form>
          </CardContent>
        </Card>

        <div className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>{zh ? "安全边界" : "Security boundary"}</CardTitle>
              <CardDescription>{zh ? "Vane 只保存 Token 的不可逆哈希。" : "Vane stores only an irreversible hash of the token."}</CardDescription>
            </CardHeader>
            <CardContent className="space-y-3 text-sm text-muted-foreground">
              <p className="flex gap-2"><Check className="mt-0.5 size-4 shrink-0 text-emerald-600" />{zh ? "删除成员、切换角色或停用工作区会立即改变访问权。" : "Membership removal, role changes, and workspace suspension take effect immediately."}</p>
              <p className="flex gap-2"><Check className="mt-0.5 size-4 shrink-0 text-emerald-600" />{zh ? "原始 Token 只显示一次，刷新页面后无法找回。" : "The raw token is shown once and cannot be recovered after refresh."}</p>
              <p className="flex gap-2"><Check className="mt-0.5 size-4 shrink-0 text-emerald-600" />{zh ? "每枚凭证独立撤销，不影响你的网页登录。" : "Each credential can be revoked without affecting your web session."}</p>
              <div className="rounded-lg bg-muted/60 p-3">
                <p className="mb-1 text-xs font-medium text-foreground">A2A endpoint</p>
                <code className="break-all text-xs">{endpoint}</code>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>{zh ? "当前状态" : "Current status"}</CardTitle>
              <CardDescription>{zh ? `${activeCount} 枚有效凭证，最多 20 枚。` : `${activeCount} active credentials, up to 20.`}</CardDescription>
            </CardHeader>
          </Card>
        </div>
      </div>

      {issued?.token && (
        <Alert className="border-amber-500/40 bg-amber-500/5">
          <KeyRound className="size-4 text-amber-600" />
          <AlertDescription className="space-y-3">
            <div>
              <p className="font-medium text-foreground">{zh ? "现在保存 Token；关闭或切换工作区后无法再次查看" : "Save this token now; it cannot be viewed again after closing or switching workspaces"}</p>
              <p className="mt-1 text-xs">{zh ? `将它作为 Authorization: Bearer <token> 发送到 ${endpoint}。不要粘贴到聊天、任务描述或日志中。` : `Send it as Authorization: Bearer <token> to ${endpoint}. Never paste it into chats, task descriptions, or logs.`}</p>
            </div>
            <div className="flex items-start gap-2">
              <code data-testid="one-time-a2a-token" className="min-w-0 flex-1 break-all rounded-lg bg-background p-3 font-mono text-xs text-foreground ring-1 ring-border">{issued.token}</code>
              <Button type="button" variant="outline" size="icon" aria-label={zh ? "复制 Token" : "Copy token"} onClick={copyIssuedToken}><Clipboard /></Button>
              <Button type="button" variant="ghost" onClick={() => setIssued(null)}>{zh ? "我已保存" : "I've saved it"}</Button>
            </div>
          </AlertDescription>
        </Alert>
      )}

      {error && <Alert variant="destructive"><AlertDescription>{error}</AlertDescription></Alert>}

      <Card>
        <CardHeader>
          <CardTitle>{zh ? "已有凭证" : "Existing credentials"}</CardTitle>
          <CardDescription>{zh ? "列表不会返回原始 Token，只显示身份、权限和生命周期。" : "The list never returns raw tokens, only identity, permissions, and lifecycle."}</CardDescription>
          <CardAction>
            <Button type="button" variant="ghost" size="sm" disabled={loading || Boolean(busy)} onClick={() => {
              const requestScope = scopeRef.current;
              setLoading(true);
              setError("");
              api.listA2AAccessTokens().then((items) => {
                if (requestScope === scopeRef.current) setTokens(items.map(publicToken));
              }).catch((cause) => {
                if (requestScope === scopeRef.current) setError(cause instanceof Error ? cause.message : (zh ? "刷新失败" : "Refresh failed"));
              }).finally(() => {
                if (requestScope === scopeRef.current) setLoading(false);
              });
            }}><RefreshCw className={loading ? "animate-spin" : ""} />{zh ? "刷新" : "Refresh"}</Button>
          </CardAction>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="flex min-h-24 items-center justify-center text-muted-foreground"><Loader2 className="mr-2 size-4 animate-spin" />{zh ? "正在加载" : "Loading"}</div>
          ) : tokens.length === 0 ? (
            <div className="rounded-xl border border-dashed p-8 text-center text-sm text-muted-foreground">{zh ? "这个工作区还没有访问凭证。" : "This workspace has no access credentials yet."}</div>
          ) : (
            <div className="divide-y rounded-xl border">
              {tokens.map((item) => {
                const status = tokenStatus(item);
                return (
                  <div key={item.id} className="flex flex-col gap-3 p-4 sm:flex-row sm:items-center sm:justify-between">
                    <div className="min-w-0 space-y-1.5">
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="flex items-center gap-1.5 font-medium">{item.actor_type === "service_account" ? <Bot className="size-4" /> : <UserRound className="size-4" />}{item.service_account_label || (zh ? "我的身份" : "My identity")}</span>
                        <Badge variant={status === "active" ? "secondary" : "outline"}>{status === "active" ? (zh ? "有效" : "Active") : status === "revoked" ? (zh ? "已撤销" : "Revoked") : (zh ? "已过期" : "Expired")}</Badge>
                      </div>
                      <div className="flex flex-wrap gap-1.5">{item.scopes.map((scope) => <Badge key={scope} variant="outline">{zh ? scopeLabels[scope].zh : scopeLabels[scope].en}</Badge>)}</div>
                      <p className="text-xs text-muted-foreground">{zh ? "创建" : "Created"} {safeDate(item.created_at, locale)} · {zh ? "到期" : "Expires"} {safeDate(item.expires_at, locale)}</p>
                    </div>
                    {status === "active" && <Button type="button" variant="destructive" size="sm" disabled={Boolean(busy)} onClick={() => revoke(item)}>{busy === `revoke:${item.id}` ? <Loader2 className="animate-spin" /> : <Trash2 />}{zh ? "撤销" : "Revoke"}</Button>}
                  </div>
                );
              })}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

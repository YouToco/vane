import { useEffect, useState } from "react";
import { KeyRound, Loader2, RotateCcw, Trash2 } from "lucide-react";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { api, ApiError } from "@/shared/api/client";
import type { CredentialStatus, PlatformProviderCredential } from "@/shared/api/client";

const providers: Array<{
  id: PlatformProviderCredential;
  name: string;
  purpose: string;
}> = [
  { id: "exa", name: "Exa", purpose: "网页检索、页面读取与 Research V3" },
  { id: "tikhub", name: "TikHub", purpose: "X、小红书、微博与公众号信息获取" },
];

type ProviderState = {
  status: CredentialStatus | null;
  apiKey: string;
  busy: "load" | "save" | "revoke" | "";
  message: string;
  error: string;
};

const initialState = (): ProviderState => ({
  status: null,
  apiKey: "",
  busy: "load",
  message: "",
  error: "",
});

export default function ProviderCredentials() {
  const [states, setStates] = useState<Record<PlatformProviderCredential, ProviderState>>({
    exa: initialState(),
    tikhub: initialState(),
  });

  function patch(provider: PlatformProviderCredential, update: Partial<ProviderState>) {
    setStates((current) => ({
      ...current,
      [provider]: { ...current[provider], ...update },
    }));
  }

  async function refresh(provider: PlatformProviderCredential) {
    try {
      const status = await api.adminProviderCredentialStatus(provider);
      patch(provider, { status, busy: "", error: "" });
    } catch (cause) {
      patch(provider, {
        busy: "",
        error: cause instanceof ApiError ? cause.message : "读取供应商凭证状态失败",
      });
    }
  }

  useEffect(() => {
    for (const provider of providers) void refresh(provider.id);
  }, []);

  async function save(provider: PlatformProviderCredential) {
    const state = states[provider];
    patch(provider, { busy: "save", message: "", error: "" });
    try {
      const status = await api.adminRotateProviderCredential(provider, {
        api_key: state.apiKey,
      });
      patch(provider, {
        status,
        apiKey: "",
        busy: "",
        message: `第 ${status.generation} 代凭证已加密保存；安全重启后成为新任务的运行权威，旧代继续服务已冻结任务。`,
      });
    } catch (cause) {
      patch(provider, {
        busy: "",
        error: cause instanceof ApiError ? cause.message : "保存供应商凭证失败",
      });
    }
  }

  async function revoke(provider: PlatformProviderCredential) {
    if (!window.confirm("撤销后不会回退到 VPS 环境变量，安全重启将因缺少有效凭证而拒绝相关运行。确定继续？")) return;
    patch(provider, { busy: "revoke", message: "", error: "" });
    try {
      await api.adminRevokeProviderCredential(provider);
      await refresh(provider);
      patch(provider, { message: "撤销已记录；当前进程只收敛已接纳工作，不会复活旧环境变量。" });
    } catch (cause) {
      patch(provider, {
        busy: "",
        error: cause instanceof ApiError ? cause.message : "撤销供应商凭证失败",
      });
    }
  }

  return (
    <Card>
      <CardHeader className="space-y-2">
        <CardTitle className="flex items-center gap-2 text-base">
          <KeyRound className="size-4" />信息获取服务密钥
        </CardTitle>
        <p className="text-sm text-muted-foreground">
          仅平台超级管理员可见。密钥加密、分代保存且永不回显；任务运行冻结精确代际，轮换不会把旧任务悄悄切到新 Key。
        </p>
      </CardHeader>
      <CardContent className="grid gap-4 lg:grid-cols-2">
        {providers.map((provider) => {
          const state = states[provider.id];
          return (
            <div key={provider.id} className="space-y-4 rounded-xl border p-4">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <h3 className="font-medium">{provider.name}</h3>
                  <p className="text-xs text-muted-foreground">{provider.purpose}</p>
                </div>
                <Badge variant={state.status?.configured ? "default" : "secondary"}>
                  {state.status?.configured ? `第 ${state.status.generation} 代` : "未存入数据库"}
                </Badge>
              </div>
              {state.status && !state.status.vault_ready && (
                <Alert variant="destructive"><AlertDescription>部署侧凭证库主密钥未配置，无法保存。</AlertDescription></Alert>
              )}
              <div className="space-y-2">
                <Label htmlFor={`${provider.id}-api-key`}>{provider.name} 新 API Key</Label>
                <Input
                  id={`${provider.id}-api-key`}
                  type="password"
                  autoComplete="new-password"
                  value={state.apiKey}
                  onChange={(event) => patch(provider.id, { apiKey: event.target.value })}
                  placeholder="每次轮换必须重新输入；不会回显旧值"
                />
              </div>
              {state.status?.fingerprint && (
                <p className="text-xs text-muted-foreground">当前指纹：<code>{state.status.fingerprint.slice(0, 16)}…</code></p>
              )}
              {state.error && <Alert variant="destructive"><AlertDescription>{state.error}</AlertDescription></Alert>}
              {state.message && <Alert><AlertDescription>{state.message}</AlertDescription></Alert>}
              <div className="flex flex-wrap gap-2">
                <Button
                  onClick={() => void save(provider.id)}
                  disabled={state.busy !== "" || !state.apiKey || state.status?.vault_ready === false}
                >
                  {state.busy === "save" ? <Loader2 className="mr-2 size-4 animate-spin" /> : <RotateCcw className="mr-2 size-4" />}
                  保存并轮换
                </Button>
                {state.status?.configured && (
                  <Button variant="destructive" onClick={() => void revoke(provider.id)} disabled={state.busy !== ""}>
                    {state.busy === "revoke" ? <Loader2 className="mr-2 size-4 animate-spin" /> : <Trash2 className="mr-2 size-4" />}
                    撤销
                  </Button>
                )}
              </div>
            </div>
          );
        })}
      </CardContent>
    </Card>
  );
}

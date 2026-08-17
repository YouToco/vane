import { useEffect, useState } from "react";
import { KeyRound, Loader2, RotateCcw, ShieldCheck, Trash2 } from "lucide-react";
import { api, ApiError } from "@/shared/api/client";
import type { CredentialStatus, FetchCredentialInput } from "@/shared/api/client";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

const emptyForm: FetchCredentialInput = { exa_api_key: "", tikhub_api_key: "" };

export default function FetchCredentials() {
  const [status, setStatus] = useState<CredentialStatus | null>(null);
  const [form, setForm] = useState<FetchCredentialInput>(emptyForm);
  const [busy, setBusy] = useState<"load" | "save" | "revoke" | "">("load");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  async function refresh() {
    setStatus(await api.adminFetchCredentialStatus());
  }

  useEffect(() => {
    refresh()
      .catch((cause) => setError(cause instanceof ApiError ? cause.message : "读取信息服务凭证状态失败"))
      .finally(() => setBusy(""));
  }, []);

  async function save() {
    setBusy("save");
    setError("");
    setMessage("");
    try {
      const next = await api.adminRotateFetchCredential(form);
      setStatus(next);
      setForm(emptyForm);
      setMessage(`第 ${next.generation} 代信息服务凭证已加密保存，将在 Vane 服务安全重启后切换。`);
    } catch (cause) {
      setError(cause instanceof ApiError ? cause.message : "保存信息服务凭证失败");
    } finally {
      setBusy("");
    }
  }

  async function revoke() {
    if (!window.confirm("撤销后，下次启动不会回退 VPS 环境变量。确定继续？")) return;
    setBusy("revoke");
    setError("");
    setMessage("");
    try {
      await api.adminRevokeFetchCredential();
      await refresh();
      setMessage("撤销已记录：当前进程继续收敛在途任务；安全重启后将 fail-closed。历史版本只保留审计。");
    } catch (cause) {
      setError(cause instanceof ApiError ? cause.message : "撤销信息服务凭证失败");
    } finally {
      setBusy("");
    }
  }

  return (
    <Card>
      <CardHeader className="space-y-2">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <CardTitle className="flex items-center gap-2 text-base">
            <KeyRound className="size-4" />信息获取服务密钥
          </CardTitle>
          <Badge variant={status?.configured ? "default" : "secondary"}>
            {status?.configured ? `第 ${status.generation} 代` : "未存入数据库"}
          </Badge>
        </div>
        <p className="text-sm text-muted-foreground">
          仅平台超级管理员可修改。Exa 和 TikHub Key 作为同一运行代次原子轮换，页面不会读取或回显原值。
        </p>
      </CardHeader>
      <CardContent className="space-y-4">
        {busy === "load" && <p className="flex items-center gap-2 text-sm"><Loader2 className="size-4 animate-spin" />读取配置</p>}
        {status && !status.vault_ready && (
          <Alert variant="destructive"><AlertDescription>部署侧尚未配置凭证库主密钥，保存操作会被服务端拒绝。</AlertDescription></Alert>
        )}
        <Alert>
          <ShieldCheck className="size-4" />
          <AlertDescription>
            数据库只保存 AES-GCM 密文、版本和审计信息。首次存入数据库后，撤销或密文损坏都不会复活旧环境变量。
          </AlertDescription>
        </Alert>
        <div className="grid gap-4 md:grid-cols-2">
          <div className="space-y-2">
            <Label htmlFor="fetch-exa-api-key">Exa 新 API Key</Label>
            <Input id="fetch-exa-api-key" type="password" autoComplete="new-password"
              value={form.exa_api_key}
              onChange={(event) => setForm((current) => ({ ...current, exa_api_key: event.target.value }))}
              placeholder="每次轮换必须重新输入" />
          </div>
          <div className="space-y-2">
            <Label htmlFor="fetch-tikhub-api-key">TikHub 新 API Key</Label>
            <Input id="fetch-tikhub-api-key" type="password" autoComplete="new-password"
              value={form.tikhub_api_key}
              onChange={(event) => setForm((current) => ({ ...current, tikhub_api_key: event.target.value }))}
              placeholder="每次轮换必须重新输入" />
          </div>
        </div>
        {status?.fingerprint && <p className="text-xs text-muted-foreground">当前指纹：<code>{status.fingerprint.slice(0, 16)}…</code></p>}
        {error && <Alert variant="destructive"><AlertDescription>{error}</AlertDescription></Alert>}
        {message && <Alert><AlertDescription>{message}</AlertDescription></Alert>}
        <div className="flex flex-wrap gap-2">
          <Button onClick={() => void save()}
            disabled={busy !== "" || !form.exa_api_key || !form.tikhub_api_key || status?.vault_ready === false}>
            {busy === "save" ? <Loader2 className="mr-2 size-4 animate-spin" /> : <RotateCcw className="mr-2 size-4" />}
            保存并轮换
          </Button>
          {status?.configured && <Button variant="destructive" onClick={() => void revoke()} disabled={busy !== ""}>
            {busy === "revoke" ? <Loader2 className="mr-2 size-4 animate-spin" /> : <Trash2 className="mr-2 size-4" />}
            撤销当前版本
          </Button>}
        </div>
      </CardContent>
    </Card>
  );
}

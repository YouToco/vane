import { useEffect, useState } from "react";
import { KeyRound, Loader2, RotateCcw, ShieldCheck, Trash2 } from "lucide-react";
import { api, ApiError } from "@/shared/api/client";
import type { CredentialStatus, LLMCredentialInput } from "@/shared/api/client";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

const emptyForm: LLMCredentialInput = {
  provider: "deepseek",
  base_url: "https://api.deepseek.com",
  api_key: "",
  model: "deepseek-chat",
  agent_provider: "",
  agent_base_url: "",
  agent_api_key: "",
  agent_model: "deepseek-chat",
  research_model: "deepseek-chat",
  max_concurrent: 4,
};

export default function LLMCredentials() {
  const [status, setStatus] = useState<CredentialStatus | null>(null);
  const [form, setForm] = useState<LLMCredentialInput>(emptyForm);
  const [busy, setBusy] = useState<"load" | "save" | "revoke" | "">("load");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  async function refresh() {
    const next = await api.adminLLMCredentialStatus();
    setStatus(next);
    const metadata = next.metadata ?? {};
    setForm((current) => ({
      ...current,
      provider: "deepseek",
      base_url: typeof metadata.base_url === "string" ? metadata.base_url : current.base_url,
      model: typeof metadata.model === "string" ? metadata.model : current.model,
      agent_provider:
        metadata.agent_provider === "deepseek" || metadata.agent_provider === "kimi"
          ? metadata.agent_provider
          : "",
      agent_base_url:
        typeof metadata.agent_base_url === "string" ? metadata.agent_base_url : "",
      agent_model: typeof metadata.agent_model === "string" ? metadata.agent_model : current.agent_model,
      research_model: typeof metadata.research_model === "string" ? metadata.research_model : current.research_model,
      max_concurrent: typeof metadata.max_concurrent === "number" ? metadata.max_concurrent : current.max_concurrent,
      api_key: "",
      agent_api_key: "",
    }));
  }

  useEffect(() => {
    refresh()
      .catch((cause) => setError(cause instanceof ApiError ? cause.message : "读取 LLM 凭证状态失败"))
      .finally(() => setBusy(""));
  }, []);

  async function save() {
    setBusy("save");
    setError("");
    setMessage("");
    try {
      const next = await api.adminRotateLLMCredential(form);
      setStatus(next);
      setForm((current) => ({ ...current, api_key: "", agent_api_key: "" }));
      setMessage(`第 ${next.generation} 代共享 LLM 凭证已加密保存。为保护在途任务，本阶段将在 Vane 服务安全重启后切换。`);
    } catch (cause) {
      setError(cause instanceof ApiError ? cause.message : "保存 LLM 凭证失败");
    } finally {
      setBusy("");
    }
  }

  async function revoke() {
    if (!window.confirm("撤销后，数据库 LLM 凭证将在下次启动时不可用。确定继续？")) return;
    setBusy("revoke");
    setError("");
    setMessage("");
    try {
      await api.adminRevokeLLMCredential();
      await refresh();
      setMessage("撤销已记录：当前进程会继续收敛在途任务；安全重启后不会回退到旧环境变量，且会因无活跃凭证拒绝启动。历史版本仅保留审计。");
    } catch (cause) {
      setError(cause instanceof ApiError ? cause.message : "撤销 LLM 凭证失败");
    } finally {
      setBusy("");
    }
  }

  const update = <K extends keyof LLMCredentialInput>(key: K, value: LLMCredentialInput[K]) =>
    setForm((current) => ({ ...current, [key]: value }));

  function selectAgentProvider(provider: LLMCredentialInput["agent_provider"]) {
    setForm((current) => ({
      ...current,
      agent_provider: provider,
      agent_base_url:
        provider === "kimi"
          ? "https://api.moonshot.cn/v1"
          : provider === "deepseek"
            ? "https://api.deepseek.com"
            : "",
      agent_api_key: "",
    }));
  }

  return (
    <Card>
      <CardHeader className="space-y-2">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <CardTitle className="flex items-center gap-2 text-base">
            <KeyRound className="size-4" />共享模型与 API 密钥
          </CardTitle>
          <Badge variant={status?.configured ? "default" : "secondary"}>
            {status?.configured ? `第 ${status.generation} 代` : "未存入数据库"}
          </Badge>
        </div>
        <p className="text-sm text-muted-foreground">
          仅平台超级管理员可见、可修改。模型路由和 API Key 由 Vane 内置管理；API Key 以 AES-GCM 密文进入数据库，页面不会读取或回显原值。
        </p>
      </CardHeader>
      <CardContent className="space-y-4">
        {busy === "load" && <p className="flex items-center gap-2 text-sm"><Loader2 className="size-4 animate-spin" />读取配置</p>}
        {status && !status.vault_ready && (
          <Alert variant="destructive"><AlertDescription>部署侧尚未配置凭证库主密钥，保存操作会被服务端拒绝。</AlertDescription></Alert>
        )}
        <Alert>
          <ShieldCheck className="size-4" />
          <AlertDescription>数据库只保存密文、版本、指纹和非敏感路由。主密钥仍由 systemd 部署凭证持有。</AlertDescription>
        </Alert>
        <Alert>
          <AlertDescription>
            当前流水线与 Research V3 使用 DeepSeek 的已冻结兼容协议；Agent 可继承同一连接，也可单独选择 DeepSeek 或 Kimi。图片、音频和视频能力仍为保留状态，未声明原生支持时不会通过转写、OCR 或抽帧绕行。
          </AlertDescription>
        </Alert>
        <div className="grid gap-4 md:grid-cols-2">
          <div className="space-y-2"><Label htmlFor="llm-provider">流水线 / Research Provider</Label><Input id="llm-provider" value="DeepSeek" disabled /></div>
          <div className="space-y-2"><Label htmlFor="llm-base-url">官方 API 地址</Label><Input id="llm-base-url" value={form.base_url} disabled /></div>
          <div className="space-y-2 md:col-span-2"><Label htmlFor="llm-api-key">DeepSeek 新 API Key</Label><Input id="llm-api-key" type="password" autoComplete="new-password" value={form.api_key} onChange={(e) => update("api_key", e.target.value)} placeholder="每次轮换必须重新输入；不会回显旧值" /></div>
          <div className="space-y-2"><Label htmlFor="llm-model">流水线模型</Label><Input id="llm-model" value={form.model} onChange={(e) => update("model", e.target.value)} /></div>
          <div className="space-y-2"><Label htmlFor="llm-research-model">研究模型</Label><Input id="llm-research-model" value={form.research_model} onChange={(e) => update("research_model", e.target.value)} /></div>
          <div className="space-y-2">
            <Label htmlFor="llm-agent-provider">Agent Provider</Label>
            <select
              id="llm-agent-provider"
              className="h-10 w-full rounded-md border bg-background px-3 text-sm"
              value={form.agent_provider}
              onChange={(event) => selectAgentProvider(event.target.value as LLMCredentialInput["agent_provider"])}
            >
              <option value="">继承 DeepSeek 主连接</option>
              <option value="deepseek">独立 DeepSeek Key</option>
              <option value="kimi">Kimi / Moonshot</option>
            </select>
          </div>
          <div className="space-y-2"><Label htmlFor="llm-agent-model">Agent 模型</Label><Input id="llm-agent-model" value={form.agent_model} onChange={(e) => update("agent_model", e.target.value)} /></div>
          {form.agent_provider && (
            <>
              <div className="space-y-2"><Label htmlFor="llm-agent-base-url">Agent 官方 API 地址</Label><Input id="llm-agent-base-url" value={form.agent_base_url} disabled /></div>
              <div className="space-y-2"><Label htmlFor="llm-agent-api-key">Agent 新 API Key</Label><Input id="llm-agent-api-key" type="password" autoComplete="new-password" value={form.agent_api_key} onChange={(e) => update("agent_api_key", e.target.value)} placeholder="独立连接每次轮换都必须重新输入" /></div>
            </>
          )}
          <div className="space-y-2"><Label htmlFor="llm-max-concurrent">最大并发</Label><Input id="llm-max-concurrent" type="number" min={1} max={128} value={form.max_concurrent} onChange={(e) => update("max_concurrent", Number(e.target.value))} /></div>
        </div>
        {status?.fingerprint && <p className="text-xs text-muted-foreground">当前指纹：<code>{status.fingerprint.slice(0, 16)}…</code></p>}
        {error && <Alert variant="destructive"><AlertDescription>{error}</AlertDescription></Alert>}
        {message && <Alert><AlertDescription>{message}</AlertDescription></Alert>}
        <div className="flex flex-wrap gap-2">
          <Button onClick={() => void save()} disabled={busy !== "" || !form.api_key || (form.agent_provider !== "" && !form.agent_api_key) || status?.vault_ready === false}>
            {busy === "save" ? <Loader2 className="mr-2 size-4 animate-spin" /> : <RotateCcw className="mr-2 size-4" />}保存并轮换
          </Button>
          {status?.configured && <Button variant="destructive" onClick={() => void revoke()} disabled={busy !== ""}>
            {busy === "revoke" ? <Loader2 className="mr-2 size-4 animate-spin" /> : <Trash2 className="mr-2 size-4" />}撤销当前版本
          </Button>}
        </div>
      </CardContent>
    </Card>
  );
}

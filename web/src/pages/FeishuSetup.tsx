import { useEffect, useMemo, useState } from "react";
import { api, ApiError } from "@/shared/api/client";
import type { FeishuStatus, VerifyResult } from "@/shared/api/client";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import {
  Collapsible,
  CollapsibleTrigger,
  CollapsibleContent,
} from "@/components/ui/collapsible";
import {
  Check,
  Copy,
  Loader2,
  ChevronDown,
  ChevronRight,
  ExternalLink,
  Shield,
  Key,
  Send,
  Radio,
  AlertTriangle,
  CheckCircle2,
  Info,
} from "lucide-react";

const STORAGE_KEY = "vane_feishu_setup_done";

type ManualDone = { s1: boolean; s2: boolean; s4: boolean; s5test: boolean };

function loadManualDone(): ManualDone {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) return { s1: false, s2: false, s4: false, s5test: false, ...JSON.parse(raw) };
  } catch {}
  return { s1: false, s2: false, s4: false, s5test: false };
}

const SCOPES: { scope: string; desc: string }[] = [
  { scope: "im:message.p2p_msg:readonly", desc: "接收单聊消息" },
  { scope: "im:message.group_at_msg:readonly", desc: "接收群 @ 消息" },
  { scope: "im:message:send_as_bot", desc: "以应用身份发消息（含卡片）" },
  { scope: "im:message:readonly", desc: "获取单聊/群组消息（引用消息等）" },
  { scope: "im:message", desc: "获取与发送消息（已读状态等）" },
  { scope: "im:message.reactions:read", desc: "查看消息表情回复" },
  { scope: "im:message.reactions:write_only", desc: "发送/删除表情回复" },
  { scope: "im:chat.access_event.bot_p2p_chat:read", desc: "订阅机器人会话事件" },
];

const EVENTS: { event: string; desc: string }[] = [
  { event: "im.message.receive_v1", desc: "接收消息" },
  { event: "im.message.message_read_v1", desc: "消息已读" },
  { event: "im.message.reaction.created_v1", desc: "消息被 Reaction" },
  { event: "im.message.reaction.deleted_v1", desc: "消息被取消 Reaction" },
  { event: "im.message.recalled_v1", desc: "消息撤回" },
  { event: "im.chat.access_event.bot_p2p_chat_entered_v1", desc: "用户进入机器人会话" },
];

function buildPermissionUrl(appId: string): string {
  const scopes = SCOPES.map((s) => s.scope).join(",");
  return `https://open.feishu.cn/app/${appId}/auth?q=${encodeURIComponent(scopes)}&token_type=tenant&op_from=openapi`;
}

function copyText(text: string): Promise<void> {
  if (navigator.clipboard?.writeText) {
    return navigator.clipboard.writeText(text);
  }
  return new Promise((resolve, reject) => {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    try {
      document.execCommand("copy") ? resolve() : reject(new Error("copy failed"));
    } finally {
      document.body.removeChild(ta);
    }
  });
}

function CopyRow({ text, desc }: { text: string; desc: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <div className="flex items-center gap-2 py-1.5">
      <code className="flex-1 rounded bg-muted px-2 py-1 text-xs font-mono break-all">
        {text}
      </code>
      <span className="text-xs text-muted-foreground shrink-0 hidden sm:inline">
        {desc}
      </span>
      <Button
        type="button"
        variant={copied ? "secondary" : "ghost"}
        size="sm"
        className="shrink-0 h-7 text-xs"
        onClick={() => {
          copyText(text)
            .then(() => {
              setCopied(true);
              setTimeout(() => setCopied(false), 1500);
            })
            .catch(() => {});
        }}
      >
        {copied ? (
          <>
            <Check className="size-3 mr-1" />
            已复制
          </>
        ) : (
          <>
            <Copy className="size-3 mr-1" />
            复制
          </>
        )}
      </Button>
    </div>
  );
}

const STEP_ICONS = [Shield, Key, Key, Radio, Send] as const;

function StepCard({
  index,
  title,
  done,
  open,
  onToggle,
  children,
}: {
  index: number;
  title: string;
  done: boolean;
  open: boolean;
  onToggle: () => void;
  children: React.ReactNode;
}) {
  const Icon = STEP_ICONS[index - 1];
  return (
    <Card className={done ? "border-emerald-200 dark:border-emerald-800" : ""}>
      <button
        type="button"
        className="flex w-full items-center gap-3 p-4 text-left hover:bg-muted/50 transition-colors rounded-t-lg"
        onClick={onToggle}
      >
        <div
          className={`flex size-8 shrink-0 items-center justify-center rounded-full text-sm font-semibold ${
            done
              ? "bg-emerald-100 text-emerald-700 dark:bg-emerald-900 dark:text-emerald-300"
              : "bg-primary/10 text-primary"
          }`}
        >
          {done ? <Check className="size-4" /> : index}
        </div>
        <div className="flex-1 flex items-center gap-2">
          <Icon className="size-4 text-muted-foreground" />
          <span className="font-medium">{title}</span>
        </div>
        {done && <Badge variant="secondary" className="text-emerald-700 dark:text-emerald-300">已完成</Badge>}
        {open ? (
          <ChevronDown className="size-4 text-muted-foreground" />
        ) : (
          <ChevronRight className="size-4 text-muted-foreground" />
        )}
      </button>
      {open && <CardContent className="pt-0 pb-4 space-y-4">{children}</CardContent>}
    </Card>
  );
}

export default function FeishuSetup() {
  const [status, setStatus] = useState<FeishuStatus | null>(null);
  const [manual, setManual] = useState<ManualDone>(loadManualDone);

  const [appId, setAppId] = useState("");
  const [appSecret, setAppSecret] = useState("");
  const [verifying, setVerifying] = useState(false);
  const [verifyResult, setVerifyResult] = useState<VerifyResult | null>(null);
  const [saving, setSaving] = useState(false);
  const [connecting, setConnecting] = useState(false);
  const [step3Error, setStep3Error] = useState("");

  const [testing, setTesting] = useState(false);
  const [step5Error, setStep5Error] = useState("");

  const [openOverride, setOpenOverride] = useState<Record<number, boolean>>({});

  function saveManual(next: ManualDone) {
    setManual(next);
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
    } catch {}
  }

  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const s = await api.feishuStatus();
        if (alive) setStatus(s);
      } catch {}
    };
    load();
    const timer = setInterval(load, connecting ? 2000 : 4000);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, [connecting]);

  const connected = !!status?.configured && !!status?.connected;
  const ownerCaptured = !!status?.owner_open_id;

  useEffect(() => {
    if (connecting && connected) setConnecting(false);
  }, [connecting, connected]);

  const done = useMemo(
    () => ({
      1: manual.s1,
      2: manual.s2,
      3: connected,
      4: manual.s4,
      5: ownerCaptured && manual.s5test,
    }),
    [manual, connected, ownerCaptured],
  );

  const firstUndone = [1, 2, 3, 4, 5].find((i) => !done[i as 1 | 2 | 3 | 4 | 5]) ?? 0;
  const isOpen = (i: number) => openOverride[i] ?? i === firstUndone;
  const toggle = (i: number) => setOpenOverride((o) => ({ ...o, [i]: !isOpen(i) }));

  async function onVerify() {
    setVerifying(true);
    setStep3Error("");
    setVerifyResult(null);
    try {
      setVerifyResult(await api.feishuVerify(appId.trim(), appSecret.trim()));
    } catch (err) {
      setStep3Error(err instanceof ApiError ? err.message : "检测失败，请重试");
    } finally {
      setVerifying(false);
    }
  }

  async function onSave() {
    setSaving(true);
    setStep3Error("");
    try {
      const r = await api.feishuConfig(appId.trim(), appSecret.trim());
      setVerifyResult(r.verify);
      setStatus(r.status);
      if (!r.status.connected) setConnecting(true);
    } catch (err) {
      setStep3Error(err instanceof ApiError ? err.message : "保存失败，请重试");
    } finally {
      setSaving(false);
    }
  }

  async function onSendTest() {
    setTesting(true);
    setStep5Error("");
    try {
      await api.feishuTest();
      saveManual({ ...manual, s5test: true });
    } catch (err) {
      setStep5Error(err instanceof ApiError ? err.message : "发送失败，请重试");
    } finally {
      setTesting(false);
    }
  }

  const credsFilled = appId.trim() !== "" && appSecret.trim() !== "";

  return (
    <div className="space-y-6">
      <div>
        <p className="text-sm text-muted-foreground">
          按顺序完成以下 5 步，即可在飞书里与 见微 Vane 对话。
          <strong className="text-foreground"> 步骤顺序不能调换</strong>——第 4 步保存"长连接"订阅方式时，飞书要求后端此刻在线。
        </p>
      </div>

      <div className="space-y-3">
        {/* Step 1 */}
        <StepCard index={1} title="创建飞书应用" done={done[1]} open={isOpen(1)} onToggle={() => toggle(1)}>
          <ol className="list-decimal list-inside space-y-2 text-sm">
            <li>
              打开{" "}
              <a
                href="https://open.feishu.cn/app"
                target="_blank"
                rel="noreferrer"
                className="text-primary hover:underline inline-flex items-center gap-0.5"
              >
                飞书开放平台 · 开发者后台
                <ExternalLink className="size-3" />
              </a>
              ，点击「创建企业自建应用」，填写应用名称（如 见微 Vane）。
            </li>
            <li>进入应用后，在「添加应用能力」中开通「机器人」能力。</li>
          </ol>
          <Button
            variant={done[1] ? "outline" : "default"}
            size="sm"
            onClick={() => saveManual({ ...manual, s1: !manual.s1 })}
          >
            {done[1] ? "取消完成标记" : "我已完成这一步"}
            {!done[1] && <Check className="ml-1 size-4" />}
          </Button>
        </StepCard>

        {/* Step 2 */}
        <StepCard index={2} title="开通权限" done={done[2]} open={isOpen(2)} onToggle={() => toggle(2)}>
          {appId.trim().startsWith("cli_") ? (
            <div className="space-y-3">
              <p className="text-sm">
                点击下方按钮一键打开权限配置页，在弹出的对话框中全选后点「确认开通权限」：
              </p>
              <Button render={<a href={buildPermissionUrl(appId.trim())} target="_blank" rel="noreferrer" />}>
                <Shield className="size-4 mr-1.5" />
                一键配置全部权限
                <ExternalLink className="size-3 ml-1" />
              </Button>
              <Collapsible>
                <CollapsibleTrigger render={<Button variant="ghost" size="sm" className="text-muted-foreground" />}>
                  <ChevronDown className="size-4 mr-1" />
                  需要开通的 {SCOPES.length} 项权限
                </CollapsibleTrigger>
                <CollapsibleContent>
                  <div className="mt-2 space-y-1">
                    {SCOPES.map((s) => (
                      <CopyRow key={s.scope} text={s.scope} desc={s.desc} />
                    ))}
                  </div>
                </CollapsibleContent>
              </Collapsible>
            </div>
          ) : (
            <div className="space-y-3">
              <p className="text-sm">
                请先在第 3 步填入 App ID（cli_ 开头），即可生成一键配置链接。也可手动逐条搜索添加：
              </p>
              <div className="space-y-1">
                {SCOPES.map((s) => (
                  <CopyRow key={s.scope} text={s.scope} desc={s.desc} />
                ))}
              </div>
            </div>
          )}
          <Button
            variant={done[2] ? "outline" : "default"}
            size="sm"
            onClick={() => saveManual({ ...manual, s2: !manual.s2 })}
          >
            {done[2] ? "取消完成标记" : "我已完成这一步"}
            {!done[2] && <Check className="ml-1 size-4" />}
          </Button>
        </StepCard>

        {/* Step 3 */}
        <StepCard index={3} title="填入凭证并连接" done={done[3]} open={isOpen(3)} onToggle={() => toggle(3)}>
          <p className="text-sm">
            在应用「凭证与基础信息」页找到 App ID 和 App Secret，填入下方并保存。
            保存后后端会立即建立飞书长连接——这是第 4 步能保存成功的前提。
          </p>
          <div className="grid gap-3 sm:grid-cols-2">
            <div className="space-y-1.5">
              <Label htmlFor="app-id">App ID</Label>
              <Input
                id="app-id"
                placeholder="cli_ 开头"
                value={appId}
                onChange={(e) => setAppId(e.target.value)}
                autoComplete="off"
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="app-secret">App Secret</Label>
              <Input
                id="app-secret"
                type="password"
                placeholder="App Secret"
                value={appSecret}
                onChange={(e) => setAppSecret(e.target.value)}
                autoComplete="off"
              />
            </div>
          </div>
          <div className="flex gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={onVerify}
              disabled={!credsFilled || verifying || saving}
            >
              {verifying ? <Loader2 className="size-4 animate-spin mr-1" /> : null}
              检测
            </Button>
            <Button
              size="sm"
              onClick={onSave}
              disabled={!credsFilled || saving || verifying}
            >
              {saving ? <Loader2 className="size-4 animate-spin mr-1" /> : null}
              保存并连接
            </Button>
          </div>
          {verifyResult && (
            <Alert
              variant={
                verifyResult.credentials_ok && verifyResult.bot_ok
                  ? "default"
                  : "destructive"
              }
              className={
                verifyResult.credentials_ok && verifyResult.bot_ok
                  ? "border-emerald-200 bg-emerald-50 dark:border-emerald-800 dark:bg-emerald-950"
                  : ""
              }
            >
              <AlertDescription>
                <div className="flex items-center gap-1.5">
                  {verifyResult.credentials_ok ? (
                    <CheckCircle2 className="size-4 text-emerald-600" />
                  ) : (
                    <AlertTriangle className="size-4" />
                  )}
                  <span>{verifyResult.credentials_ok ? "凭证有效" : "凭证无效"}</span>
                  {verifyResult.bot_name && <span>，机器人：{verifyResult.bot_name}</span>}
                </div>
                {verifyResult.detail && (
                  <p className="mt-1 text-xs opacity-80">{verifyResult.detail}</p>
                )}
                {verifyResult.credentials_ok && !verifyResult.bot_ok && (
                  <p className="mt-1 text-xs opacity-80">
                    提示：应用尚未发布时机器人状态未就绪属正常现象，不影响保存——第 4 步末尾发布版本后自然就绪。
                  </p>
                )}
              </AlertDescription>
            </Alert>
          )}
          {step3Error && (
            <Alert variant="destructive">
              <AlertDescription>{step3Error}</AlertDescription>
            </Alert>
          )}
          {connecting && !connected && (
            <Alert className="border-blue-200 bg-blue-50 dark:border-blue-800 dark:bg-blue-950">
              <AlertDescription>
                <div className="flex items-center gap-2">
                  <Loader2 className="size-4 animate-spin text-blue-600" />
                  <span>正在建立飞书长连接，请稍候…</span>
                </div>
                {status?.last_error && (
                  <p className="mt-1 text-xs opacity-80">最近错误：{status.last_error}</p>
                )}
              </AlertDescription>
            </Alert>
          )}
          {connected && (
            <Alert className="border-emerald-200 bg-emerald-50 dark:border-emerald-800 dark:bg-emerald-950">
              <AlertDescription className="flex items-center gap-1.5">
                <CheckCircle2 className="size-4 text-emerald-600" />
                长连接已建立{status?.bot_name ? `，机器人：${status.bot_name}` : ""}，可以进行第 4 步了。
              </AlertDescription>
            </Alert>
          )}
        </StepCard>

        {/* Step 4 */}
        <StepCard index={4} title="配置事件订阅并发布" done={done[4]} open={isOpen(4)} onToggle={() => toggle(4)}>
          {!connected && (
            <Alert>
              <Info className="size-4" />
              <AlertDescription>
                请先完成第 3 步：只有后端长连接在线时，飞书控制台才允许保存"长连接"订阅方式。
              </AlertDescription>
            </Alert>
          )}
          <ol className="list-decimal list-inside space-y-2 text-sm">
            <li>回到飞书开发者后台，进入应用的「事件与回调」页面。</li>
            <li>
              订阅方式选择「<strong>使用长连接接收事件</strong>」并保存（后端此刻在线，保存才会成功）。
            </li>
            <li>在「已添加事件」中添加以下事件：</li>
          </ol>
          <div className="space-y-1">
            {EVENTS.map((e) => (
              <CopyRow key={e.event} text={e.event} desc={e.desc} />
            ))}
          </div>
          <ol className="list-decimal list-inside space-y-2 text-sm" start={4}>
            <li>进入「版本管理与发布」，创建版本并发布（企业自建应用一般即时生效）。</li>
          </ol>
          <Button
            variant={done[4] ? "outline" : "default"}
            size="sm"
            onClick={() => saveManual({ ...manual, s4: !manual.s4 })}
          >
            {done[4] ? "取消完成标记" : "我已完成这一步"}
            {!done[4] && <Check className="ml-1 size-4" />}
          </Button>
        </StepCard>

        {/* Step 5 */}
        <StepCard index={5} title="对接测试" done={done[5]} open={isOpen(5)} onToggle={() => toggle(5)}>
          <p className="text-sm">在飞书里找到你的机器人，给它发一条消息（任意文本即可）。</p>
          {!ownerCaptured ? (
            <Alert className="border-blue-200 bg-blue-50 dark:border-blue-800 dark:bg-blue-950">
              <AlertDescription className="flex items-center gap-2">
                <Loader2 className="size-4 animate-spin text-blue-600" />
                等待收到你的第一条消息…（发送后几秒内会自动识别）
              </AlertDescription>
            </Alert>
          ) : (
            <Alert className="border-emerald-200 bg-emerald-50 dark:border-emerald-800 dark:bg-emerald-950">
              <AlertDescription className="flex items-center gap-1.5">
                <CheckCircle2 className="size-4 text-emerald-600" />
                已捕获 Owner：{status?.owner_name || status?.owner_open_id}
              </AlertDescription>
            </Alert>
          )}
          <Button
            size="sm"
            onClick={onSendTest}
            disabled={!ownerCaptured || testing}
          >
            {testing ? <Loader2 className="size-4 animate-spin mr-1" /> : <Send className="size-4 mr-1" />}
            发送测试卡片
          </Button>
          {manual.s5test && (
            <Alert className="border-emerald-200 bg-emerald-50 dark:border-emerald-800 dark:bg-emerald-950">
              <AlertDescription className="flex items-center gap-1.5">
                <CheckCircle2 className="size-4 text-emerald-600" />
                测试卡片已发送，去飞书查收。接入完成！
              </AlertDescription>
            </Alert>
          )}
          {step5Error && (
            <Alert variant="destructive">
              <AlertDescription>{step5Error}</AlertDescription>
            </Alert>
          )}
        </StepCard>
      </div>
    </div>
  );
}

import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { api, ApiError } from "../api";
import type { FeishuStatus, VerifyResult } from "../api";

// 飞书接入五步向导。
// ⚠️ 步骤顺序是硬约束：飞书控制台保存"长连接"订阅方式时要求当时有 WS 客户端在线，
// 所以必须先填凭证让后端连上（第 3 步），再回控制台配事件订阅（第 4 步），顺序不可调换。

// 手动确认类步骤（1/2/4）与测试卡片结果没有服务端状态可查，
// 存 localStorage 让勾选在刷新后不丢
const STORAGE_KEY = "vane_feishu_setup_done";

type ManualDone = { s1: boolean; s2: boolean; s4: boolean; s5test: boolean };

function loadManualDone(): ManualDone {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) return { s1: false, s2: false, s4: false, s5test: false, ...JSON.parse(raw) };
  } catch {
    // localStorage 不可用或数据损坏时按全未完成处理即可
  }
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

// 复制到剪贴板：优先 Clipboard API（生产是 HTTPS，可用），
// 本地 http 开发等非安全上下文降级到 execCommand
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
    <div className="copy-row">
      <code className="copy-code">{text}</code>
      <span className="copy-desc">{desc}</span>
      <button
        type="button"
        className={"btn btn-mini " + (copied ? "btn-copied" : "btn-ghost")}
        onClick={() => {
          copyText(text)
            .then(() => {
              setCopied(true);
              setTimeout(() => setCopied(false), 1500);
            })
            .catch(() => {
              // 复制失败就保持按钮原样，用户可手动选中 code 复制
            });
        }}
      >
        {copied ? "✓ 已复制" : "复制"}
      </button>
    </div>
  );
}

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
  children: ReactNode;
}) {
  return (
    <section className={"card step-card" + (done ? " step-done" : "")}>
      <button type="button" className="step-header" onClick={onToggle}>
        <span className={"step-badge" + (done ? " step-badge-done" : "")}>
          {done ? "✓" : index}
        </span>
        <span className="step-title">{title}</span>
        <span className="step-chevron">{open ? "▾" : "▸"}</span>
      </button>
      {open && <div className="step-body">{children}</div>}
    </section>
  );
}

export default function FeishuSetup() {
  const [status, setStatus] = useState<FeishuStatus | null>(null);
  const [manual, setManual] = useState<ManualDone>(loadManualDone);

  // 第 3 步：凭证表单
  const [appId, setAppId] = useState("");
  const [appSecret, setAppSecret] = useState("");
  const [verifying, setVerifying] = useState(false);
  const [verifyResult, setVerifyResult] = useState<VerifyResult | null>(null);
  const [saving, setSaving] = useState(false);
  const [connecting, setConnecting] = useState(false); // 保存成功后等待 WS 连上的轮询期
  const [step3Error, setStep3Error] = useState("");

  // 第 5 步：测试卡片
  const [testing, setTesting] = useState(false);
  const [step5Error, setStep5Error] = useState("");

  // 手动打开/收起的步骤覆盖默认展开逻辑
  const [openOverride, setOpenOverride] = useState<Record<number, boolean>>({});

  function saveManual(next: ManualDone) {
    setManual(next);
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
    } catch {
      // 存不进去也不影响当前会话使用
    }
  }

  // 全页轮询 status：第 3 步等连接、第 5 步等 owner 都靠它驱动。
  // 连接等待期加密轮询（2s），平时 4s
  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const s = await api.feishuStatus();
        if (alive) setStatus(s);
      } catch {
        // 轮询失败静默，下一轮重试；401 由 api.ts 统一踢回登录
      }
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

  // 连上之后结束"连接中"轮询态
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

  // 默认展开第一个未完成的步骤；用户手动开合优先
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
    <div className="page">
      <h2 className="page-title">飞书接入向导</h2>
      <p className="muted wizard-intro">
        按顺序完成以下 5 步，即可在飞书里与 见微 Vane 对话。
        <strong>步骤顺序不能调换</strong>——第 4 步保存"长连接"订阅方式时，飞书要求后端此刻在线。
      </p>

      <StepCard index={1} title="创建飞书应用" done={done[1]} open={isOpen(1)} onToggle={() => toggle(1)}>
        <ol className="step-list">
          <li>
            打开{" "}
            <a href="https://open.feishu.cn/app" target="_blank" rel="noreferrer">
              飞书开放平台 · 开发者后台
            </a>
            ，点击「创建企业自建应用」，填写应用名称（如 见微 Vane）。
          </li>
          <li>进入应用后，在「添加应用能力」中开通「机器人」能力。</li>
        </ol>
        <button
          type="button"
          className={"btn " + (done[1] ? "btn-ghost" : "btn-primary")}
          onClick={() => saveManual({ ...manual, s1: !manual.s1 })}
        >
          {done[1] ? "取消完成标记" : "我已完成这一步 ✓"}
        </button>
      </StepCard>

      <StepCard index={2} title="开通权限" done={done[2]} open={isOpen(2)} onToggle={() => toggle(2)}>
        {appId.trim().startsWith("cli_") ? (
          <>
            <p>
              点击下方按钮一键打开权限配置页，在弹出的对话框中全选后点「确认开通权限」：
            </p>
            <div className="btn-row">
              <a
                className="btn btn-primary"
                href={buildPermissionUrl(appId.trim())}
                target="_blank"
                rel="noreferrer"
              >
                一键配置全部权限
              </a>
            </div>
            <details className="scope-details">
              <summary className="muted">需要开通的 {SCOPES.length} 项权限</summary>
              <div className="copy-list">
                {SCOPES.map((s) => (
                  <CopyRow key={s.scope} text={s.scope} desc={s.desc} />
                ))}
              </div>
            </details>
          </>
        ) : (
          <>
            <p>请先在第 3 步填入 App ID（cli_ 开头），即可生成一键配置链接。也可手动逐条搜索添加：</p>
            <div className="copy-list">
              {SCOPES.map((s) => (
                <CopyRow key={s.scope} text={s.scope} desc={s.desc} />
              ))}
            </div>
          </>
        )}
        <button
          type="button"
          className={"btn " + (done[2] ? "btn-ghost" : "btn-primary")}
          onClick={() => saveManual({ ...manual, s2: !manual.s2 })}
        >
          {done[2] ? "取消完成标记" : "我已完成这一步 ✓"}
        </button>
      </StepCard>

      <StepCard index={3} title="填入凭证并连接" done={done[3]} open={isOpen(3)} onToggle={() => toggle(3)}>
        <p>
          在应用「凭证与基础信息」页找到 App ID 和 App Secret，填入下方并保存。
          保存后后端会立即建立飞书长连接——这是第 4 步能保存成功的前提。
        </p>
        <div className="form-grid">
          <input
            className="input"
            placeholder="App ID（cli_ 开头）"
            value={appId}
            onChange={(e) => setAppId(e.target.value)}
            autoComplete="off"
          />
          <input
            className="input"
            type="password"
            placeholder="App Secret"
            value={appSecret}
            onChange={(e) => setAppSecret(e.target.value)}
            autoComplete="off"
          />
        </div>
        <div className="btn-row">
          <button type="button" className="btn btn-ghost" onClick={onVerify} disabled={!credsFilled || verifying || saving}>
            {verifying ? <span className="spinner spinner-dark" /> : "检测"}
          </button>
          <button type="button" className="btn btn-primary" onClick={onSave} disabled={!credsFilled || saving || verifying}>
            {saving ? <span className="spinner" /> : "保存并连接"}
          </button>
        </div>
        {verifyResult && (
          <div className={"alert " + (verifyResult.credentials_ok && verifyResult.bot_ok ? "alert-ok" : "alert-warn")}>
            {verifyResult.credentials_ok ? "凭证有效" : "凭证无效"}
            {verifyResult.bot_name && <>，机器人：{verifyResult.bot_name}</>}
            {verifyResult.detail && <div className="alert-detail">{verifyResult.detail}</div>}
            {verifyResult.credentials_ok && !verifyResult.bot_ok && (
              <div className="alert-detail">
                提示：应用尚未发布时机器人状态未就绪属正常现象，不影响保存——第 4 步末尾发布版本后自然就绪。
              </div>
            )}
          </div>
        )}
        {step3Error && <div className="alert alert-error">{step3Error}</div>}
        {connecting && !connected && (
          <div className="alert alert-info">
            <span className="spinner spinner-dark" /> 正在建立飞书长连接，请稍候…
            {status?.last_error && <div className="alert-detail">最近错误：{status.last_error}</div>}
          </div>
        )}
        {connected && (
          <div className="alert alert-ok">✓ 长连接已建立{status?.bot_name ? `，机器人：${status.bot_name}` : ""}，可以进行第 4 步了。</div>
        )}
      </StepCard>

      <StepCard index={4} title="配置事件订阅并发布" done={done[4]} open={isOpen(4)} onToggle={() => toggle(4)}>
        {!connected && (
          <div className="alert alert-warn">请先完成第 3 步：只有后端长连接在线时，飞书控制台才允许保存"长连接"订阅方式。</div>
        )}
        <ol className="step-list">
          <li>回到飞书开发者后台，进入应用的「事件与回调」页面。</li>
          <li>订阅方式选择「<strong>使用长连接接收事件</strong>」并保存（后端此刻在线，保存才会成功）。</li>
          <li>在「已添加事件」中添加以下事件：</li>
        </ol>
        <div className="copy-list">
          {EVENTS.map((e) => (
            <CopyRow key={e.event} text={e.event} desc={e.desc} />
          ))}
        </div>
        <ol className="step-list" start={4}>
          <li>进入「版本管理与发布」，创建版本并发布（企业自建应用一般即时生效）。</li>
        </ol>
        <button
          type="button"
          className={"btn " + (done[4] ? "btn-ghost" : "btn-primary")}
          onClick={() => saveManual({ ...manual, s4: !manual.s4 })}
        >
          {done[4] ? "取消完成标记" : "我已完成这一步 ✓"}
        </button>
      </StepCard>

      <StepCard index={5} title="对接测试" done={done[5]} open={isOpen(5)} onToggle={() => toggle(5)}>
        <p>在飞书里找到你的机器人，给它发一条消息（任意文本即可）。</p>
        {!ownerCaptured ? (
          <div className="alert alert-info">
            <span className="spinner spinner-dark" /> 等待收到你的第一条消息…（发送后几秒内会自动识别）
          </div>
        ) : (
          <div className="alert alert-ok">
            ✓ 已捕获 Owner：{status?.owner_name || status?.owner_open_id}
          </div>
        )}
        <div className="btn-row">
          <button type="button" className="btn btn-primary" onClick={onSendTest} disabled={!ownerCaptured || testing}>
            {testing ? <span className="spinner" /> : "发送测试卡片"}
          </button>
        </div>
        {manual.s5test && <div className="alert alert-ok">✓ 测试卡片已发送，去飞书查收。接入完成！</div>}
        {step5Error && <div className="alert alert-error">{step5Error}</div>}
      </StepCard>
    </div>
  );
}

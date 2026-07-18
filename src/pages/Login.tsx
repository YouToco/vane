import { useState } from "react";
import type { FormEvent } from "react";
import { api, ApiError } from "../api";

// 登录 / 注册页：邮箱 + 密码（后端决议 D2′，vane#73）。
//
// 改造前是单个共享密码框——那时"知道密码"就等于"是主人"，没有用户概念。
// 真 SaaS 下每个租户有自己的账号，故改为邮箱+密码；注册需邀请码（决议 D4：
// 邀请制是平台垫付第三方 API 成本的财务闸门）。
//
// **本页必须与后端 vane#73 同批上线**：后端合并后旧的 {password} 形态一律 401，
// 而这个 bundle 是线上 Dashboard 的唯一入口，脱节就是全量登不进去。
type Mode = "login" | "register";

export default function Login() {
  const [mode, setMode] = useState<Mode>("login");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [inviteCode, setInviteCode] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const isRegister = mode === "register";
  // 注册额外要邀请码；两种模式都要邮箱+密码。
  const canSubmit =
    email.trim() !== "" && password !== "" && (!isRegister || inviteCode.trim() !== "");

  function switchMode(next: Mode) {
    setMode(next);
    setError("");
    // 密码与邀请码不跨模式保留：避免"在注册页输了密码、切到登录页误以为已填"。
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
    } catch (err) {
      // 后端的错误文案已是人话且刻意统一（登录失败一律"邮箱或密码不正确"，
      // 不区分"邮箱不存在"与"密码错"以防账号枚举），直接展示即可。
      setError(err instanceof ApiError ? err.message : isRegister ? "注册失败，请重试" : "登录失败，请重试");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="login-wrap">
      <form className="card login-card" onSubmit={onSubmit}>
        <div className="login-brand">见微 Vane</div>
        <p className="login-sub">AI 个性化信息推送 · 控制台</p>

        <input
          className="input"
          type="email"
          autoComplete="email"
          placeholder="邮箱"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          autoFocus
        />
        <input
          className="input"
          type="password"
          // 注册用 new-password 提示浏览器给强密码建议；登录用 current-password
          // 让密码管理器正确填充。写反会让密码管理器在登录时提议"生成新密码"。
          autoComplete={isRegister ? "new-password" : "current-password"}
          placeholder={isRegister ? "设置密码（至少 8 位）" : "密码"}
          value={password}
          onChange={(e) => setPassword(e.target.value)}
        />
        {isRegister && (
          <input
            className="input"
            type="text"
            placeholder="邀请码"
            value={inviteCode}
            onChange={(e) => setInviteCode(e.target.value)}
          />
        )}

        {error && <div className="alert alert-error">{error}</div>}

        <button className="btn btn-primary btn-block" type="submit" disabled={loading || !canSubmit}>
          {loading ? <span className="spinner" /> : isRegister ? "注 册" : "登 录"}
        </button>

        <button
          type="button"
          className="login-switch"
          onClick={() => switchMode(isRegister ? "login" : "register")}
          disabled={loading}
        >
          {isRegister ? "已有账号？去登录" : "有邀请码？去注册"}
        </button>
      </form>
    </div>
  );
}

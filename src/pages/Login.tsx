import { useState } from "react";
import type { FormEvent } from "react";
import { api, ApiError } from "../api";

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
    <div className="login-wrap">
      <form className="card login-card" onSubmit={onSubmit}>
        <div className="login-brand">见微 Vane</div>
        <p className="login-sub">AI 个性化信息推送 · 控制台</p>
        <input
          className="input"
          type="password"
          placeholder="请输入访问密码"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          autoFocus
        />
        {error && <div className="alert alert-error">{error}</div>}
        <button className="btn btn-primary btn-block" type="submit" disabled={loading || !password}>
          {loading ? <span className="spinner" /> : "登 录"}
        </button>
      </form>
    </div>
  );
}

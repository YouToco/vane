import { useEffect, useState } from "react";
import { api, ApiError } from "../api";
import type { FeishuStatus } from "../api";

// 总览页：一张飞书连接状态大卡片。
// 每 5 秒轮询一次 status——连接是后端 WS 长连接，状态可能随时变化，页面要跟得上。
export default function Home() {
  const [status, setStatus] = useState<FeishuStatus | null>(null);
  const [error, setError] = useState("");

  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const s = await api.feishuStatus();
        if (alive) {
          setStatus(s);
          setError("");
        }
      } catch (err) {
        if (alive) setError(err instanceof ApiError ? err.message : "加载失败");
      }
    };
    load();
    const timer = setInterval(load, 5000);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, []);

  return (
    <div className="page">
      <h2 className="page-title">总览</h2>
      <div className="card status-card">
        {status === null && !error && (
          <div className="status-loading">
            <span className="spinner spinner-dark" /> 正在获取飞书连接状态…
          </div>
        )}
        {error && <div className="alert alert-error">{error}</div>}
        {status !== null && !status.configured && (
          <div className="status-empty">
            <div className="status-icon">🤖</div>
            <div className="status-headline">尚未接入飞书</div>
            <p className="muted">
              完成飞书机器人接入后，即可在飞书里直接与 见微 Vane 对话。
            </p>
            <a className="btn btn-primary" href="#/setup">
              前往接入向导 →
            </a>
          </div>
        )}
        {status !== null && status.configured && (
          <div className="status-body">
            <div className="status-line">
              <span className={"dot " + (status.connected ? "dot-ok" : "dot-bad")} />
              <span className="status-headline">
                {status.connected ? "飞书已连接" : "飞书未连接"}
              </span>
            </div>
            <dl className="status-grid">
              <div>
                <dt>机器人</dt>
                <dd>{status.bot_name || "—"}</dd>
              </div>
              <div>
                <dt>Owner</dt>
                <dd>
                  {status.owner_name || (status.owner_open_id ? status.owner_open_id : "未捕获")}
                </dd>
              </div>
              <div>
                <dt>连接时间</dt>
                <dd>
                  {status.connected_at
                    ? new Date(status.connected_at).toLocaleString("zh-CN")
                    : "—"}
                </dd>
              </div>
            </dl>
            {!status.connected && status.last_error && (
              <div className="alert alert-error">最近错误：{status.last_error}</div>
            )}
            {!status.connected && (
              <a className="btn btn-ghost" href="#/setup">
                去接入向导排查 →
              </a>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

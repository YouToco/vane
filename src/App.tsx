import { useEffect, useState } from "react";
import { api } from "./api";
import Login from "./pages/Login";
import Home from "./pages/Home";
import FeishuSetup from "./pages/FeishuSetup";
import Schedules from "./pages/Schedules";
import Sources from "./pages/Sources";

// hash 微型路由：不引 react-router（契约禁止新依赖），
// 两三个页面用 hashchange 监听足够，还天然兼容静态托管的 SPA 回退。
function useHash(): string {
  const [hash, setHash] = useState(location.hash || "#/");
  useEffect(() => {
    const onChange = () => setHash(location.hash || "#/");
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);
  return hash;
}

// hash → 页面组件。集中在一处便于新增 tab；未知 hash 回落总览。
function renderPage(hash: string) {
  switch (hash) {
    case "#/setup":
      return <FeishuSetup />;
    case "#/schedules":
      return <Schedules />;
    case "#/sources":
      return <Sources />;
    default:
      return <Home />;
  }
}

// 需鉴权布局：进入时探一次 /api/auth/me——未登录时由 api.ts 统一踢到 #/login，
// 这里只负责在确认前不渲染页面内容，避免闪一下再跳走
function Shell({ hash }: { hash: string }) {
  const [checked, setChecked] = useState(false);

  useEffect(() => {
    let alive = true;
    api
      .me()
      .then(() => alive && setChecked(true))
      .catch(() => {
        // 401 已由 api.ts 跳转登录页；其他错误也不渲染内容，等用户重进
      });
    return () => {
      alive = false;
    };
  }, []);

  async function onLogout() {
    try {
      await api.logout();
    } catch {
      // 登出失败也照样回登录页，cookie 会自然过期
    }
    location.hash = "#/login";
  }

  if (!checked) {
    return (
      <div className="page-loading">
        <span className="spinner spinner-dark" /> 加载中…
      </div>
    );
  }

  return (
    <div className="shell">
      <header className="topbar">
        <a className="brand" href="#/">
          见微 <span className="brand-accent">Vane</span>
        </a>
        <nav className="nav">
          <a className={hash === "#/" ? "nav-link nav-active" : "nav-link"} href="#/">
            总览
          </a>
          <a className={hash === "#/schedules" ? "nav-link nav-active" : "nav-link"} href="#/schedules">
            定时任务
          </a>
          <a className={hash === "#/sources" ? "nav-link nav-active" : "nav-link"} href="#/sources">
            信源
          </a>
          <a className={hash === "#/setup" ? "nav-link nav-active" : "nav-link"} href="#/setup">
            飞书接入
          </a>
        </nav>
        <button type="button" className="btn btn-mini btn-ghost" onClick={onLogout}>
          退出登录
        </button>
      </header>
      <main className="main">{renderPage(hash)}</main>
    </div>
  );
}

export default function App() {
  const hash = useHash();
  if (hash === "#/login") return <Login />;
  return <Shell hash={hash} />;
}

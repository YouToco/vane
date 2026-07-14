// 统一 fetch 封装。
// 为什么集中封装：所有请求都要带 cookie（credentials:'include'），且 401 需要统一踢回登录页，
// 分散在各页面写会漏；错误统一转成后端约定的 {"error":"人话"} 文案，页面只管展示。

export interface FeishuStatus {
  configured: boolean;
  connected: boolean;
  bot_name: string;
  owner_open_id: string;
  owner_name: string;
  last_error: string;
  connected_at?: string;
}

export interface VerifyResult {
  credentials_ok: boolean;
  bot_ok: boolean;
  bot_name: string;
  detail: string;
}

export interface ConfigResult {
  status: FeishuStatus;
  verify: VerifyResult;
}

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let res: Response;
  try {
    res = await fetch(path, { credentials: "include", ...init });
  } catch {
    // fetch 本身抛错说明网络层失败（后端没起/断网），统一转成人话，
    // 避免各页面还要单独处理 TypeError
    throw new ApiError(0, "网络错误，请确认后端服务在线后重试");
  }
  // 会话失效统一踢回登录页；登录接口自身的 401 要留给登录页展示"密码错误"，不能跳
  if (res.status === 401 && path !== "/api/auth/login") {
    location.hash = "#/login";
    throw new ApiError(401, "登录已失效，请重新登录");
  }
  if (!res.ok) {
    let msg = `请求失败（HTTP ${res.status}）`;
    try {
      const body = (await res.json()) as { error?: unknown };
      if (typeof body.error === "string" && body.error) msg = body.error;
    } catch {
      // 响应体不是 JSON 时保留默认文案即可
    }
    throw new ApiError(res.status, msg);
  }
  return (await res.json()) as T;
}

function post<T>(path: string, body?: unknown): Promise<T> {
  return request<T>(path, {
    method: "POST",
    headers: body !== undefined ? { "Content-Type": "application/json" } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
}

export const api = {
  login: (password: string) => post<{ ok: boolean }>("/api/auth/login", { password }),
  logout: () => post<{ ok: boolean }>("/api/auth/logout"),
  me: () => request<{ ok: boolean }>("/api/auth/me"),
  feishuStatus: () => request<FeishuStatus>("/api/feishu/status"),
  feishuVerify: (appId: string, appSecret: string) =>
    post<VerifyResult>("/api/feishu/verify", { app_id: appId, app_secret: appSecret }),
  feishuConfig: (appId: string, appSecret: string) =>
    post<ConfigResult>("/api/feishu/config", { app_id: appId, app_secret: appSecret }),
  feishuTest: () => post<{ ok: boolean }>("/api/feishu/test"),
};

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

// ---- M3 推送管道相关类型 ----

// PushScope 与后端 workflow.PushScope 对齐（B1）。M3 固化组件默认留空 = 该用户全部 active 订阅。
export interface PushScope {
  source_ids?: number[];
  top_n?: number;
}

// ScheduleSpec 是前端时间选择器编译出的中立频率结构（B2 spec_json / B7 ScheduleSpec）。
// 二选一：cron 表达式（每天/每周档位）或 every_seconds（自定义间隔）。绝不透传任意 cron。
export interface ScheduleSpec {
  cron?: string;
  every_seconds?: number;
  tz?: string;
}

// Schedule 对应 schedules 镜像表（B2）。GET /api/schedules 读 Postgres 镜像。
export interface Schedule {
  id: string;
  nl_description: string;
  spec: ScheduleSpec;
  scope: PushScope;
  status: string; // active/paused
  next_run?: string; // 可选：后端若从 Temporal Describe 补充下次触发时间则有，否则前端按 spec 推导展示
  created_at?: string;
}

// 加订阅请求体，与后端 addSubscriptionReq 对齐（api/subscriptions.go）。
// rss 只传 {url}（type 缺省即 rss，向后兼容）；exa/tikhub_xhs 传结构化搜索参数，
// 幂等合成键（exa://... / tikhub://...）由后端生成，前端不拼。
export type AddSubscriptionReq =
  | { url: string }
  | { type: "exa"; query: string; category?: string }
  | { type: "tikhub_xhs"; keyword: string };

// Source 直接复用后端 types.Source 的 JSON（entities.go）。
// 信源管理页只用到其中几列，其余字段忽略即可。
export interface Source {
  id: number;
  type: string;
  url: string;
  title: string;
  status: string; // active/disabled/paused
  fail_count: number;
  last_fetched_at?: string;
  next_fetch_at?: string;
  created_at?: string;
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

// 后端 schedules 镜像表的 JSONB 列 tag 可能是 spec/spec_json（另一 agent owns types.Schedule），
// 且 pgx 序列化 JSONB 时可能是内联对象也可能是字符串——这里统一收敛，页面只面对稳定形状。
function asObject<T>(v: unknown): T {
  if (typeof v === "string") {
    try {
      return JSON.parse(v) as T;
    } catch {
      return {} as T;
    }
  }
  return (v ?? {}) as T;
}

function normalizeSchedule(raw: Record<string, unknown>): Schedule {
  return {
    id: String(raw.id ?? ""),
    nl_description: typeof raw.nl_description === "string" ? raw.nl_description : "",
    spec: asObject<ScheduleSpec>(raw.spec ?? raw.spec_json),
    scope: asObject<PushScope>(raw.scope ?? raw.scope_json),
    status: typeof raw.status === "string" ? raw.status : "active",
    next_run: (raw.next_run ?? raw.next_run_at) as string | undefined,
    created_at: raw.created_at as string | undefined,
  };
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

  // ---- M3 定时任务（B8）----
  listSchedules: () =>
    request<Record<string, unknown>[]>("/api/schedules").then((rows) =>
      (rows ?? []).map(normalizeSchedule),
    ),
  createSchedule: (spec: ScheduleSpec, scope: PushScope, nlDescription: string) =>
    post<{ schedule_id: string }>("/api/schedules", {
      spec,
      scope,
      nl_description: nlDescription,
    }),
  deleteSchedule: (id: string) =>
    request<{ ok: boolean }>(`/api/schedules/${encodeURIComponent(id)}`, { method: "DELETE" }),
  // push/now 的 body 是 {scope?}（B8）；固化"现在推一次"默认不带 scope = 推全部订阅
  pushNow: (scope?: PushScope) =>
    post<{ run_id: string }>("/api/push/now", scope ? { scope } : {}),

  // ---- M3 信源管理（B8）----
  listSubscriptions: () => request<Source[]>("/api/subscriptions"),
  addSubscription: (req: AddSubscriptionReq) =>
    post<{ source_id: number }>("/api/subscriptions", req),
  removeSubscription: (sourceId: number) =>
    request<{ ok: boolean }>(`/api/subscriptions/${sourceId}`, { method: "DELETE" }),
};

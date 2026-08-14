// 北京时间展示的统一入口（此前 5 个页面各持一份逐字副本，收敛于此）。
//
// 为什么钉死 zh-CN + hour12:false 而不跟访问者 locale：全站时间列的口径是
// 「北京时间」——推送调度、后端聚合都以北京日界叙事，展示格式跟着数据口径
// 走而不是跟着 UI 语言走。hour12 必须显式写死：它的默认值由引擎按 locale
// 推导，属于会随环境漂移的隐式依赖（TaskDashboard 曾漏写，当前 V8 下
// zh-CN 恰好也是 24 小时制所以没暴露——这正是把它收敛到一处的理由）。
export const BEIJING_TZ = "Asia/Shanghai";

export function fmtBeijing(iso?: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString("zh-CN", { timeZone: BEIJING_TZ, hour12: false });
}

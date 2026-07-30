import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Info, ShieldCheck } from "lucide-react";
import Observability from "./Observability";
import Costs from "./Costs";
import Invites from "./Invites";
import Pricing from "./Pricing";
import CallCostLedger from "./CallCostLedger";
import { useI18n } from "@/i18n";

// 管理面：只承载**平台级**视图（跨租户/系统级），与用户面严格分开。
//
// 为什么这几页必须在这里而不在用户面：它们打的 /api/admin/* 在后端被
// requirePlatformOwner 门控（tenant_id==1）。之前把它们摆在用户侧边栏里，
// 非平台 owner 的用户点进去只会吃 404——不是安全漏洞（后端拦住了），
// 是前端把管理员的东西摆给了所有人。页顶的 owner 标识条同理：向误入者
// （以及未来的第二个管理员）说明这里的一切是平台级的，不是个人设置。
export default function Admin() {
  const { t, locale } = useI18n();
  const A = t.app.admin;
  // 可观测/成本两页正文是平台运维视图，保持中文（受众就是平台 owner 本人）；
  // 非中文语言下加一条说明，避免用户以为页面坏了——同 Settings 的 channelZhOnly 先例。
  // 邀请页是新写的、全量走字典，不需要这条。
  const zhOnly =
    locale === "zh" || locale === "zh-Hant" ? null : (
      <Alert>
        <Info className="size-4" />
        <AlertDescription>{A.zhOnly}</AlertDescription>
      </Alert>
    );

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center gap-3">
        <h1 className="text-xl font-semibold">{A.title}</h1>
        {/* 平台级标识：brand 层做背景/边框、brand-strong 做小字，
            遵循 docs/design-system.md 的对比度基线（brand 本色 <3:1 禁做字色）。 */}
        <span className="inline-flex items-center gap-1.5 rounded-full border border-brand/40 bg-brand/10 px-2.5 py-1 text-xs font-medium text-brand-strong">
          <ShieldCheck className="size-3.5" />
          {A.ownerBadge}
        </span>
      </div>
      <Tabs defaultValue="observability">
        <TabsList className="max-w-full justify-start overflow-x-auto">
          <TabsTrigger value="observability">{A.tabObservability}</TabsTrigger>
          <TabsTrigger value="costs">{A.tabCosts}</TabsTrigger>
          <TabsTrigger value="cost-calls">{A.tabCallCosts}</TabsTrigger>
          <TabsTrigger value="pricing">{A.tabPricing}</TabsTrigger>
          <TabsTrigger value="invites">{A.tabInvites}</TabsTrigger>
        </TabsList>
        <TabsContent value="observability" className="mt-4 space-y-4">
          {zhOnly}
          <Observability />
        </TabsContent>
        <TabsContent value="costs" className="mt-4 space-y-4">
          {zhOnly}
          <Costs />
        </TabsContent>
        <TabsContent value="cost-calls" className="mt-4 space-y-4">
          {zhOnly}
          <CallCostLedger />
        </TabsContent>
        <TabsContent value="pricing" className="mt-4 space-y-4">
          {zhOnly}
          <Pricing />
        </TabsContent>
        <TabsContent value="invites" className="mt-4">
          <Invites />
        </TabsContent>
      </Tabs>
    </div>
  );
}

import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import Observability from "./Observability";
import Costs from "./Costs";

// 管理面：只承载**平台级**视图（跨租户/系统级），与用户面严格分开。
//
// 为什么这两页必须在这里而不在用户面：它们打的 /api/admin/observability 与
// /api/admin/runstats 在后端被 requirePlatformOwner 门控（tenant_id==1）。
// 之前把它们摆在用户侧边栏里，非平台 owner 的用户点进去只会吃 404——
// 不是安全漏洞（后端拦住了），是前端把管理员的东西摆给了所有人。
export default function Admin() {
  return (
    <div className="space-y-6">
      <h1 className="text-xl font-semibold">管理后台</h1>
      <Tabs defaultValue="observability">
        <TabsList>
          <TabsTrigger value="observability">可观测</TabsTrigger>
          <TabsTrigger value="costs">LLM 成本</TabsTrigger>
        </TabsList>
        <TabsContent value="observability" className="mt-4">
          <Observability />
        </TabsContent>
        <TabsContent value="costs" className="mt-4">
          <Costs />
        </TabsContent>
      </Tabs>
    </div>
  );
}

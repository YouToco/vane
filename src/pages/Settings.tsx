import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import FeishuSetup from "./FeishuSetup";
import Profile from "./Profile";

// 侧边栏「我的画像」「推送通道」是两个独立入口，但共用本页的 tab 容器：
// 入口指向 #/settings 与 #/settings/channel，由 hash 决定落在哪个 tab。
// 这样侧边栏保持扁平（用户一眼看到有什么），页面内仍可左右切换。
export default function Settings({ hash }: { hash: string }) {
  const tab = hash === "#/settings/channel" ? "channel" : "profile";

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-semibold">设置</h1>
      <Tabs
        value={tab}
        onValueChange={(v) => {
          location.hash = v === "channel" ? "#/settings/channel" : "#/settings";
        }}
      >
        <TabsList>
          <TabsTrigger value="profile">我的画像</TabsTrigger>
          <TabsTrigger value="channel">推送通道</TabsTrigger>
        </TabsList>
        <TabsContent value="profile" className="mt-4">
          <Profile />
        </TabsContent>
        <TabsContent value="channel" className="mt-4">
          <FeishuSetup />
        </TabsContent>
      </Tabs>
    </div>
  );
}

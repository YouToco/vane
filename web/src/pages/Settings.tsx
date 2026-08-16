import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Info } from "lucide-react";
import FeishuSetup from "./FeishuSetup";
import TelegramSetup from "./TelegramSetup";
import DeliveryChannelPreferenceCard from "./DeliveryChannelPreference";
import Profile from "./Profile";
import { useI18n } from "@/i18n";

// 侧边栏「我的画像」「推送通道」是两个独立入口，但共用本页的 tab 容器：
// 入口指向 #/settings 与 #/settings/channel，由 hash 决定落在哪个 tab。
// 这样侧边栏保持扁平（用户一眼看到有什么），页面内仍可左右切换。
export default function Settings({ hash }: { hash: string }) {
  const { t, locale } = useI18n();
  const S = t.app.settings;
  const tab = hash === "#/settings/channel" ? "channel" : "profile";
  // 推送通道配置指南面向飞书（中国版）开放平台，正文保持中文；
  // 非中文语言下给一条说明，避免用户以为页面坏了。
  const zhChannel = locale === "zh" || locale === "zh-Hant";

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-semibold">{S.title}</h1>
      <Tabs
        value={tab}
        onValueChange={(v) => {
          location.hash = v === "channel" ? "#/settings/channel" : "#/settings";
        }}
      >
        <TabsList>
          <TabsTrigger value="profile">{S.tabProfile}</TabsTrigger>
          <TabsTrigger value="channel">{S.tabChannel}</TabsTrigger>
        </TabsList>
        <TabsContent value="profile" className="mt-4">
          <Profile />
        </TabsContent>
        <TabsContent value="channel" className="mt-4 space-y-4">
          {!zhChannel && (
            <Alert>
              <Info className="size-4" />
              <AlertDescription>{S.channelZhOnly}</AlertDescription>
            </Alert>
          )}
          <DeliveryChannelPreferenceCard />
          <FeishuSetup />
          <TelegramSetup />
        </TabsContent>
      </Tabs>
    </div>
  );
}

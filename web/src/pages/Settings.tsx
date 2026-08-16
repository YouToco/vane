import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Info } from "lucide-react";
import FeishuSetup from "./FeishuSetup";
import Profile from "./Profile";
import WorkspaceMembers from "./WorkspaceMembers";
import A2AAccessTokens from "./A2AAccessTokens";
import { useI18n } from "@/i18n";
import type { MeResponse } from "@/shared/api/client";

// 侧边栏「我的画像」「推送通道」是两个独立入口，但共用本页的 tab 容器：
// 入口指向 #/settings 与 #/settings/channel，由 hash 决定落在哪个 tab。
// 这样侧边栏保持扁平（用户一眼看到有什么），页面内仍可左右切换。
export default function Settings({
  hash,
  me,
  onAuthorityChanged,
}: {
  hash: string;
  me: MeResponse;
  onAuthorityChanged: () => void;
}) {
  const { t, locale } = useI18n();
  const S = t.app.settings;
  const tab = hash === "#/settings/channel"
    ? "channel"
    : hash === "#/settings/members"
      ? "members"
      : hash === "#/settings/access"
        ? "access"
      : "profile";
  // 推送通道配置指南面向飞书（中国版）开放平台，正文保持中文；
  // 非中文语言下给一条说明，避免用户以为页面坏了。
  const zhChannel = locale === "zh" || locale === "zh-Hant";

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-semibold">{S.title}</h1>
      <Tabs
        value={tab}
        onValueChange={(v) => {
          location.hash = v === "channel"
            ? "#/settings/channel"
            : v === "members"
              ? "#/settings/members"
              : v === "access"
                ? "#/settings/access"
              : "#/settings";
        }}
      >
        <TabsList className="max-w-full justify-start overflow-x-auto">
          <TabsTrigger value="profile">{S.tabProfile}</TabsTrigger>
          <TabsTrigger value="channel">{S.tabChannel}</TabsTrigger>
          <TabsTrigger value="members">{S.tabMembers}</TabsTrigger>
          <TabsTrigger value="access">{locale === "zh" || locale === "zh-Hant" ? "访问凭证" : "Access credentials"}</TabsTrigger>
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
          <FeishuSetup />
        </TabsContent>
        <TabsContent value="members" className="mt-4">
          <WorkspaceMembers me={me} onAuthorityChanged={onAuthorityChanged} />
        </TabsContent>
        <TabsContent value="access" className="mt-4">
          <A2AAccessTokens me={me} />
        </TabsContent>
      </Tabs>
    </div>
  );
}

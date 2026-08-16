import { useEffect, useState } from "react";
import { BellRing, Loader2 } from "lucide-react";
import { api, ApiError } from "@/shared/api/client";
import type { DeliveryChannelPreference, DeliveryChannelSelection } from "@/shared/api/client";
import type { TelegramRoute } from "@/shared/api/client";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

const choices: Array<{
  value: DeliveryChannelSelection;
  title: string;
  description: string;
}> = [
  { value: "feishu", title: "仅飞书", description: "简报、周报等只发送到飞书。" },
  { value: "telegram", title: "仅 Telegram", description: "简报、周报等只发送到 Telegram。" },
  { value: "both", title: "两个渠道", description: "同一份内容分别发送，两个渠道独立结算。" },
];

export default function DeliveryChannelPreferenceCard() {
  const [preference, setPreference] = useState<DeliveryChannelPreference | null>(null);
  const [saving, setSaving] = useState<DeliveryChannelSelection | "">("");
  const [routes, setRoutes] = useState<TelegramRoute[]>([]);
  const [routeID, setRouteID] = useState<number | undefined>();
  const [message, setMessage] = useState("");

  useEffect(() => {
    Promise.all([api.deliveryChannelPreference(), api.telegramStatus()])
      .then(([nextPreference, telegram]) => {
        setPreference(nextPreference);
        setRouteID(nextPreference.telegram_route_id);
        setRoutes(telegram.routes ?? []);
      })
      .catch((error) => setMessage(error instanceof ApiError ? error.message : "读取投递渠道失败"));
  }, []);

  async function choose(selection: DeliveryChannelSelection) {
    setSaving(selection);
    setMessage("");
    try {
      const next = await api.patchDeliveryChannelPreference(selection, routeID);
      setPreference(next);
      setRouteID(next.telegram_route_id);
      setMessage("默认投递渠道已保存。主动推送运行时完成渠道化后会使用这个选择；历史回执不会被改写。");
    } catch (error) {
      setMessage(error instanceof ApiError ? error.message : "保存投递渠道失败");
    } finally {
      setSaving("");
    }
  }

  return (
    <Card>
      <CardHeader className="space-y-2">
        <CardTitle className="flex items-center gap-2 text-base">
          <BellRing className="size-4" />默认推送渠道
        </CardTitle>
        <p className="text-sm text-muted-foreground">
          预先设置 Vane 生成的简报、周报和后续主动通知发到哪里。主动推送运行时尚在渠道化；选择两个渠道时，每个渠道将拥有独立发送回执，一个渠道故障不会伪装成另一个渠道成功。
        </p>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="grid gap-3 md:grid-cols-3">
          {choices.map((choice) => {
            const selected = preference?.selection === choice.value;
            return (
              <Button
                key={choice.value}
                variant={selected ? "default" : "outline"}
                className="h-auto min-h-20 items-start justify-start whitespace-normal px-4 py-3 text-left"
                disabled={saving !== ""}
                onClick={() => void choose(choice.value)}
              >
                {saving === choice.value && <Loader2 className="mr-2 mt-0.5 size-4 shrink-0 animate-spin" />}
                <span>
                  <span className="block font-medium">{choice.title}</span>
                  <span className={selected ? "block text-xs text-primary-foreground/80" : "block text-xs text-muted-foreground"}>
                    {choice.description}
                  </span>
                </span>
              </Button>
            );
          })}
        </div>
        {(preference?.selection === "telegram" || preference?.selection === "both") && (
          <div className="space-y-2 rounded-lg border p-3">
            <label htmlFor="telegram-delivery-route" className="text-sm font-medium">Telegram 推送目的地</label>
            <div className="flex flex-wrap gap-2">
              <select
                id="telegram-delivery-route"
                className="h-9 min-w-60 rounded-md border bg-background px-3 text-sm"
                value={routeID ?? ""}
                onChange={(event) => setRouteID(event.target.value ? Number(event.target.value) : undefined)}
              >
                <option value="">自动使用私聊</option>
                {routes.map((route) => (
                  <option key={route.id} value={route.id}>
                    {route.kind === "topic" ? "论坛话题" : route.kind === "group" ? "群组" : "私聊"} #{route.id}
                  </option>
                ))}
              </select>
              <Button
                variant="outline"
                disabled={saving !== ""}
                onClick={() => void choose(preference.selection)}
              >保存目的地</Button>
            </div>
            <p className="text-xs text-muted-foreground">目的地会冻结到每条发送 effect；改选不会重定向历史消息。</p>
          </div>
        )}
        {preference && !preference.explicit && (
          <Alert><AlertDescription>当前沿用兼容默认值：仅飞书。保存后才会建立显式偏好。</AlertDescription></Alert>
        )}
        {message && <p className="text-sm text-muted-foreground">{message}</p>}
      </CardContent>
    </Card>
  );
}

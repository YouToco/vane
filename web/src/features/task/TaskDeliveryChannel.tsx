import { useEffect, useState } from "react";
import { BellRing, Loader2 } from "lucide-react";
import { api, ApiError } from "@/shared/api/client";
import type {
  DeliveryChannelPreference,
  DeliveryChannelSelection,
  TelegramRoute,
} from "@/shared/api/client";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

const channelLabels: Record<DeliveryChannelSelection, string> = {
  feishu: "仅飞书",
  telegram: "仅 Telegram",
  both: "飞书 + Telegram",
};

export function deliveryChannelLabel(preference?: DeliveryChannelPreference): string {
  return preference ? channelLabels[preference.selection] : "读取中";
}

export default function TaskDeliveryChannel({
  scheduleID,
  initial,
  onChange,
}: {
  scheduleID: string;
  initial: DeliveryChannelPreference;
  onChange?: (next: DeliveryChannelPreference) => void;
}) {
  const [preference, setPreference] = useState(initial);
  const [routes, setRoutes] = useState<TelegramRoute[]>([]);
  const [routeID, setRouteID] = useState<number | undefined>(initial.telegram_route_id);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");

  useEffect(() => {
    let alive = true;
    api.telegramStatus()
      .then((status) => alive && setRoutes(status.routes ?? []))
      .catch(() => undefined);
    return () => { alive = false; };
  }, []);

  async function apply(selection: DeliveryChannelSelection) {
    setBusy(true);
    setMessage("");
    try {
      const next = await api.patchTaskDeliveryChannelPreference(
        scheduleID,
        selection,
        routeID,
      );
      setPreference(next);
      setRouteID(next.telegram_route_id);
      setMessage("任务推送渠道已保存。");
      onChange?.(next);
    } catch (cause) {
      setMessage(cause instanceof ApiError ? cause.message : "保存任务推送渠道失败");
    } finally {
      setBusy(false);
    }
  }

  async function inheritAccount() {
    setBusy(true);
    setMessage("");
    try {
      const next = await api.deleteTaskDeliveryChannelPreference(scheduleID);
      setPreference(next);
      setRouteID(next.telegram_route_id);
      setMessage("已恢复使用账号默认推送渠道。");
      onChange?.(next);
    } catch (cause) {
      setMessage(cause instanceof ApiError ? cause.message : "恢复账号默认渠道失败");
    } finally {
      setBusy(false);
    }
  }

  const needsTelegram =
    preference.selection === "telegram" || preference.selection === "both";

  return (
    <Card>
      <CardHeader className="space-y-2">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <CardTitle className="flex items-center gap-2 text-base">
            <BellRing className="size-4" />定时推送渠道
          </CardTitle>
          <Badge variant="outline">{deliveryChannelLabel(preference)}</Badge>
        </div>
        <p className="text-sm text-muted-foreground">
          这项设置只影响当前任务；每次运行都会按保存时的 Bot、群组和话题发送。
        </p>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="flex flex-wrap gap-2">
          {(Object.keys(channelLabels) as DeliveryChannelSelection[]).map((selection) => (
            <Button
              key={selection}
              size="sm"
              variant={preference.selection === selection ? "default" : "outline"}
              disabled={busy}
              onClick={() => void apply(selection)}
            >
              {busy && preference.selection === selection && (
                <Loader2 className="mr-2 size-4 animate-spin" />
              )}
              {channelLabels[selection]}
            </Button>
          ))}
          {preference.scope === "task" && (
            <Button size="sm" variant="ghost" disabled={busy} onClick={() => void inheritAccount()}>
              使用账号默认
            </Button>
          )}
        </div>
        {needsTelegram && (
          <div className="flex flex-wrap items-end gap-2 rounded-lg border p-3">
            <label className="grid gap-1 text-sm" htmlFor="task-telegram-route">
              Telegram 目的地
              <select
                id="task-telegram-route"
                className="h-9 min-w-60 rounded-md border bg-background px-3"
                value={routeID ?? ""}
                disabled={busy}
                onChange={(event) => setRouteID(event.target.value ? Number(event.target.value) : undefined)}
              >
                <option value="">自动使用私聊</option>
                {routes.map((route) => (
                  <option key={route.id} value={route.id}>
                    {route.kind === "topic" ? "论坛话题" : route.kind === "group" ? "群组" : "私聊"} #{route.id}
                  </option>
                ))}
              </select>
            </label>
            <Button size="sm" variant="outline" disabled={busy} onClick={() => void apply(preference.selection)}>
              保存目的地
            </Button>
          </div>
        )}
        {preference.scope !== "task" && (
          <p className="text-xs text-muted-foreground">当前继承账号默认值；保存任一选项后建立任务级覆盖。</p>
        )}
        {message && <Alert><AlertDescription>{message}</AlertDescription></Alert>}
      </CardContent>
    </Card>
  );
}

import { useEffect, useState } from "react";
import { toast } from "sonner";
import { api, ApiError } from "../api";
import type { Invite } from "../api";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from "@/components/ui/table";
import { RefreshCw, Loader2, Plus, Copy, Ban } from "lucide-react";
import { fmt, useI18n } from "@/i18n";
import { fmtBeijing } from "@/lib/time";

// 判定用服务端算好的 used/expired 布尔（vane#104：used=用满、expired 按服务端
// 时钟），前端不从 used_count/expires_at 重算——两边口径漂移时以服务端为准。
// 这里只把后端建议的「有效」档细分成 未使用/使用中（多用码部分消费），
// 细分依据 used_count 是展示层加工，不改变有效性判定。
type InviteState = "unused" | "partial" | "used" | "expired";

function inviteState(inv: Invite): InviteState {
  if (inv.used) return "used";
  if (inv.expired) return "expired";
  return inv.used_count > 0 ? "partial" : "unused";
}

// unused 是在外流通的活码，最醒目；expired 标红提示该作废清理；used 是完结态，灰化。
const STATE_VARIANT: Record<InviteState, "default" | "secondary" | "outline" | "destructive"> = {
  unused: "default",
  partial: "secondary",
  used: "outline",
  expired: "destructive",
};

export default function Invites() {
  const { t } = useI18n();
  const V = t.app.admin.invites;
  const STATE_LABEL: Record<InviteState, string> = {
    unused: V.statusUnused,
    partial: V.statusPartial,
    used: V.statusUsed,
    expired: V.statusExpired,
  };
  const [items, setItems] = useState<Invite[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [creating, setCreating] = useState(false);
  const [revokingCode, setRevokingCode] = useState<string | null>(null);
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    api
      .adminListInvites()
      .then((rows) => {
        if (!alive) return;
        setItems(rows);
        setLoadError("");
      })
      .catch((err) => {
        if (!alive) return;
        setLoadError(err instanceof ApiError ? err.message : t.app.common.loadFailed);
        setItems([]);
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [nonce]);

  async function onCreate() {
    setCreating(true);
    try {
      const inv = await api.adminCreateInvite();
      toast.success(fmt(V.generated, { code: inv.code }));
      setNonce((n) => n + 1);
    } catch (err) {
      toast.error(err instanceof ApiError ? err.message : V.generateFailed);
    } finally {
      setCreating(false);
    }
  }

  async function onRevoke(code: string) {
    setRevokingCode(code);
    try {
      await api.adminRevokeInvite(code);
      toast.success(fmt(V.revoked, { code }));
      setNonce((n) => n + 1);
    } catch (err) {
      toast.error(err instanceof ApiError ? err.message : V.revokeFailed);
      // 409/404 意味着本地列表已失真：码在加载后被注册流消费（409）或已在
      // 别处被作废（404）。只 toast 不重取会让表格继续显示「未使用」+可点的
      // 作废按钮，与刚弹出的错误自相矛盾——冲突错误自带一次视图重同步。
      if (err instanceof ApiError && (err.status === 409 || err.status === 404)) {
        setNonce((n) => n + 1);
      }
    } finally {
      setRevokingCode(null);
    }
  }

  async function onCopy(code: string) {
    try {
      await navigator.clipboard.writeText(code);
      toast.success(V.copied);
    } catch {
      // 非安全上下文（生产是 https、dev 是 localhost，正常到不了这里）或权限被拒
      toast.error(V.copyFailed);
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between gap-4">
        <p className="text-sm text-muted-foreground">{V.desc}</p>
        <div className="flex shrink-0 gap-2">
          <Button
            variant="outline"
            size="sm"
            title={V.refresh}
            onClick={() => setNonce((n) => n + 1)}
            disabled={loading}
          >
            {loading ? (
              <Loader2 className="size-4 animate-spin" />
            ) : (
              <RefreshCw className="size-4" />
            )}
          </Button>
          <Button size="sm" onClick={onCreate} disabled={creating}>
            {creating ? (
              <>
                <Loader2 className="mr-1 size-4 animate-spin" />
                {V.generating}
              </>
            ) : (
              <>
                <Plus className="mr-1 size-4" />
                {V.generate}
              </>
            )}
          </Button>
        </div>
      </div>

      {loadError && (
        <Alert variant="destructive">
          <AlertDescription>{loadError}</AlertDescription>
        </Alert>
      )}

      {loading ? (
        <Card>
          <CardContent className="py-6 space-y-3">
            {Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="flex gap-4">
                <Skeleton className="h-4 w-40" />
                <Skeleton className="h-4 w-16" />
                <Skeleton className="h-4 w-12" />
                <Skeleton className="h-4 w-32" />
              </div>
            ))}
          </CardContent>
        </Card>
      ) : items.length === 0 ? (
        !loadError && (
          <Card>
            <CardContent className="py-12 text-center text-muted-foreground">
              {V.empty}
            </CardContent>
          </Card>
        )
      ) : (
        <Card>
          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>{V.colCode}</TableHead>
                  <TableHead>{V.colStatus}</TableHead>
                  <TableHead className="text-right">{V.colUses}</TableHead>
                  <TableHead>{V.colIssuedAt}</TableHead>
                  <TableHead>{V.colExpiresAt}</TableHead>
                  <TableHead>{V.colConsumedAt}</TableHead>
                  <TableHead className="text-right">{V.colActions}</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {items.map((inv) => {
                  const state = inviteState(inv);
                  return (
                    <TableRow key={inv.code}>
                      <TableCell className="whitespace-nowrap">
                        <span className="inline-flex items-center gap-1">
                          <code className="font-mono text-sm">{inv.code}</code>
                          <Button
                            variant="ghost"
                            size="icon"
                            className="size-6"
                            title={V.copy}
                            onClick={() => onCopy(inv.code)}
                          >
                            <Copy className="size-3.5" />
                          </Button>
                        </span>
                      </TableCell>
                      <TableCell>
                        <Badge variant={STATE_VARIANT[state]}>{STATE_LABEL[state]}</Badge>
                      </TableCell>
                      <TableCell className="text-right font-mono text-sm">
                        {inv.used_count} / {inv.max_uses}
                      </TableCell>
                      <TableCell className="text-sm whitespace-nowrap">
                        {fmtBeijing(inv.created_at)}
                      </TableCell>
                      <TableCell className="text-sm whitespace-nowrap">
                        {inv.expires_at ? (
                          fmtBeijing(inv.expires_at)
                        ) : (
                          <span className="text-muted-foreground">{V.neverExpires}</span>
                        )}
                      </TableCell>
                      {/* used_by/used_at 是最近一次消费（多用码上 used_count 才是权威计数）；
                          owner 无邮箱（纯飞书用户）时只有时刻没有人。 */}
                      <TableCell className="text-sm whitespace-nowrap">
                        {inv.used_at ? (
                          <span>
                            {inv.used_by && <span className="mr-1.5">{inv.used_by}</span>}
                            <span className={inv.used_by ? "text-muted-foreground" : ""}>
                              {fmtBeijing(inv.used_at)}
                            </span>
                          </span>
                        ) : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </TableCell>
                      <TableCell className="text-right">
                        {/* 约定只有未消费过的码可作废（used_count==0）；用过的不摆
                            注定被后端拒绝的按钮——与 App.tsx 管理入口同一哲学。 */}
                        {inv.used_count === 0 ? (
                          <Button
                            variant="ghost"
                            size="sm"
                            className="text-destructive hover:text-destructive"
                            onClick={() => onRevoke(inv.code)}
                            disabled={revokingCode !== null}
                          >
                            {revokingCode === inv.code ? (
                              <Loader2 className="size-4 animate-spin" />
                            ) : (
                              <Ban className="size-4" />
                            )}
                            {V.revoke}
                          </Button>
                        ) : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </div>
        </Card>
      )}
    </div>
  );
}

import { useState } from "react";
import { api } from "../api";
import { Button } from "@/components/ui/button";
import { RefreshCw, Loader2 } from "lucide-react";
import { useI18n } from "@/i18n";
import DeliveriesTable from "@/components/DeliveriesTable";

// 投递档案页：DeliveriesTable 专门承载跨任务的发送凭证、反馈和审计记录。
// 本页只负责全局取数（api.listDeliveries）与刷新按钮（换 key 重挂载）。
export default function History() {
  const { t } = useI18n();
  const [nonce, setNonce] = useState(0);
  const [loading, setLoading] = useState(true);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm text-muted-foreground">{t.app.history.desc}</p>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={() => setNonce((n) => n + 1)}
          disabled={loading}
        >
          {loading ? (
            <Loader2 className="size-4 animate-spin" />
          ) : (
            <RefreshCw className="size-4" />
          )}
        </Button>
      </div>

      <DeliveriesTable
        key={nonce}
        fetchPage={api.listDeliveries}
        emptyText={t.app.history.empty}
        onLoadingChange={setLoading}
      />
    </div>
  );
}

import { useState } from "react";
import {
  Building2,
  Check,
  ChevronsUpDown,
  Loader2,
  UserRound,
} from "lucide-react";
import { toast } from "sonner";

import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { SidebarMenuButton } from "@/components/ui/sidebar";
import type { MeResponse, WorkspaceSummary } from "@/shared/api/client";

const roleLabels: Record<WorkspaceSummary["role"], string> = {
  owner: "Owner",
  admin: "Admin",
  member: "Member",
};

export function WorkspaceSwitcher({
  me,
  onSwitch,
}: {
  me: MeResponse;
  onSwitch: (tenantID: number) => Promise<void>;
}) {
  const [switchingTo, setSwitchingTo] = useState<number | null>(null);
  const workspaces = me.workspaces ?? [];
  const current = workspaces.find((workspace) => workspace.id === me.tenant_id);

  // During a rolling deploy an older backend may omit workspaces. Hiding the
  // selector is safer than inventing a workspace from tenant_id.
  if (!current) return null;

  async function switchTo(workspace: WorkspaceSummary) {
    if (workspace.id === me.tenant_id || switchingTo !== null) return;
    setSwitchingTo(workspace.id);
    try {
      await onSwitch(workspace.id);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "切换工作区失败");
      setSwitchingTo(null);
    }
  }

  const CurrentIcon = current.kind === "personal" ? UserRound : Building2;
  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        render={
          <SidebarMenuButton
            size="lg"
            aria-label={`当前工作区：${current.name}`}
            className="data-[popup-open]:bg-sidebar-accent data-[popup-open]:text-sidebar-accent-foreground"
          />
        }
      >
        <span className="flex size-8 items-center justify-center rounded-lg bg-sidebar-accent">
          <CurrentIcon className="size-4" />
        </span>
        <span className="flex min-w-0 flex-1 flex-col leading-tight">
          <span className="truncate text-sm font-medium">{current.name}</span>
          <span className="text-[10px] text-muted-foreground">
            {current.kind === "personal" ? "个人空间" : "团队空间"} · {roleLabels[current.role]}
          </span>
        </span>
        {switchingTo !== null ? (
          <Loader2 className="ml-auto size-4 animate-spin text-muted-foreground" />
        ) : (
          <ChevronsUpDown className="ml-auto size-4 text-muted-foreground" />
        )}
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" side="bottom">
        {workspaces.map((workspace) => {
          const Icon = workspace.kind === "personal" ? UserRound : Building2;
          const selected = workspace.id === me.tenant_id;
          return (
            <DropdownMenuItem
              key={workspace.id}
              onClick={() => void switchTo(workspace)}
              disabled={switchingTo !== null || selected}
            >
              <Icon />
              <span className="min-w-0 flex-1">
                <span className="block truncate">{workspace.name}</span>
                <span className="block text-[10px] text-muted-foreground">
                  {roleLabels[workspace.role]}
                </span>
              </span>
              {selected && <Check className="ml-auto" aria-label="当前" />}
            </DropdownMenuItem>
          );
        })}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

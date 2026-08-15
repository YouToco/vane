import { lazy, Suspense } from "react";
import { api, PLATFORM_OWNER_TENANT_ID } from "@/shared/api/client";
import type { MeResponse } from "@/shared/api/client";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarInset,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarProvider,
  SidebarTrigger,
} from "@/components/ui/sidebar";
import { Separator } from "@/components/ui/separator";
import {
  Home as HomeIcon,
  ListTodo,
  Send,
  User,
  MessageSquare,
  ShieldCheck,
  LogOut,
  Loader2,
  ChevronsUpDown,
  Users,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { LogoMark } from "@/shared/brand/Logo";
import { LocaleSwitch } from "@/app/LocaleSwitch";
import { useI18n } from "@/i18n";
import { clearTaskMutationSessionStorage } from "@/shared/runtime/task-action-session";
import { WorkspaceSwitcher } from "@/app/WorkspaceSwitcher";

const Home = lazy(() => import("@/pages/Home"));
const TaskDashboard = lazy(() => import("@/pages/TaskDashboard"));
const TaskDetail = lazy(() => import("@/pages/TaskDetail"));
const History = lazy(() => import("@/pages/History"));
const Settings = lazy(() => import("@/pages/Settings"));
const Admin = lazy(() => import("@/pages/Admin"));

interface NavItem {
  hash: string;
  label: string;
  icon: LucideIcon;
}

// 侧边栏按「这是谁的东西」分组，而不是按「这是什么功能」。
// 我的情报 = 日常要看的产出；账号 = 影响产出的自身设置。
// 文案来自 i18n 字典，导航结构随语言重建（结构轻，无需 memo 精调）。
function useNav() {
  const { t } = useI18n();
  const N = t.app.nav;
  const groups: { label: string; items: NavItem[] }[] = [
    {
      label: N.groupIntel,
      items: [
        { hash: "#/", label: N.home, icon: HomeIcon },
        { hash: "#/tasks", label: N.tasks, icon: ListTodo },
        { hash: "#/history", label: N.history, icon: Send },
      ],
    },
    {
      label: N.groupAccount,
      items: [
        { hash: "#/settings", label: N.profile, icon: User },
        { hash: "#/settings/channel", label: N.channel, icon: MessageSquare },
        { hash: "#/settings/members", label: N.members, icon: Users },
      ],
    },
  ];
  const all: NavItem[] = [
    ...groups.flatMap((g) => g.items),
    { hash: "#/admin", label: N.admin, icon: ShieldCheck },
  ];
  return { groups, all };
}

// 任务详情是唯一的动态路由：#/tasks/{scheduleID}。schedule id 是后端生成的
// push-{user}-{uuid}（无斜杠），前缀截断即可，不需要真路由库。
function taskDetailID(hash: string): string | null {
  if (!hash.startsWith("#/tasks/")) return null;
  const id = decodeURIComponent(hash.slice("#/tasks/".length));
  return id || null;
}

function renderPage(
  hash: string,
  isPlatformOwner: boolean,
  actorScope: string,
  me: MeResponse,
  onAuthorityChanged: () => void,
) {
  const detailID = taskDetailID(hash);
  if (detailID) {
    return <TaskDetail scheduleID={detailID} actorScope={actorScope} />;
  }
  switch (hash) {
    case "#/tasks":
      return <TaskDashboard actorScope={actorScope} />;
    case "#/history":
      return <History />;
    case "#/settings":
    case "#/settings/channel":
    case "#/settings/members":
      return <Settings hash={hash} me={me} onAuthorityChanged={onAuthorityChanged} />;
    case "#/admin":
      // 前端兜底：非平台 owner 直接落回首页。真正的拦截在后端
      // requirePlatformOwner，这里只是避免渲染一个注定 404 的页面。
      return isPlatformOwner ? <Admin /> : <Home />;
    default:
      return <Home />;
  }
}

function PageFallback() {
  return (
    <div
      className="flex min-h-48 items-center justify-center gap-2 text-muted-foreground"
      role="status"
      aria-label="Loading page"
    >
      <Loader2 className="size-5 animate-spin" />
    </div>
  );
}

// NavUser：sidebar 底部的当前用户块（SaaS 惯例）——首字母头像 + 邮箱 +
// owner 徽章，下拉收纳退出登录。email 为空（存量飞书用户）时退化显示 #user_id。
function NavUser({
  me,
  isPlatformOwner,
  onLogout,
}: {
  me: MeResponse;
  isPlatformOwner: boolean;
  onLogout: () => void;
}) {
  const { t } = useI18n();
  const email = me.email?.trim() ?? "";
  const display = email || `#${me.user_id}`;
  const initial = (email[0] ?? "V").toUpperCase();
  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        render={
          <SidebarMenuButton
            size="lg"
            className="data-[popup-open]:bg-sidebar-accent data-[popup-open]:text-sidebar-accent-foreground"
          />
        }
      >
        <Avatar className="size-8 rounded-lg">
          <AvatarFallback className="rounded-lg bg-brand/15 text-brand-strong text-sm font-semibold">
            {initial}
          </AvatarFallback>
        </Avatar>
        <div className="flex min-w-0 flex-col leading-tight">
          <span className="truncate text-sm font-medium">{display}</span>
          {isPlatformOwner && (
            <span className="text-[10px] font-medium text-brand-strong">Owner</span>
          )}
        </div>
        <ChevronsUpDown className="ml-auto size-4 text-muted-foreground" />
      </DropdownMenuTrigger>
      {/* Popup 基类自带 w-(--anchor-width) 等宽，无需额外宽度类 */}
      <DropdownMenuContent side="top" align="start">
        <DropdownMenuItem onClick={onLogout}>
          <LogOut />
          {t.app.nav.logout}
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

function AppSidebar({
  hash,
  me,
  isPlatformOwner,
  onLogout,
  onSwitchWorkspace,
}: {
  hash: string;
  me: MeResponse;
  isPlatformOwner: boolean;
  onLogout: () => void;
  onSwitchWorkspace: (tenantID: number) => Promise<void>;
}) {
  const { t } = useI18n();
  const { groups } = useNav();
  return (
    <Sidebar>
      <SidebarHeader>
        <a href="#/" className="flex items-center gap-2 px-2 py-1">
          <LogoMark />
          <div className="flex flex-col leading-tight">
            <span className="text-sm font-semibold">{t.brandName}</span>
            <span className="text-[11px] text-muted-foreground">{t.app.nav.tagline}</span>
          </div>
        </a>
        <WorkspaceSwitcher me={me} onSwitch={onSwitchWorkspace} />
      </SidebarHeader>
      <SidebarContent>
        {groups.map((group) => (
          <SidebarGroup key={group.label}>
            <SidebarGroupLabel>{group.label}</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                {group.items.map((item) => (
                  <SidebarMenuItem key={item.hash}>
                    <SidebarMenuButton
                      render={<a href={item.hash} />}
                      isActive={
                        hash === item.hash ||
                        (item.hash === "#/tasks" && Boolean(taskDetailID(hash)))
                      }
                    >
                      <item.icon />
                      <span>{item.label}</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                ))}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        ))}
      </SidebarContent>
      <SidebarFooter>
        <SidebarMenu>
          {isPlatformOwner && (
            <SidebarMenuItem>
              <SidebarMenuButton render={<a href="#/admin" />} isActive={hash === "#/admin"}>
                <ShieldCheck />
                <span>{t.app.nav.admin}</span>
              </SidebarMenuButton>
            </SidebarMenuItem>
          )}
          <SidebarMenuItem>
            <NavUser me={me} isPlatformOwner={isPlatformOwner} onLogout={onLogout} />
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarFooter>
    </Sidebar>
  );
}

function Shell({ hash, me }: { hash: string; me: MeResponse }) {
  const { t } = useI18n();
  const { all } = useNav();
  const isPlatformOwner = me.tenant_id === PLATFORM_OWNER_TENANT_ID;
  const actorScope = `${me.tenant_id}:${me.user_id}`;
  async function onLogout() {
    try {
      clearTaskMutationSessionStorage();
    } catch {}
    try {
      await api.logout();
    } catch {}
    location.reload();
  }
  async function onSwitchWorkspace(tenantID: number) {
    // Pending write confirmations are scoped to the old workspace and must
    // never survive a principal/token rotation.
    clearTaskMutationSessionStorage();
    await api.switchWorkspace(tenantID);
    location.hash = "#/";
    location.reload();
  }
  function onAuthorityChanged() {
    // Ownership transfer invalidates the server session. Clear all browser
    // state scoped to the old Principal before returning to authentication.
    clearTaskMutationSessionStorage();
    location.hash = "#/login";
    location.reload();
  }

  return (
    <SidebarProvider>
      <AppSidebar
        hash={hash}
        me={me}
        isPlatformOwner={isPlatformOwner}
        onLogout={onLogout}
        onSwitchWorkspace={onSwitchWorkspace}
      />
      <SidebarInset>
        <header className="flex h-12 shrink-0 items-center gap-2 border-b px-4">
          <SidebarTrigger className="-ml-1" />
          <Separator orientation="vertical" className="mr-2 !h-4" />
          <span className="text-sm text-muted-foreground">
            {/* 详情页归属「我的任务」；找不到的 hash 落回首页文案，与 renderPage 一致 */}
            {taskDetailID(hash)
              ? t.app.nav.tasks
              : (all.find((i) => i.hash === hash)?.label ?? t.app.nav.home)}
          </span>
          <div className="ml-auto">
            <LocaleSwitch />
          </div>
        </header>
        <main className="flex-1 overflow-y-auto">
          <div className="mx-auto max-w-5xl p-6">
            <Suspense fallback={<PageFallback />}>
              {renderPage(hash, isPlatformOwner, actorScope, me, onAuthorityChanged)}
            </Suspense>
          </div>
        </main>
      </SidebarInset>
    </SidebarProvider>
  );
}

export default function AuthenticatedApp({
  hash,
  me,
}: {
  hash: string;
  me: MeResponse;
}) {
  return <Shell hash={hash} me={me} />;
}

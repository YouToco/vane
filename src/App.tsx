import { useEffect, useState } from "react";
import { api, PLATFORM_OWNER_TENANT_ID } from "./api";
import Landing from "./pages/Landing";
import Login from "./pages/Login";
import Home from "./pages/Home";
import TaskDashboard from "./pages/TaskDashboard";
import History from "./pages/History";
import Settings from "./pages/Settings";
import Admin from "./pages/Admin";
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
} from "lucide-react";
import type { LucideIcon } from "lucide-react";
import { LogoMark } from "@/components/brand/Logo";
import { LocaleSwitch } from "@/components/LocaleSwitch";
import { useI18n } from "@/i18n";

function useHash(): string {
  const [hash, setHash] = useState(location.hash || "#/");
  useEffect(() => {
    const onChange = () => setHash(location.hash || "#/");
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);
  return hash;
}

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
      ],
    },
  ];
  const all: NavItem[] = [
    ...groups.flatMap((g) => g.items),
    { hash: "#/admin", label: N.admin, icon: ShieldCheck },
  ];
  return { groups, all };
}

function renderPage(hash: string, isPlatformOwner: boolean) {
  switch (hash) {
    case "#/tasks":
      return <TaskDashboard />;
    case "#/history":
      return <History />;
    case "#/settings":
    case "#/settings/channel":
      return <Settings hash={hash} />;
    case "#/admin":
      // 前端兜底：非平台 owner 直接落回首页。真正的拦截在后端
      // requirePlatformOwner，这里只是避免渲染一个注定 404 的页面。
      return isPlatformOwner ? <Admin /> : <Home />;
    default:
      return <Home />;
  }
}

function AppSidebar({
  hash,
  isPlatformOwner,
  onLogout,
}: {
  hash: string;
  isPlatformOwner: boolean;
  onLogout: () => void;
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
      </SidebarHeader>
      <SidebarContent>
        {groups.map((group) => (
          <SidebarGroup key={group.label}>
            <SidebarGroupLabel>{group.label}</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                {group.items.map((item) => (
                  <SidebarMenuItem key={item.hash}>
                    <SidebarMenuButton render={<a href={item.hash} />} isActive={hash === item.hash}>
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
            <SidebarMenuButton onClick={onLogout}>
              <LogOut />
              <span>{t.app.nav.logout}</span>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarFooter>
    </Sidebar>
  );
}

function Shell({ hash, isPlatformOwner }: { hash: string; isPlatformOwner: boolean }) {
  const { t } = useI18n();
  const { all } = useNav();
  async function onLogout() {
    try {
      await api.logout();
    } catch {}
    location.reload();
  }

  return (
    <SidebarProvider>
      <AppSidebar hash={hash} isPlatformOwner={isPlatformOwner} onLogout={onLogout} />
      <SidebarInset>
        <header className="flex h-12 shrink-0 items-center gap-2 border-b px-4">
          <SidebarTrigger className="-ml-1" />
          <Separator orientation="vertical" className="mr-2 !h-4" />
          <span className="text-sm text-muted-foreground">
            {all.find((i) => i.hash === hash)?.label ?? t.app.nav.home}
          </span>
          <div className="ml-auto">
            <LocaleSwitch />
          </div>
        </header>
        <main className="flex-1 overflow-y-auto">
          <div className="mx-auto max-w-5xl p-6">{renderPage(hash, isPlatformOwner)}</div>
        </main>
      </SidebarInset>
    </SidebarProvider>
  );
}

export default function App() {
  const hash = useHash();
  const [authed, setAuthed] = useState<boolean | null>(null);
  const [isPlatformOwner, setIsPlatformOwner] = useState(false);

  useEffect(() => {
    api
      .me()
      .then((me) => {
        setIsPlatformOwner(me.tenant_id === PLATFORM_OWNER_TENANT_ID);
        setAuthed(true);
      })
      .catch(() => setAuthed(false));
  }, []);

  if (hash === "#/login") return <Login />;
  if (authed === null) {
    return (
      <div className="flex h-screen items-center justify-center gap-2 text-muted-foreground">
        <Loader2 className="size-5 animate-spin" />
      </div>
    );
  }
  if (!authed) return <Landing />;
  return <Shell hash={hash} isPlatformOwner={isPlatformOwner} />;
}

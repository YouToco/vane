import { useEffect, useState } from "react";
import { api } from "./api";
import Login from "./pages/Login";
import Home from "./pages/Home";
import FeishuSetup from "./pages/FeishuSetup";
import Schedules from "./pages/Schedules";
import Sources from "./pages/Sources";
import Observability from "./pages/Observability";
import Costs from "./pages/Costs";
import History from "./pages/History";
import Profile from "./pages/Profile";
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
  LayoutDashboard,
  Clock,
  Rss,
  History as HistoryIcon,
  User,
  MessageSquare,
  Activity,
  DollarSign,
  LogOut,
  Loader2,
  Zap,
} from "lucide-react";

function useHash(): string {
  const [hash, setHash] = useState(location.hash || "#/");
  useEffect(() => {
    const onChange = () => setHash(location.hash || "#/");
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);
  return hash;
}

const NAV_ITEMS = [
  { hash: "#/", label: "总览", icon: LayoutDashboard },
  { hash: "#/schedules", label: "定时任务", icon: Clock },
  { hash: "#/sources", label: "信源", icon: Rss },
  { hash: "#/history", label: "推送历史", icon: HistoryIcon },
  { hash: "#/profile", label: "画像", icon: User },
] as const;

const ADMIN_ITEMS = [
  { hash: "#/setup", label: "飞书接入", icon: MessageSquare },
  { hash: "#/observability", label: "可观测", icon: Activity },
  { hash: "#/costs", label: "成本", icon: DollarSign },
] as const;

function renderPage(hash: string) {
  switch (hash) {
    case "#/setup":
      return <FeishuSetup />;
    case "#/schedules":
      return <Schedules />;
    case "#/sources":
      return <Sources />;
    case "#/observability":
      return <Observability />;
    case "#/history":
      return <History />;
    case "#/profile":
      return <Profile />;
    case "#/costs":
      return <Costs />;
    default:
      return <Home />;
  }
}

function AppSidebar({ hash, onLogout }: { hash: string; onLogout: () => void }) {
  return (
    <Sidebar>
      <SidebarHeader>
        <a href="#/" className="flex items-center gap-2 px-2 py-1">
          <div className="flex size-8 items-center justify-center rounded-lg bg-primary text-primary-foreground">
            <Zap className="size-4" />
          </div>
          <div className="flex flex-col leading-tight">
            <span className="text-sm font-semibold">见微 Vane</span>
            <span className="text-[11px] text-muted-foreground">AI 信息推送</span>
          </div>
        </a>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>主要功能</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {NAV_ITEMS.map((item) => (
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
        <SidebarGroup>
          <SidebarGroupLabel>设置与监控</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {ADMIN_ITEMS.map((item) => (
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
      </SidebarContent>
      <SidebarFooter>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton onClick={onLogout}>
              <LogOut />
              <span>退出登录</span>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarFooter>
    </Sidebar>
  );
}

function Shell({ hash }: { hash: string }) {
  const [checked, setChecked] = useState(false);

  useEffect(() => {
    let alive = true;
    api
      .me()
      .then(() => alive && setChecked(true))
      .catch(() => {});
    return () => {
      alive = false;
    };
  }, []);

  async function onLogout() {
    try {
      await api.logout();
    } catch {}
    location.hash = "#/login";
  }

  if (!checked) {
    return (
      <div className="flex h-screen items-center justify-center gap-2 text-muted-foreground">
        <Loader2 className="size-5 animate-spin" />
        <span>加载中…</span>
      </div>
    );
  }

  return (
    <SidebarProvider>
      <AppSidebar hash={hash} onLogout={onLogout} />
      <SidebarInset>
        <header className="flex h-12 shrink-0 items-center gap-2 border-b px-4">
          <SidebarTrigger className="-ml-1" />
          <Separator orientation="vertical" className="mr-2 !h-4" />
          <span className="text-sm text-muted-foreground">
            {[...NAV_ITEMS, ...ADMIN_ITEMS].find((i) => i.hash === hash)?.label ?? "总览"}
          </span>
        </header>
        <main className="flex-1 overflow-y-auto">
          <div className="mx-auto max-w-5xl p-6">{renderPage(hash)}</div>
        </main>
      </SidebarInset>
    </SidebarProvider>
  );
}

export default function App() {
  const hash = useHash();
  if (hash === "#/login") return <Login />;
  return <Shell hash={hash} />;
}

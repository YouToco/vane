import { lazy, Suspense, useEffect, useState } from "react";
import { Loader2 } from "lucide-react";
import { api } from "@/shared/api/client";
import type { MeResponse } from "@/shared/api/client";

const Landing = lazy(() => import("@/pages/Landing"));
const Login = lazy(() => import("@/pages/Login"));
const AuthenticatedApp = lazy(() => import("./AuthenticatedApp"));

function useHash(): string {
  const [hash, setHash] = useState(location.hash || "#/");
  useEffect(() => {
    const onChange = () => setHash(location.hash || "#/");
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);
  return hash;
}

function AppFallback() {
  return (
    <div
      className="flex h-screen items-center justify-center gap-2 text-muted-foreground"
      role="status"
      aria-label="Loading application"
    >
      <Loader2 className="size-5 animate-spin" />
    </div>
  );
}

export default function App() {
  const hash = useHash();
  // me 三态：undefined=探测中，null=未登录，对象=已登录（含用户块所需 email）。
  const [me, setMe] = useState<MeResponse | null | undefined>(undefined);

  useEffect(() => {
    api
      .me()
      .then(setMe)
      .catch(() => setMe(null));
  }, []);

  if (me === undefined && hash !== "#/login") return <AppFallback />;

  return (
    <Suspense fallback={<AppFallback />}>
      {hash === "#/login" ? (
        <Login />
      ) : me === null ? (
        <Landing />
      ) : me ? (
        <AuthenticatedApp hash={hash} me={me} />
      ) : (
        <AppFallback />
      )}
    </Suspense>
  );
}

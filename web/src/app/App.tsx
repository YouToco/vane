import { lazy, Suspense, useEffect, useState } from "react";
import { Loader2 } from "lucide-react";
import { api } from "@/shared/api/client";
import type { MeResponse } from "@/shared/api/client";

const Landing = lazy(() => import("@/pages/Landing"));
const Login = lazy(() => import("@/pages/Login"));
const Setup = lazy(() => import("@/pages/Setup"));
const AccountSecurity = lazy(() => import("@/pages/AccountSecurity"));
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
  const isAccountSecurityRoute =
    hash === "#/forgot-password" ||
    hash.startsWith("#/verify-email?") ||
    hash.startsWith("#/reset-password?");
  // me 三态：undefined=探测中，null=未登录，对象=已登录（含用户块所需 email）。
  const [me, setMe] = useState<MeResponse | null | undefined>(undefined);
  const [setupState, setSetupState] = useState<
    "active" | "setup_required" | "unavailable" | undefined
  >(undefined);

  useEffect(() => {
    api
      .setupStatus()
      .then((status) => {
        if (status.setup_required) {
          setSetupState("setup_required");
          setMe(null);
          return;
        }
        setSetupState("active");
        api
          .me()
          .then(setMe)
          .catch(() => setMe(null));
      })
      .catch(() => {
        setSetupState("unavailable");
        setMe(null);
      });
  }, []);

  if (
    setupState === undefined ||
    (setupState === "active" &&
      me === undefined &&
      hash !== "#/login" &&
      !isAccountSecurityRoute)
  ) {
    return <AppFallback />;
  }

  return (
    <Suspense fallback={<AppFallback />}>
      {setupState === "setup_required" || setupState === "unavailable" ? (
        <Setup unavailable={setupState === "unavailable"} />
      ) : isAccountSecurityRoute ? (
        <AccountSecurity hash={hash} />
      ) : hash === "#/login" ? (
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

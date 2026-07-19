import { useEffect, useState } from "react";
import { api, ApiError } from "../api";
import type { Profile as ProfileData } from "../api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import { Separator } from "@/components/ui/separator";
import { RefreshCw, Loader2, Tag, FileText, Ban } from "lucide-react";
import { useI18n } from "@/i18n";

const BEIJING_TZ = "Asia/Shanghai";

function fmtBeijing(iso?: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString("zh-CN", { timeZone: BEIJING_TZ, hour12: false });
}

export default function Profile() {
  const { t } = useI18n();
  const P = t.app.profile;
  const [profile, setProfile] = useState<ProfileData | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [notGenerated, setNotGenerated] = useState(false);
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    api
      .profile()
      .then((p) => {
        if (!alive) return;
        setProfile(p);
        setNotGenerated(false);
        setLoadError("");
      })
      .catch((err) => {
        if (!alive) return;
        if (err instanceof ApiError && err.status === 404) {
          setNotGenerated(true);
          setProfile(null);
          setLoadError("");
          return;
        }
        setLoadError(err instanceof ApiError ? err.message : t.app.common.loadFailed);
        setProfile(null);
        setNotGenerated(false);
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [nonce]);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold tracking-tight">{P.title}</h2>
          <p className="text-sm text-muted-foreground mt-1">{P.desc}</p>
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

      {loadError && (
        <Alert variant="destructive">
          <AlertDescription>{loadError}</AlertDescription>
        </Alert>
      )}

      {loading ? (
        <div className="space-y-4">
          <Card>
            <CardContent className="py-6 space-y-4">
              <div className="grid grid-cols-3 gap-4">
                {Array.from({ length: 3 }).map((_, i) => (
                  <div key={i} className="space-y-2">
                    <Skeleton className="h-3 w-16" />
                    <Skeleton className="h-5 w-24" />
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="py-6 space-y-3">
              <Skeleton className="h-4 w-20" />
              <div className="flex gap-2">
                {Array.from({ length: 5 }).map((_, i) => (
                  <Skeleton key={i} className="h-6 w-16 rounded-full" />
                ))}
              </div>
            </CardContent>
          </Card>
        </div>
      ) : notGenerated ? (
        <Card>
          <CardContent className="py-12 text-center text-muted-foreground">
            {P.notGenerated}
          </CardContent>
        </Card>
      ) : profile ? (
        <>
          <Card>
            <CardContent className="py-6">
              <div className="grid grid-cols-3 gap-6">
                <div>
                  <p className="text-xs text-muted-foreground mb-1">{P.industry}</p>
                  <p className="text-sm font-medium">
                    {profile.industry || (
                      <span className="text-muted-foreground">{t.app.common.notFilled}</span>
                    )}
                  </p>
                </div>
                <div>
                  <p className="text-xs text-muted-foreground mb-1">{P.occupation}</p>
                  <p className="text-sm font-medium">
                    {profile.occupation || (
                      <span className="text-muted-foreground">{t.app.common.notFilled}</span>
                    )}
                  </p>
                </div>
                <div>
                  <p className="text-xs text-muted-foreground mb-1">{P.updatedAt}</p>
                  <p className="text-sm font-medium">{fmtBeijing(profile.updated_at)}</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <Tag className="size-4" />
                {P.tags}
              </CardTitle>
            </CardHeader>
            <CardContent>
              {profile.tags.length === 0 ? (
                <p className="text-sm text-muted-foreground">{P.noTags}</p>
              ) : (
                <div className="flex flex-wrap gap-2">
                  {profile.tags.map((t) => (
                    <Badge key={t} variant="secondary">
                      {t}
                    </Badge>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <FileText className="size-4" />
                {P.summary}
              </CardTitle>
            </CardHeader>
            <CardContent>
              {profile.summary ? (
                <pre className="text-sm leading-relaxed whitespace-pre-wrap font-sans">
                  {profile.summary}
                </pre>
              ) : (
                <p className="text-sm text-muted-foreground">{P.noSummary}</p>
              )}
              <Separator className="my-3" />
              <p className="text-xs text-muted-foreground">{P.negNote}</p>
            </CardContent>
          </Card>

          {profile.removed_tags.length > 0 && (
            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-base flex items-center gap-2">
                  <Ban className="size-4" />
                  {P.removedTags}
                </CardTitle>
              </CardHeader>
              <CardContent>
                <p className="text-xs text-muted-foreground mb-3">{P.removedNote}</p>
                <div className="flex flex-wrap gap-2">
                  {profile.removed_tags.map((t) => (
                    <Badge key={t} variant="outline" className="text-muted-foreground line-through">
                      {t}
                    </Badge>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}
        </>
      ) : null}
    </div>
  );
}

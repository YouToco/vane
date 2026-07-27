import { useCallback, useEffect, useId, useMemo, useRef, useState } from "react";
import { api, ApiError } from "../api";
import type {
  EditableProfileField,
  Profile as ProfileData,
  ProfileEdit,
  UpdateProfileRequest,
} from "../api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import { Separator } from "@/components/ui/separator";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import {
  Ban,
  ChevronDown,
  FileText,
  History,
  Loader2,
  RefreshCw,
  RotateCcw,
  Save,
  X,
} from "lucide-react";
import { fmt, useI18n } from "@/i18n";
import { fmtBeijing } from "@/lib/time";
import { cn } from "@/lib/utils";

interface EditableProfile {
  industry: string;
  occupation: string;
  tags: string[];
}

function editableFrom(profile: ProfileData): EditableProfile {
  return {
    industry: profile.industry,
    occupation: profile.occupation,
    tags: [...profile.tags],
  };
}

function cleanTag(raw: string): string {
  return raw.trim();
}

function uniqueTags(tags: string[]): string[] {
  const seen = new Set<string>();
  return tags.filter((raw) => {
    const tag = cleanTag(raw);
    if (!tag || seen.has(tag)) return false;
    seen.add(tag);
    return true;
  }).map(cleanTag);
}

function tagParts(raw: string): string[] {
  return raw.split(/[,，]/);
}

function hasControlCharacter(value: string): boolean {
  return /[\u0000-\u001f\u007f-\u009f\u2028\u2029]/u.test(value);
}

function buildUpdate(
  profile: ProfileData,
  draft: EditableProfile,
): UpdateProfileRequest | null {
  const input: UpdateProfileRequest = {
    expected_updated_at: profile.updated_at || null,
  };
  // 先比较原始值，再只规范化用户实际改过的字段。否则存量画像里的 legacy
  // whitespace/重复标签会在用户只改职业时被悄悄带进 PATCH，越过人工意图边界。
  if (draft.industry !== profile.industry) {
    const industry = draft.industry.trim();
    if (industry !== profile.industry) input.industry = industry;
  }
  if (draft.occupation !== profile.occupation) {
    const occupation = draft.occupation.trim();
    if (occupation !== profile.occupation) input.occupation = occupation;
  }
  if (JSON.stringify(draft.tags) !== JSON.stringify(profile.tags)) {
    const tags = uniqueTags(draft.tags);
    if (JSON.stringify(tags) !== JSON.stringify(profile.tags)) input.tags = tags;
  }
  return Object.keys(input).length === 1 ? null : input;
}

export default function Profile() {
  const { t } = useI18n();
  const P = t.app.profile;
  const industryID = useId();
  const occupationID = useId();
  const tagInputID = useId();
  const loadFailedRef = useRef(t.app.common.loadFailed);
  loadFailedRef.current = t.app.common.loadFailed;
  const saveIntent = useRef<{ signature: string; key: string } | null>(null);
  const undoIntents = useRef(new Map<string, string>());
  const [profile, setProfile] = useState<ProfileData | null>(null);
  const [draft, setDraft] = useState<EditableProfile | null>(null);
  const [tagInput, setTagInput] = useState("");
  const [edits, setEdits] = useState<ProfileEdit[]>([]);
  const [loading, setLoading] = useState(true);
  const [editsLoading, setEditsLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [undoingID, setUndoingID] = useState("");
  const [loadError, setLoadError] = useState("");
  const [editsError, setEditsError] = useState("");
  const [mutationError, setMutationError] = useState("");
  const [conflict, setConflict] = useState(false);
  const [saved, setSaved] = useState(false);
  const [notGenerated, setNotGenerated] = useState(false);
  const [summaryOpen, setSummaryOpen] = useState(false);
  const [nonce, setNonce] = useState(0);

  const reload = useCallback(() => {
    setNonce((n) => n + 1);
  }, []);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    setEditsLoading(true);
    setLoadError("");
    setEditsError("");
    setMutationError("");
    setConflict(false);

    void api
      .profile()
      .then((next) => {
        if (!alive) return;
        setProfile(next);
        setDraft(editableFrom(next));
        setTagInput("");
        setNotGenerated(false);
      })
      .catch((err) => {
        if (!alive) return;
        if (err instanceof ApiError && err.status === 404) {
          const emptyProfile: ProfileData = {
            industry: "",
            occupation: "",
            tags: [],
            removed_tags: [],
            summary: "",
            created_at: "",
            updated_at: "",
          };
          setNotGenerated(true);
          setProfile(emptyProfile);
          setDraft(editableFrom(emptyProfile));
          return;
        }
        setLoadError(err instanceof ApiError ? err.message : loadFailedRef.current);
        setProfile(null);
        setDraft(null);
        setNotGenerated(false);
      })
      .finally(() => {
        if (alive) setLoading(false);
      });

    void api
      .profileEdits(20)
      .then((response) => {
        if (!alive) return;
        setEdits(response.edits);
      })
      .catch((err) => {
        if (!alive) return;
        setEdits([]);
        setEditsError(err instanceof ApiError ? err.message : loadFailedRef.current);
      })
      .finally(() => {
        if (alive) setEditsLoading(false);
      });

    return () => {
      alive = false;
    };
  }, [nonce]);

  const effectiveDraft = useMemo(() => {
    if (!draft || !tagInput.trim()) return draft;
    const incoming = tagParts(tagInput).map(cleanTag);
    return { ...draft, tags: uniqueTags([...draft.tags, ...incoming]) };
  }, [draft, tagInput]);
  const update = useMemo(
    () => (profile && effectiveDraft ? buildUpdate(profile, effectiveDraft) : null),
    [effectiveDraft, profile],
  );
  const tagValidation = useMemo(() => {
    if (!effectiveDraft || !profile) return "";
    if (hasControlCharacter(tagInput)) return P.invalidTagControl;
    if (
      tagInput.trim() &&
      tagParts(tagInput).some((part) => cleanTag(part) === "")
    ) {
      return P.emptyTag;
    }
    // 未触碰的 legacy tags 由后端后续迁移处理，不能阻塞对其他字段的人工修正。
    if (JSON.stringify(effectiveDraft.tags) === JSON.stringify(profile.tags)) return "";
    if (effectiveDraft.tags.some(hasControlCharacter)) return P.invalidTagControl;
    if (effectiveDraft.tags.length > 12) return P.tooManyTags;
    const oversized = effectiveDraft.tags.find((tag) => Array.from(tag).length > 20);
    return oversized ? fmt(P.tagTooLong, { tag: oversized }) : "";
  }, [
    P.emptyTag,
    P.invalidTagControl,
    P.tagTooLong,
    P.tooManyTags,
    effectiveDraft,
    profile,
    tagInput,
  ]);

  function requestReload() {
    if (update && !window.confirm(P.confirmReload)) return;
    reload();
  }

  function idempotencyKey(prefix: string): string {
    return `${prefix}-${crypto.randomUUID()}`;
  }

  function addPendingTags() {
    if (!draft || hasControlCharacter(tagInput)) return;
    const incomingParts = tagParts(tagInput);
    if (incomingParts.some((part) => cleanTag(part) === "")) return;
    const incoming = incomingParts.map(cleanTag);
    const tags = uniqueTags([...draft.tags, ...incoming]);
    setDraft({ ...draft, tags });
    setTagInput("");
  }

  function removeTag(tag: string) {
    if (!draft) return;
    setDraft({ ...draft, tags: draft.tags.filter((item) => item !== tag) });
  }

  async function save() {
    if (!update || saving || tagValidation) return;
    setSaving(true);
    setMutationError("");
    setConflict(false);
    setSaved(false);
    try {
      const signature = JSON.stringify(update);
      if (!saveIntent.current || saveIntent.current.signature !== signature) {
        saveIntent.current = {
          signature,
          key: idempotencyKey("profile-edit"),
        };
      }
      await api.updateProfile(update, saveIntent.current.key);
      saveIntent.current = null;
      setSaved(true);
      reload();
    } catch (err) {
      if (err instanceof ApiError && err.status === 409) {
        saveIntent.current = null;
        setConflict(true);
      } else {
        setMutationError(err instanceof ApiError ? err.message : P.saveFailed);
      }
    } finally {
      setSaving(false);
    }
  }

  async function undo(edit: ProfileEdit) {
    if (!profile || !edit.undoable || undoingID) return;
    if (update && !window.confirm(P.confirmUndoDirty)) return;
    setUndoingID(edit.id);
    setMutationError("");
    setConflict(false);
    setSaved(false);
    try {
      let key = undoIntents.current.get(edit.id);
      if (!key) {
        key = idempotencyKey("profile-undo");
        undoIntents.current.set(edit.id, key);
      }
      await api.undoProfileEdit(edit.id, profile.updated_at, key);
      undoIntents.current.delete(edit.id);
      setSaved(true);
      reload();
    } catch (err) {
      if (err instanceof ApiError && err.status === 409) {
        undoIntents.current.delete(edit.id);
        setConflict(true);
      } else {
        setMutationError(err instanceof ApiError ? err.message : P.undoFailed);
      }
    } finally {
      setUndoingID("");
    }
  }

  function changeLabel(change: ProfileEdit["changes"][number]): string {
    const labels: Record<EditableProfileField, string> = {
      industry: P.industry,
      occupation: P.occupation,
      tags: P.tags,
    };
    const renderValue = (value: string | string[] | null) =>
      Array.isArray(value) ? value.join(P.tagSeparator) || P.emptyValue : value || P.emptyValue;
    return fmt(P.editChange, {
      field: labels[change.field],
      before: renderValue(change.before),
      after: renderValue(change.after),
    });
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <h2 className="text-xl font-semibold tracking-tight">{P.title}</h2>
          <p className="mt-1 text-sm text-muted-foreground">{P.desc}</p>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={requestReload}
          disabled={loading || saving || Boolean(undoingID)}
          aria-label={P.reload}
        >
          {loading ? (
            <Loader2 className="size-4 animate-spin" />
          ) : (
            <RefreshCw className="size-4" />
          )}
          {P.reload}
        </Button>
      </div>

      {loadError && (
        <Alert variant="destructive" role="alert">
          <AlertDescription>{loadError}</AlertDescription>
        </Alert>
      )}
      {conflict && (
        <Alert variant="destructive" role="alert">
          <AlertDescription className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <span>{P.conflict}</span>
            <Button size="sm" variant="outline" onClick={requestReload}>
              <RefreshCw className="size-4" />
              {P.reload}
            </Button>
          </AlertDescription>
        </Alert>
      )}
      {mutationError && (
        <Alert variant="destructive" role="alert">
          <AlertDescription>{mutationError}</AlertDescription>
        </Alert>
      )}
      {saved && (
        <Alert role="status">
          <AlertDescription>{P.saved}</AlertDescription>
        </Alert>
      )}

      {loading ? (
        <ProfileSkeleton />
      ) : profile && draft ? (
        <>
          {notGenerated && (
            <Alert role="status">
              <AlertDescription>{P.notGenerated}</AlertDescription>
            </Alert>
          )}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">{P.editTitle}</CardTitle>
            </CardHeader>
            <CardContent className="space-y-5">
              <div className="grid gap-4 sm:grid-cols-2">
                <div className="space-y-2">
                  <Label htmlFor={industryID}>{P.industry}</Label>
                  <Input
                    id={industryID}
                    value={draft.industry}
                    onChange={(event) =>
                      setDraft({ ...draft, industry: event.target.value })
                    }
                    disabled={saving || Boolean(undoingID)}
                    autoComplete="organization-title"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor={occupationID}>{P.occupation}</Label>
                  <Input
                    id={occupationID}
                    value={draft.occupation}
                    onChange={(event) =>
                      setDraft({ ...draft, occupation: event.target.value })
                    }
                    disabled={saving || Boolean(undoingID)}
                    autoComplete="organization-title"
                  />
                </div>
              </div>

              <div className="space-y-3">
                <div>
                  <Label htmlFor={tagInputID}>{P.tags}</Label>
                  <p id={`${tagInputID}-hint`} className="mt-1 text-xs text-muted-foreground">
                    {P.tagHint}
                  </p>
                </div>
                {draft.tags.length > 0 && (
                  <div className="flex flex-wrap gap-2" aria-label={P.currentTags}>
                    {draft.tags.map((tag) => (
                      <Badge key={tag} variant="secondary" className="gap-1 pr-1">
                        <span>{tag}</span>
                        <button
                          type="button"
                          className="rounded-sm p-0.5 outline-none hover:bg-foreground/10 focus-visible:ring-2 focus-visible:ring-ring"
                          onClick={() => removeTag(tag)}
                          aria-label={fmt(P.removeTag, { tag })}
                          disabled={saving || Boolean(undoingID)}
                        >
                          <X className="size-3" />
                        </button>
                      </Badge>
                    ))}
                  </div>
                )}
                <div className="flex flex-col gap-2 sm:flex-row">
                  <Input
                    id={tagInputID}
                    value={tagInput}
                    onChange={(event) => setTagInput(event.target.value)}
                    onKeyDown={(event) => {
                      if (event.key === "Enter" || event.key === ",") {
                        event.preventDefault();
                        addPendingTags();
                      }
                    }}
                    onBlur={() => {
                      if (tagInput.trim()) addPendingTags();
                    }}
                    aria-describedby={`${tagInputID}-hint${tagValidation ? ` ${tagInputID}-error` : ""}`}
                    aria-invalid={tagValidation ? true : undefined}
                    placeholder={P.tagPlaceholder}
                    disabled={saving || Boolean(undoingID)}
                  />
                  <Button
                    type="button"
                    variant="outline"
                    onClick={addPendingTags}
                    disabled={!tagInput.trim() || saving || Boolean(undoingID)}
                  >
                    {P.addTag}
                  </Button>
                </div>
                {tagValidation && (
                  <p
                    id={`${tagInputID}-error`}
                    className="text-sm text-destructive"
                    role="alert"
                  >
                    {tagValidation}
                  </p>
                )}
              </div>

              <Separator />
              <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                <p className="text-xs text-muted-foreground">
                  {profile.updated_at
                    ? fmt(P.updatedAtValue, { time: fmtBeijing(profile.updated_at) })
                    : P.notSavedYet}
                </p>
                <div className="flex gap-2">
                  <Button
                    variant="outline"
                    onClick={() => {
                      setDraft(editableFrom(profile));
                      setTagInput("");
                      setMutationError("");
                      setConflict(false);
                    }}
                    disabled={!update || saving || Boolean(undoingID)}
                  >
                    {P.discard}
                  </Button>
                  <Button
                    onClick={() => void save()}
                    disabled={!update || Boolean(tagValidation) || saving || Boolean(undoingID)}
                  >
                    {saving ? (
                      <Loader2 className="size-4 animate-spin" />
                    ) : (
                      <Save className="size-4" />
                    )}
                    {saving ? P.saving : P.save}
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>

          <Collapsible open={summaryOpen} onOpenChange={setSummaryOpen}>
            <Card>
              <CardHeader className="pb-3">
                <CollapsibleTrigger className="flex w-full items-center justify-between rounded-md text-left outline-none focus-visible:ring-2 focus-visible:ring-ring">
                  <span className="flex items-center gap-2 font-heading text-base font-medium">
                    <FileText className="size-4" />
                    {P.systemExplanation}
                  </span>
                  <ChevronDown
                    className={cn(
                      "size-4 transition-transform",
                      summaryOpen && "rotate-180",
                    )}
                  />
                </CollapsibleTrigger>
              </CardHeader>
              <CollapsibleContent>
                <CardContent>
                  <p className="mb-3 text-xs text-muted-foreground">{P.summaryNote}</p>
                  {profile.summary ? (
                    <pre className="whitespace-pre-wrap font-sans text-sm leading-relaxed">
                      {profile.summary}
                    </pre>
                  ) : (
                    <p className="text-sm text-muted-foreground">{P.noSummary}</p>
                  )}
                </CardContent>
              </CollapsibleContent>
            </Card>
          </Collapsible>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="flex items-center gap-2 text-base">
                <Ban className="size-4" />
                {P.removedTags}
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="mb-3 text-xs text-muted-foreground">{P.removedNote}</p>
              {profile.removed_tags.length > 0 ? (
                <div className="flex flex-wrap gap-2">
                  {profile.removed_tags.map((tag) => (
                    <Badge key={tag} variant="outline" className="text-muted-foreground">
                      {tag}
                    </Badge>
                  ))}
                </div>
              ) : (
                <p className="text-sm text-muted-foreground">{P.noRemovedTags}</p>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="flex items-center gap-2 text-base">
                <History className="size-4" />
                {P.editHistory}
              </CardTitle>
            </CardHeader>
            <CardContent>
              {editsLoading ? (
                <div className="space-y-3" aria-label={P.loadingHistory}>
                  <Skeleton className="h-14 w-full" />
                  <Skeleton className="h-14 w-full" />
                </div>
              ) : editsError ? (
                <Alert variant="destructive" role="alert">
                  <AlertDescription>{editsError}</AlertDescription>
                </Alert>
              ) : edits.length === 0 ? (
                <p className="text-sm text-muted-foreground">{P.noEdits}</p>
              ) : (
                <ol className="space-y-3">
                  {edits.map((edit) => (
                    <li
                      key={edit.id}
                      className="flex flex-col gap-3 rounded-lg border p-3 sm:flex-row sm:items-start sm:justify-between"
                    >
                      <div className="min-w-0 space-y-1">
                        <p className="text-xs text-muted-foreground">
                          <Badge variant="outline" className="mr-2">
                            {edit.kind === "undo" ? P.kindUndo : P.kindEdit}
                          </Badge>
                          {fmt(P.editAt, {
                            time: fmtBeijing(edit.created_at),
                            actor: P.you,
                          })}
                        </p>
                        {edit.changes.length > 0 ? (
                          <ul className="space-y-1 text-sm">
                            {edit.changes.map((change, index) => (
                              <li key={`${change.field}-${index}`} className="break-words">
                                {changeLabel(change)}
                              </li>
                            ))}
                          </ul>
                        ) : (
                          <p className="text-sm text-muted-foreground">{P.noChangeDetails}</p>
                        )}
                      </div>
                      {edit.undoable && (
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => void undo(edit)}
                          disabled={saving || Boolean(undoingID)}
                        >
                          {undoingID === edit.id ? (
                            <Loader2 className="size-4 animate-spin" />
                          ) : (
                            <RotateCcw className="size-4" />
                          )}
                          {undoingID === edit.id ? P.undoing : P.undo}
                        </Button>
                      )}
                    </li>
                  ))}
                </ol>
              )}
            </CardContent>
          </Card>
        </>
      ) : null}
    </div>
  );
}

function ProfileSkeleton() {
  return (
    <div className="space-y-4" aria-busy="true">
      <Card>
        <CardContent className="space-y-4 py-6">
          <div className="grid gap-4 sm:grid-cols-2">
            {Array.from({ length: 2 }).map((_, index) => (
              <div key={index} className="space-y-2">
                <Skeleton className="h-3 w-16" />
                <Skeleton className="h-8 w-full" />
              </div>
            ))}
          </div>
          <Skeleton className="h-20 w-full" />
        </CardContent>
      </Card>
      <Card>
        <CardContent className="space-y-3 py-6">
          <Skeleton className="h-4 w-32" />
          <Skeleton className="h-14 w-full" />
        </CardContent>
      </Card>
    </div>
  );
}

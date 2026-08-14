import { useCallback, useEffect, useId, useMemo, useRef, useState } from "react";
import { api, ApiError } from "@/shared/api/client";
import type {
  EditableProfileField,
  Profile as ProfileData,
  ProfileClaim,
  ProfileClaimActionRequest,
  ProfileClaimEvent,
  ProfileClaimField,
  ProfileClaimsResponse,
  ProfileEdit,
  ProfileEpochActionRequest,
  ProfileEpochActionResponse,
  UpdateProfileRequest,
} from "@/shared/api/client";
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
  CheckCircle2,
  ChevronDown,
  Database,
  EyeOff,
  FileText,
  History,
  Loader2,
  Pencil,
  Pin,
  RefreshCw,
  RotateCcw,
  Save,
  X,
} from "lucide-react";
import { fmt, useI18n } from "@/i18n";
import { profileEpochCopy } from "@/i18n/profile-epoch";
import { fmtBeijing } from "@/shared/utils/time";
import { cn } from "@/shared/utils/class-names";

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

type WithoutClaimAuthority<T> = T extends unknown
  ? Omit<T, "expected_epoch" | "expected_version">
  : never;

function evidenceRange(ref?: string): string {
  const match = ref?.match(/^feedbacks:\((\d+),(\d+)\]$/);
  if (!match) return "";
  const first = Number(match[1]) + 1;
  const last = Number(match[2]);
  return first <= last ? `${first}–${last}` : "";
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

function mergeProfileClaimPage(
  current: ProfileClaimsResponse,
  incoming: ProfileClaimsResponse,
): ProfileClaimsResponse {
  const claimsByID = new Map(
    current.claims.map((claim) => [claim.id, claim]),
  );
  for (const claim of incoming.claims) claimsByID.set(claim.id, claim);
  const eventsByID = new Map(
    current.events.map((event) => [event.id, event]),
  );
  for (const event of incoming.events) eventsByID.set(event.id, event);
  return {
    ...current,
    claims: [...claimsByID.values()],
    events: [...eventsByID.values()],
    events_has_more: incoming.events_has_more === true,
    events_next_cursor: incoming.events_next_cursor,
  };
}

export function retireProfileClaims(
  _current: ProfileClaimsResponse,
  response: ProfileEpochActionResponse,
): ProfileClaimsResponse {
  return {
    profile_epoch: response.profile_epoch,
    version: response.version,
    restore_allowed: response.restore_allowed,
    claims: [],
    events: [],
    events_has_more: false,
  };
}

export default function Profile() {
  const { t, locale } = useI18n();
  const P = t.app.profile;
  const E = profileEpochCopy(locale);
  const industryID = useId();
  const occupationID = useId();
  const tagInputID = useId();
  const loadFailedRef = useRef(t.app.common.loadFailed);
  loadFailedRef.current = t.app.common.loadFailed;
  const saveIntent = useRef<{ signature: string; key: string } | null>(null);
  const claimIntents = useRef(new Map<string, string>());
  const epochIntent = useRef<{ signature: string; key: string } | null>(null);
  const epochResetTriggerRef = useRef<HTMLButtonElement | null>(null);
  const epochRestoreTriggerRef = useRef<HTMLButtonElement | null>(null);
  const epochCancelRef = useRef<HTMLButtonElement | null>(null);
  const conflictReloadRef = useRef<HTMLButtonElement | null>(null);
  const epochNoticeRef = useRef<HTMLDivElement | null>(null);
  const epochReturnFocus = useRef<"" | "reset" | "restore">("");
  const shouldRestoreEpochFocus = useRef(false);
  const claimLoadEpoch = useRef(0);
  const [profile, setProfile] = useState<ProfileData | null>(null);
  const [draft, setDraft] = useState<EditableProfile | null>(null);
  const [tagInput, setTagInput] = useState("");
  const [edits, setEdits] = useState<ProfileEdit[]>([]);
  const [claims, setClaims] = useState<ProfileClaimsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [editsLoading, setEditsLoading] = useState(true);
  const [claimsLoading, setClaimsLoading] = useState(true);
  const [olderClaimsLoading, setOlderClaimsLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [claimActionID, setClaimActionID] = useState("");
  const [epochAction, setEpochAction] = useState<"" | "reset" | "restore">("");
  const [epochConfirm, setEpochConfirm] = useState<"" | "reset" | "restore">("");
  const [epochNotice, setEpochNotice] = useState("");
  const [correctingClaimID, setCorrectingClaimID] = useState("");
  const [correctionValue, setCorrectionValue] = useState("");
  const [loadError, setLoadError] = useState("");
  const [editsError, setEditsError] = useState("");
  const [claimsError, setClaimsError] = useState("");
  const [olderClaimsError, setOlderClaimsError] = useState("");
  const [mutationError, setMutationError] = useState("");
  const [conflict, setConflict] = useState(false);
  const [saved, setSaved] = useState(false);
  const [notGenerated, setNotGenerated] = useState(false);
  const [claimsNotInitialized, setClaimsNotInitialized] = useState(false);
  const [summaryOpen, setSummaryOpen] = useState(false);
  const [nonce, setNonce] = useState(0);

  const reload = useCallback(() => {
    setNonce((n) => n + 1);
  }, []);

  const replaceInitialClaims = useCallback(async (epoch: number) => {
    try {
      const response = await api.profileClaims();
      if (claimLoadEpoch.current !== epoch) return;
      setClaimsNotInitialized(false);
      setClaims(response);
    } catch (err) {
      if (claimLoadEpoch.current !== epoch) return;
      if (err instanceof ApiError && err.status === 404) {
        setClaimsNotInitialized(true);
        setClaims({
          version: 0,
          claims: [],
          events: [],
          events_has_more: false,
        });
        return;
      }
      setClaimsNotInitialized(false);
      setClaims(null);
      setClaimsError(err instanceof ApiError ? err.message : loadFailedRef.current);
    } finally {
      if (claimLoadEpoch.current === epoch) setClaimsLoading(false);
    }
  }, []);

  useEffect(() => {
    let alive = true;
    const claimsEpoch = claimLoadEpoch.current + 1;
    claimLoadEpoch.current = claimsEpoch;
    setLoading(true);
    setEditsLoading(true);
    setClaimsLoading(true);
    setOlderClaimsLoading(false);
    setLoadError("");
    setEditsError("");
    setClaimsError("");
    setOlderClaimsError("");
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

    void replaceInitialClaims(claimsEpoch);

    return () => {
      alive = false;
      if (claimLoadEpoch.current === claimsEpoch) {
        claimLoadEpoch.current += 1;
      }
    };
  }, [nonce, replaceInitialClaims]);

  const effectiveDraft = useMemo(() => {
    if (!draft || !tagInput.trim()) return draft;
    const incoming = tagParts(tagInput).map(cleanTag);
    return { ...draft, tags: uniqueTags([...draft.tags, ...incoming]) };
  }, [draft, tagInput]);
  const claimsByID = useMemo(
    () => new Map((claims?.claims ?? []).map((claim) => [claim.id, claim])),
    [claims?.claims],
  );
  const update = useMemo(
    () => (profile && effectiveDraft ? buildUpdate(profile, effectiveDraft) : null),
    [effectiveDraft, profile],
  );
  const epochAuthorityReady =
    claims !== null &&
    Number.isSafeInteger(claims.profile_epoch) &&
    Number(claims.profile_epoch) >= 0 &&
    Number.isSafeInteger(claims.version) &&
    claims.version >= 0;
  const claimMutationsLocked =
    conflict ||
    claimsLoading ||
    Boolean(claimActionID) ||
    Boolean(epochAction) ||
    Boolean(epochConfirm);
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
    epochIntent.current = null;
    shouldRestoreEpochFocus.current = false;
    epochReturnFocus.current = "";
    setEpochConfirm("");
    setEpochNotice("");
    reload();
  }

  function openEpochConfirmation(action: "reset" | "restore") {
    epochReturnFocus.current = action;
    shouldRestoreEpochFocus.current = false;
    setEpochConfirm(action);
  }

  function closeEpochConfirmation() {
    shouldRestoreEpochFocus.current = true;
    setEpochConfirm("");
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

  async function applyClaimAction(
    input: WithoutClaimAuthority<ProfileClaimActionRequest>,
    intentID: string,
  ) {
    if (
      !claims ||
      !epochAuthorityReady ||
      conflict ||
      claimsLoading ||
      claimActionID ||
      epochAction ||
      epochConfirm
    ) {
      return;
    }
    if (update && !window.confirm(P.confirmClaimDirty)) return;
    const request = {
      ...input,
      expected_epoch: claims.profile_epoch as number,
      expected_version: claims.version,
    } as ProfileClaimActionRequest;
    const signature = JSON.stringify(request);
    setClaimActionID(intentID);
    setMutationError("");
    setConflict(false);
    setSaved(false);
    try {
      let key = claimIntents.current.get(signature);
      if (!key) {
        key = idempotencyKey("profile-claim");
        claimIntents.current.set(signature, key);
      }
      await api.applyProfileClaimAction(request, key);
      claimIntents.current.delete(signature);
      setCorrectingClaimID("");
      setCorrectionValue("");
      setSaved(true);
      reload();
    } catch (err) {
      if (err instanceof ApiError && err.status === 409) {
        claimIntents.current.delete(signature);
        setConflict(true);
      } else {
        setMutationError(err instanceof ApiError ? err.message : P.claimActionFailed);
      }
    } finally {
      setClaimActionID("");
    }
  }

  async function applyEpochAction(action: "reset" | "restore") {
    if (
      !claims ||
      !epochAuthorityReady ||
      epochAction ||
      conflict ||
      claimsLoading ||
      claimActionID ||
      update ||
      (action === "restore" && claims.restore_allowed !== true)
    ) {
      return;
    }
    const authority = {
      expected_epoch: claims.profile_epoch as number,
      expected_version: claims.version,
    };
    const request: ProfileEpochActionRequest =
      action === "reset"
        ? {
            ...authority,
            action: "reset",
            scope: "history_learning",
          }
        : {
            ...authority,
            action: "restore",
          };
    const signature = JSON.stringify(request);
    if (!epochIntent.current || epochIntent.current.signature !== signature) {
      epochIntent.current = {
        signature,
        key: idempotencyKey(`profile-epoch-${action}`),
      };
    }

    setEpochAction(action);
    setMutationError("");
    setConflict(false);
    setSaved(false);
    setEpochNotice("");
    try {
      const response = await api.applyProfileEpochAction(
        request,
        epochIntent.current.key,
      );
      if (response.action !== action) throw new Error("profile epoch action mismatch");
      epochIntent.current = null;
      shouldRestoreEpochFocus.current = false;
      epochReturnFocus.current = "";
      setEpochConfirm("");
      setEpochNotice(action === "reset" ? E.resetDone : E.restoreDone);
      setProfile(response.profile);
      setDraft(editableFrom(response.profile));
      setTagInput("");
      // Invalidate any old-epoch pagination synchronously with the authority
      // transition. Waiting for reload()'s effect would leave a race where an
      // in-flight old page could merge retired facts back into the new epoch.
      claimLoadEpoch.current += 1;
      setOlderClaimsLoading(false);
      setOlderClaimsError("");
      // Retire the old epoch's claim/event projection immediately. Keeping
      // those rows while swapping only their authority would let a fast click
      // address an inactive-epoch claim with the new epoch token.
      setClaims((current) =>
        current ? retireProfileClaims(current, response) : current,
      );
      // The action response intentionally does not duplicate current claims or
      // events. Reload all three profile projections after committing the
      // returned authority instead of pretending the old list is current.
      reload();
    } catch (err) {
      if (err instanceof ApiError && err.status === 409) {
        epochIntent.current = null;
        shouldRestoreEpochFocus.current = false;
        epochReturnFocus.current = "";
        setEpochConfirm("");
        setConflict(true);
      } else {
        setMutationError(err instanceof ApiError ? err.message : E.epochActionFailed);
      }
    } finally {
      setEpochAction("");
    }
  }

  useEffect(() => {
    if (epochConfirm) {
      epochCancelRef.current?.focus();
      const onKeyDown = (event: KeyboardEvent) => {
        if (event.key !== "Escape" || epochAction) return;
        event.preventDefault();
        closeEpochConfirmation();
      };
      document.addEventListener("keydown", onKeyDown);
      return () => document.removeEventListener("keydown", onKeyDown);
    }
    if (!shouldRestoreEpochFocus.current) return;
    shouldRestoreEpochFocus.current = false;
    const target =
      epochReturnFocus.current === "restore"
        ? epochRestoreTriggerRef.current
        : epochResetTriggerRef.current;
    epochReturnFocus.current = "";
    target?.focus();
  }, [epochAction, epochConfirm]);

  useEffect(() => {
    if (conflict) conflictReloadRef.current?.focus();
  }, [conflict]);

  useEffect(() => {
    if (epochNotice) epochNoticeRef.current?.focus();
  }, [epochNotice]);

  async function loadOlderClaimEvents() {
    const cursor = claims?.events_next_cursor;
    if (
      !claims ||
      claims.events_has_more !== true ||
      !cursor ||
      olderClaimsLoading
    ) {
      return;
    }
    const epoch = claimLoadEpoch.current;
    setOlderClaimsLoading(true);
    setOlderClaimsError("");
    try {
      const page = await api.profileClaims(cursor);
      if (claimLoadEpoch.current !== epoch) return;
      setClaims((current) =>
        current ? mergeProfileClaimPage(current, page) : current,
      );
    } catch (err) {
      if (claimLoadEpoch.current !== epoch) return;
      if (err instanceof ApiError && err.status === 409) {
        const restartEpoch = claimLoadEpoch.current + 1;
        claimLoadEpoch.current = restartEpoch;
        setOlderClaimsLoading(false);
        setConflict(true);
        setClaims(null);
        setClaimsError("");
        setClaimsLoading(true);
        setOlderClaimsError("");
        await replaceInitialClaims(restartEpoch);
        return;
      }
      setOlderClaimsError(
        err instanceof ApiError ? err.message : loadFailedRef.current,
      );
    } finally {
      if (claimLoadEpoch.current === epoch) setOlderClaimsLoading(false);
    }
  }

  function claimFieldLabel(field: ProfileClaimField): string {
    return {
      industry: P.industry,
      occupation: P.occupation,
      tag: P.claimFieldTag,
      summary: P.claimFieldSummary,
    }[field];
  }

  function claimSource(claim: ProfileClaim): string {
    if (claim.source.state === "manual") return P.sourceManual;
    if (claim.source.state === "source_unavailable") return P.sourceUnavailable;
    const range =
      claim.source.ref_type === "feedback_range"
        ? evidenceRange(claim.source.ref)
        : "";
    return range ? fmt(P.sourceEvidenceRange, { range }) : P.sourceEvidence;
  }

  function claimEventLabel(event: ProfileClaimEvent): string {
    return {
      correct: P.claimKindCorrect,
      suppress: P.claimKindSuppress,
      pin: P.claimKindPin,
      revoke: P.claimKindRevoke,
    }[event.kind];
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
          disabled={loading || saving || Boolean(claimActionID) || Boolean(epochAction)}
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
            <Button
              ref={conflictReloadRef}
              size="sm"
              variant="outline"
              onClick={requestReload}
            >
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
      {epochNotice && (
        <Alert ref={epochNoticeRef} role="status" tabIndex={-1}>
          <AlertDescription>{epochNotice}</AlertDescription>
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
          {notGenerated && claimsNotInitialized && (
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
                    disabled={saving}
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
                    disabled={saving}
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
                          disabled={saving}
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
                    disabled={saving}
                  />
                  <Button
                    type="button"
                    variant="outline"
                    onClick={addPendingTags}
                    disabled={!tagInput.trim() || saving}
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
                    disabled={!update || saving}
                  >
                    {P.discard}
                  </Button>
                  <Button
                    onClick={() => void save()}
                    disabled={!update || Boolean(tagValidation) || saving}
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
          )}

          {!notGenerated && (
            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="flex flex-wrap items-center gap-2 text-base">
                  <RotateCcw className="size-4" />
                  {E.learningResetTitle}
                  {epochAuthorityReady && (
                    <Badge variant="outline">
                      {fmt(E.currentEpoch, { epoch: claims.profile_epoch as number })}
                    </Badge>
                  )}
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="space-y-1 text-sm text-muted-foreground">
                  <p>{E.learningResetNote}</p>
                  <p>{E.epochAuditNote}</p>
                </div>

                {claimsLoading ? (
                  <Skeleton className="h-10 w-full" />
                ) : !epochAuthorityReady ? (
                  <Alert role="status">
                    <AlertDescription className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                      <span>{E.epochUnavailable}</span>
                      <Button size="sm" variant="outline" onClick={requestReload}>
                        <RefreshCw className="size-4" />
                        {P.reload}
                      </Button>
                    </AlertDescription>
                  </Alert>
                ) : epochConfirm ? (
                  <div
                    role="alertdialog"
                    aria-label={
                      epochConfirm === "reset" ? E.resetConfirmTitle : E.restoreConfirmTitle
                    }
                    className="space-y-4 rounded-lg border border-destructive/40 bg-destructive/5 p-4"
                  >
                    <div className="space-y-1">
                      <p className="font-medium">
                        {epochConfirm === "reset"
                          ? E.resetConfirmTitle
                          : E.restoreConfirmTitle}
                      </p>
                      <p className="text-sm text-muted-foreground">
                        {epochConfirm === "reset"
                          ? E.resetConfirmNote
                          : E.restoreConfirmNote}
                      </p>
                    </div>
                    <div className="flex flex-col-reverse gap-2 sm:flex-row sm:justify-end">
                      <Button
                        type="button"
                        variant="outline"
                        ref={epochCancelRef}
                        onClick={closeEpochConfirmation}
                        disabled={Boolean(epochAction)}
                      >
                        {E.cancelEpochAction}
                      </Button>
                      <Button
                        type="button"
                        variant={epochConfirm === "reset" ? "destructive" : "default"}
                        onClick={() => void applyEpochAction(epochConfirm)}
                        disabled={
                          Boolean(epochAction) ||
                          Boolean(update) ||
                          conflict ||
                          claimsLoading
                        }
                      >
                        {epochAction === epochConfirm && (
                          <Loader2 className="size-4 animate-spin" />
                        )}
                        {epochConfirm === "reset"
                          ? E.confirmReset
                          : E.confirmRestore}
                      </Button>
                    </div>
                  </div>
                ) : (
                  <div className="space-y-3">
                    {update && (
                      <p className="text-sm text-destructive" role="status">
                        {E.epochDirty}
                      </p>
                    )}
                    <div className="flex flex-col gap-2 sm:flex-row sm:flex-wrap">
                      <Button
                        type="button"
                        variant="destructive"
                        ref={epochResetTriggerRef}
                        onClick={() => openEpochConfirmation("reset")}
                        disabled={
                          Boolean(update) ||
                          saving ||
                          Boolean(claimActionID) ||
                          Boolean(epochAction) ||
                          conflict ||
                          claimsLoading
                        }
                      >
                        <RotateCcw className="size-4" />
                        {E.resetLearning}
                      </Button>
                      {claims.restore_allowed === true && (
                        <Button
                          type="button"
                          variant="outline"
                          ref={epochRestoreTriggerRef}
                          onClick={() => openEpochConfirmation("restore")}
                          disabled={
                            Boolean(update) ||
                            saving ||
                            Boolean(claimActionID) ||
                            Boolean(epochAction) ||
                            conflict ||
                            claimsLoading
                          }
                        >
                          <History className="size-4" />
                          {E.restoreLearning}
                        </Button>
                      )}
                    </div>
                    <p className="text-xs text-muted-foreground">
                      {claims.restore_allowed === true
                        ? E.restoreAvailable
                        : E.restoreUnavailable}
                    </p>
                  </div>
                )}
              </CardContent>
            </Card>
          )}

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="flex items-center gap-2 text-base">
                <Database className="size-4" />
                {P.claimsTitle}
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <p className="text-xs text-muted-foreground">{P.claimsNote}</p>
              {claimsLoading ? (
                <div className="space-y-3" aria-label={P.loadingClaims}>
                  <Skeleton className="h-24 w-full" />
                  <Skeleton className="h-24 w-full" />
                </div>
              ) : claimsError ? (
                <Alert variant="destructive" role="alert">
                  <AlertDescription className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                    <span className="break-words">{claimsError}</span>
                    <Button size="sm" variant="outline" onClick={requestReload}>
                      <RefreshCw className="size-4" />
                      {P.reload}
                    </Button>
                  </AlertDescription>
                </Alert>
              ) : !claims || claims.claims.length === 0 ? (
                <p className="text-sm text-muted-foreground">{P.noClaims}</p>
              ) : (
                <ol className="space-y-3">
                  {claims.claims.map((claim) => {
                    const canCorrect = claim.active && epochAuthorityReady;
                    const isCorrecting = correctingClaimID === claim.id;
                    const correctionLimit =
                      claim.field === "summary" ? 240 : claim.field === "tag" ? 20 : 200;
                    const correctionTooLong =
                      Array.from(correctionValue.trim()).length > correctionLimit;
                    return (
                      <li
                        key={claim.id}
                        className="min-w-0 space-y-3 rounded-lg border p-3"
                      >
                        <div className="flex min-w-0 flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                          <div className="min-w-0 space-y-2">
                            <div className="flex flex-wrap items-center gap-2">
                              <Badge variant="outline">
                                {claimFieldLabel(claim.field)}
                              </Badge>
                              <Badge variant={claim.active ? "default" : "secondary"}>
                                {claim.active ? P.claimActive : P.claimInactive}
                              </Badge>
                              {claim.pinned && (
                                <Badge variant="secondary">
                                  <Pin className="mr-1 size-3" />
                                  {P.claimPinned}
                                </Badge>
                              )}
                            </div>
                            <p className="break-words text-sm font-medium">{claim.value}</p>
                            <p className="break-words text-xs text-muted-foreground">
                              {claimSource(claim)}
                            </p>
                          </div>
                          {claim.active && epochAuthorityReady && (
                            <div className="flex w-full flex-wrap gap-2 sm:w-auto sm:shrink-0">
                              {canCorrect && (
                                <Button
                                  size="sm"
                                  variant="outline"
                                  onClick={() => {
                                    setCorrectingClaimID(isCorrecting ? "" : claim.id);
                                    setCorrectionValue(isCorrecting ? "" : claim.value);
                                  }}
                                  disabled={claimMutationsLocked}
                                >
                                  <Pencil className="size-4" />
                                  {P.claimCorrect}
                                </Button>
                              )}
                              <Button
                                size="sm"
                                variant="outline"
                                onClick={() =>
                                  void applyClaimAction(
                                    { action: "suppress", claim_id: claim.id },
                                    `suppress-${claim.id}`,
                                  )
                                }
                                disabled={claimMutationsLocked}
                              >
                                {claimActionID === `suppress-${claim.id}` ? (
                                  <Loader2 className="size-4 animate-spin" />
                                ) : (
                                  <EyeOff className="size-4" />
                                )}
                                {P.claimSuppress}
                              </Button>
                              {!claim.pinned && (
                                <Button
                                  size="sm"
                                  variant="outline"
                                  onClick={() =>
                                    void applyClaimAction(
                                      { action: "pin", claim_id: claim.id },
                                      `pin-${claim.id}`,
                                    )
                                  }
                                  disabled={claimMutationsLocked}
                                >
                                  {claimActionID === `pin-${claim.id}` ? (
                                    <Loader2 className="size-4 animate-spin" />
                                  ) : (
                                    <Pin className="size-4" />
                                  )}
                                  {P.claimPin}
                                </Button>
                              )}
                            </div>
                          )}
                        </div>
                        {isCorrecting && canCorrect && (
                          <div className="space-y-2 rounded-md bg-muted/40 p-3">
                            <Label htmlFor={`claim-correction-${claim.id}`}>
                              {fmt(P.claimCorrectionLabel, {
                                field: claimFieldLabel(claim.field),
                              })}
                            </Label>
                            <div className="flex min-w-0 flex-col gap-2 sm:flex-row">
                              <Input
                                id={`claim-correction-${claim.id}`}
                                value={correctionValue}
                                onChange={(event) => setCorrectionValue(event.target.value)}
                                aria-invalid={correctionTooLong || undefined}
                                maxLength={correctionLimit}
                                disabled={claimMutationsLocked}
                              />
                              <Button
                                onClick={() =>
                                  void applyClaimAction(
                                    {
                                      action: "correct",
                                      claim_id: claim.id,
                                      value: correctionValue.trim(),
                                    },
                                    `correct-${claim.id}`,
                                  )
                                }
                                disabled={
                                  !correctionValue.trim() ||
                                  correctionTooLong ||
                                  claimMutationsLocked
                                }
                              >
                                {claimActionID === `correct-${claim.id}` ? (
                                  <Loader2 className="size-4 animate-spin" />
                                ) : (
                                  <CheckCircle2 className="size-4" />
                                )}
                                {P.claimConfirmCorrection}
                              </Button>
                            </div>
                            <p
                              className={cn(
                                "text-xs text-muted-foreground",
                                correctionTooLong && "text-destructive",
                              )}
                            >
                              {correctionTooLong
                                ? fmt(P.claimCorrectionTooLong, {
                                    limit: correctionLimit,
                                  })
                                : fmt(P.claimCorrectionHint, {
                                    limit: correctionLimit,
                                  })}
                            </p>
                          </div>
                        )}
                      </li>
                    );
                  })}
                </ol>
              )}

              <Separator />
              <div className="space-y-3">
                <h3 className="flex items-center gap-2 text-sm font-medium">
                  <History className="size-4" />
                  {P.claimHistory}
                </h3>
                {!claimsLoading && claims && claims.events.length === 0 ? (
                  <p className="text-sm text-muted-foreground">{P.noClaimEvents}</p>
                ) : claims && claims.events.length > 0 ? (
                  <ol className="space-y-2">
                    {claims.events.map((event) => {
                      const target = event.target_claim_id
                        ? claimsByID.get(event.target_claim_id)
                        : undefined;
                      return (
                        <li
                          key={event.id}
                          className="flex min-w-0 flex-col gap-3 rounded-lg border p-3 sm:flex-row sm:items-center sm:justify-between"
                        >
                          <div className="min-w-0">
                            <p className="flex flex-wrap items-center gap-2 text-sm">
                              <Badge variant="outline">{claimEventLabel(event)}</Badge>
                              {event.revoked && (
                                <Badge variant="secondary">{P.claimRevoked}</Badge>
                              )}
                              {target && (
                                <span className="break-words">
                                  {claimFieldLabel(target.field)} · {target.value}
                                </span>
                              )}
                            </p>
                            <p className="mt-1 text-xs text-muted-foreground">
                              {fmtBeijing(event.created_at)}
                            </p>
                          </div>
                          {event.revocable && epochAuthorityReady && (
                            <Button
                              size="sm"
                              variant="outline"
                              onClick={() =>
                                void applyClaimAction(
                                  { action: "revoke", event_id: event.id },
                                  `revoke-${event.id}`,
                                )
                              }
                              disabled={claimMutationsLocked}
                            >
                              {claimActionID === `revoke-${event.id}` ? (
                                <Loader2 className="size-4 animate-spin" />
                              ) : (
                                <RotateCcw className="size-4" />
                              )}
                              {P.claimRevoke}
                            </Button>
                          )}
                        </li>
                      );
                    })}
                  </ol>
                ) : null}
                {olderClaimsError && (
                  <Alert variant="destructive" role="alert">
                    <AlertDescription className="break-words">
                      {olderClaimsError}
                    </AlertDescription>
                  </Alert>
                )}
                {claims?.events_has_more && claims.events_next_cursor && (
                  <div className="flex min-w-0 flex-col items-stretch gap-2 sm:flex-row sm:justify-center">
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      className="w-full sm:w-auto"
                      onClick={() => void loadOlderClaimEvents()}
                      disabled={olderClaimsLoading || Boolean(claimActionID)}
                      aria-busy={olderClaimsLoading || undefined}
                    >
                      {olderClaimsLoading && (
                        <Loader2 className="size-4 animate-spin" />
                      )}
                      {t.app.common.loadMore}
                    </Button>
                  </div>
                )}
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
                {P.legacyEditHistory}
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <p className="text-xs text-muted-foreground">{P.legacyEditNote}</p>
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

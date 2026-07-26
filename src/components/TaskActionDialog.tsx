import { useCallback, useEffect, useRef, useState } from "react";
import { Loader2 } from "lucide-react";
import { api, ApiError } from "@/api";
import type { TaskActionPreview, TaskActionStatus } from "@/api";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import {
  normalizeTaskActionField,
  taskActionPayloadHash,
} from "@/lib/task-action-canonical";
import {
  TASK_ACTION_STORAGE_PREFIX,
  TASK_PROPOSAL_STORAGE_PREFIX,
} from "@/lib/task-action-session";

export interface TaskActionDialogLabels {
  title: string;
  description: string;
  placeholder: string;
  inputLabel: string;
  draft: string;
  drafting: string;
  preview: string;
  confirm: string;
  confirming: string;
  cancel: string;
  close: string;
  waiting: string;
  checkAgain: string;
  requestFailed: string;
  resultStatus: string;
  invalidProposal: string;
  status: Record<string, string>;
}

interface TaskActionDialogProps {
  open: boolean;
  actorScope: string;
  taskID?: string;
  labels: TaskActionDialogLabels;
  onClose: (status?: TaskActionStatus) => void;
  onComplete: (status: TaskActionStatus) => void;
}

interface StoredTaskAction {
  version: 1;
  id: string;
  kind: "create" | "edit";
  scopeTaskID?: string;
  status: string;
  terminal: boolean;
  resultTaskID?: string;
  notified: boolean;
}

interface StoredProposalAttempt {
  version: 1;
  requestID: string;
  payloadHash: string;
}

const POLL_INTERVAL_MS = 1500;
const POLL_LIMIT = 80;
const POLL_RETRY_MS = 15_000;
function storageKey(actorScope: string, taskID?: string): string {
  const actor = encodeURIComponent(actorScope);
  return taskID
    ? `${TASK_ACTION_STORAGE_PREFIX}:${actor}:edit:${encodeURIComponent(taskID)}`
    : `${TASK_ACTION_STORAGE_PREFIX}:${actor}:create`;
}

function proposalStorageKey(actorScope: string, taskID?: string): string {
  const actor = encodeURIComponent(actorScope);
  return taskID
    ? `${TASK_PROPOSAL_STORAGE_PREFIX}:${actor}:edit:${encodeURIComponent(taskID)}`
    : `${TASK_PROPOSAL_STORAGE_PREFIX}:${actor}:create`;
}

function readStoredAction(key: string): StoredTaskAction | null {
  if (typeof window === "undefined") return null;
  try {
    const parsed = JSON.parse(window.sessionStorage.getItem(key) ?? "null") as
      | Partial<StoredTaskAction>
      | null;
    if (
      parsed?.version !== 1 ||
      typeof parsed.id !== "string" ||
      (parsed.kind !== "create" && parsed.kind !== "edit") ||
      typeof parsed.status !== "string" ||
      typeof parsed.terminal !== "boolean" ||
      typeof parsed.notified !== "boolean"
    ) {
      return null;
    }
    return parsed as StoredTaskAction;
  } catch {
    return null;
  }
}

function writeStoredAction(key: string, action: StoredTaskAction): void {
  if (typeof window === "undefined") return;
  try {
    // Only the opaque action ID and minimal lifecycle metadata are retained.
    // Proposal text, summaries, replies, and error bodies never enter storage.
    window.sessionStorage.setItem(key, JSON.stringify(action));
  } catch {
    // Storage can be disabled or full. Polling still works for this mount.
  }
}

function removeStoredAction(key: string): void {
  if (typeof window === "undefined") return;
  try {
    window.sessionStorage.removeItem(key);
  } catch {
    // The in-memory state can still be acknowledged even if storage is blocked.
  }
}

function readProposalAttempt(key: string): StoredProposalAttempt | null {
  if (typeof window === "undefined") return null;
  try {
    const parsed = JSON.parse(window.sessionStorage.getItem(key) ?? "null") as
      | Partial<StoredProposalAttempt>
      | null;
    if (
      parsed?.version !== 1 ||
      typeof parsed.requestID !== "string" ||
      typeof parsed.payloadHash !== "string"
    ) {
      return null;
    }
    return parsed as StoredProposalAttempt;
  } catch {
    return null;
  }
}

function writeProposalAttempt(
  key: string,
  attempt: StoredProposalAttempt,
): void {
  if (typeof window === "undefined") return;
  try {
    window.sessionStorage.setItem(key, JSON.stringify(attempt));
  } catch {
    // The request still has in-memory idempotency for this attempt.
  }
}

function newRequestID(): string {
  return globalThis.crypto.randomUUID();
}

function statusFromStored(action: StoredTaskAction): TaskActionStatus {
  return {
    id: action.id,
    kind: action.kind,
    status: action.status,
    terminal: action.terminal,
    ...(action.resultTaskID ? { task_id: action.resultTaskID } : {}),
  };
}

export default function TaskActionDialog({
  open,
  actorScope,
  taskID,
  labels,
  onClose,
  onComplete,
}: TaskActionDialogProps) {
  const [input, setInput] = useState("");
  const [reply, setReply] = useState("");
  const [preview, setPreview] = useState<TaskActionPreview | null>(null);
  const [status, setStatus] = useState<TaskActionStatus | null>(null);
  const [acceptedActionID, setAcceptedActionID] = useState("");
  const [error, setError] = useState("");
  const [waiting, setWaiting] = useState(false);
  const [busy, setBusy] = useState<
    "draft" | "confirm" | "cancel" | "poll" | ""
  >("");
  const pollGeneration = useRef(0);
  const retryTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pollRef = useRef<(actionID: string) => Promise<void>>(async () => {});
  const previousOpen = useRef(open);
  const labelsRef = useRef(labels);
  const onCompleteRef = useRef(onComplete);
  const normalizedTaskID = normalizeTaskActionField(taskID ?? "");
  const actionKind: StoredTaskAction["kind"] = normalizedTaskID
    ? "edit"
    : "create";
  const actionStorageKey = storageKey(
    actorScope,
    normalizedTaskID || undefined,
  );
  const proposalAttemptStorageKey = proposalStorageKey(
    actorScope,
    normalizedTaskID || undefined,
  );

  useEffect(() => {
    labelsRef.current = labels;
    onCompleteRef.current = onComplete;
  }, [labels, onComplete]);

  function clearRetryTimer() {
    if (retryTimer.current !== null) {
      clearTimeout(retryTimer.current);
      retryTimer.current = null;
    }
  }

  function reset({ removeStored = false } = {}) {
    pollGeneration.current += 1;
    clearRetryTimer();
    if (removeStored) removeStoredAction(actionStorageKey);
    setInput("");
    setReply("");
    setPreview(null);
    setStatus(null);
    setAcceptedActionID("");
    setError("");
    setWaiting(false);
    setBusy("");
  }

  function persistAccepted(
    actionID: string,
    current?: TaskActionStatus,
  ): StoredTaskAction {
    const previous = readStoredAction(actionStorageKey);
    const next: StoredTaskAction = {
      version: 1,
      id: actionID,
      kind: actionKind,
      ...(normalizedTaskID ? { scopeTaskID: normalizedTaskID } : {}),
      status: current?.status ?? previous?.status ?? "accepted",
      terminal: current?.terminal ?? previous?.terminal ?? false,
      ...(current?.task_id
        ? { resultTaskID: current.task_id }
        : previous?.resultTaskID
          ? { resultTaskID: previous.resultTaskID }
          : {}),
      notified: previous?.notified ?? false,
    };
    writeStoredAction(actionStorageKey, next);
    return next;
  }

  const poll = useCallback(
    async (actionID: string) => {
      clearRetryTimer();
      const generation = ++pollGeneration.current;
      setAcceptedActionID(actionID);
      setBusy("poll");
      setWaiting(false);

      for (let attempt = 0; attempt < POLL_LIMIT; attempt += 1) {
        if (generation !== pollGeneration.current) return;
        try {
          const current = await api.taskActionStatus(actionID);
          if (generation !== pollGeneration.current) return;
          setError("");
          setStatus(current);
          if (current.message) setReply(current.message);
          const stored = persistAccepted(actionID, current);
          if (!current.terminal && current.status === "pending") {
            // A confirm/cancel request can fail before reaching the server.
            // The durable read is authoritative: pending means the proposal
            // still awaits a user decision, so restore the buttons instead of
            // polling forever or discarding its known action identity.
            setPreview((previous) => previous ?? {
              id: current.id,
              kind: current.kind,
              ...(current.kind === "edit" && current.task_id
                ? { task_id: current.task_id }
                : {}),
              summary:
                current.summary ??
                current.message ??
                labelsRef.current.preview,
            });
            setAcceptedActionID("");
            setBusy("");
            setWaiting(false);
            return;
          }
          if (current.terminal) {
            setBusy("");
            setWaiting(false);
            if (!stored.notified) {
              onCompleteRef.current(current);
              writeStoredAction(actionStorageKey, {
                ...stored,
                notified: true,
              });
            }
            return;
          }
        } catch (err) {
          if (generation !== pollGeneration.current) return;
          if (err instanceof ApiError && err.status === 404) {
            removeStoredAction(actionStorageKey);
            setAcceptedActionID("");
            setStatus(null);
            setPreview(null);
            setBusy("");
            setWaiting(false);
            setError(err.message);
            return;
          }
          setError(
            err instanceof ApiError
              ? err.message
              : labelsRef.current.requestFailed,
          );
        }
        await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
      }

      if (generation !== pollGeneration.current) return;
      setBusy("");
      setWaiting(true);
      retryTimer.current = setTimeout(() => {
        retryTimer.current = null;
        if (generation === pollGeneration.current) {
          void pollRef.current(actionID);
        }
      }, POLL_RETRY_MS);
    },
    // taskID/actionStorageKey define the durable owner-scoped action slot.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [actionKind, actionStorageKey, normalizedTaskID],
  );

  useEffect(() => {
    pollRef.current = poll;
  }, [poll]);

  useEffect(() => {
    pollGeneration.current += 1;
    clearRetryTimer();
    setInput("");
    setReply("");
    setPreview(null);
    setError("");
    setWaiting(false);
    setBusy("");

    const stored = readStoredAction(actionStorageKey);
    if (
      !stored ||
      stored.kind !== actionKind ||
      (stored.scopeTaskID ?? "") !== normalizedTaskID
    ) {
      setStatus(null);
      setAcceptedActionID("");
      return () => {
        pollGeneration.current += 1;
        clearRetryTimer();
      };
    }

    setAcceptedActionID(stored.id);
    setStatus(statusFromStored(stored));
    void pollRef.current(stored.id);

    return () => {
      pollGeneration.current += 1;
      clearRetryTimer();
    };
  }, [actionKind, actionStorageKey, normalizedTaskID]);

  useEffect(() => {
    const reopened = open && !previousOpen.current;
    previousOpen.current = open;
    if (
      reopened &&
      acceptedActionID &&
      status?.terminal !== true &&
      waiting &&
      busy === ""
    ) {
      void pollRef.current(acceptedActionID);
    }
  }, [acceptedActionID, busy, open, status?.terminal, waiting]);

  function close() {
    const terminalStatus = status?.terminal ? status : undefined;
    const retained = readStoredAction(actionStorageKey);
    if (terminalStatus || (!acceptedActionID && !retained)) {
      reset({ removeStored: terminalStatus !== undefined });
    }
    onClose(terminalStatus);
  }

  async function draft() {
    const text = normalizeTaskActionField(input);
    if (!text || busy || acceptedActionID) return;
    const generation = ++pollGeneration.current;
    setBusy("draft");
    setError("");
    setReply("");
    setPreview(null);
    setStatus(null);
    try {
      const payloadHash = await taskActionPayloadHash(
        actionKind,
        normalizedTaskID,
        text,
      );
      const previous = readProposalAttempt(proposalAttemptStorageKey);
      const requestID =
        previous?.payloadHash === payloadHash
          ? previous.requestID
          : `${newRequestID()}.${payloadHash}`;
      writeProposalAttempt(proposalAttemptStorageKey, {
        version: 1,
        requestID,
        payloadHash,
      });
      const proposal = await api.proposeTaskAction(
        text,
        normalizedTaskID || undefined,
        requestID,
      );
      if (generation !== pollGeneration.current) return;
      removeStoredAction(proposalAttemptStorageKey);
      if (
        proposal.action &&
        (proposal.action.kind !== actionKind ||
          (proposal.action.task_id ?? "") !== normalizedTaskID)
      ) {
        setReply("");
        setPreview(null);
        setError(labelsRef.current.invalidProposal);
        return;
      }
      setReply(proposal.reply);
      setPreview(proposal.action ?? null);
    } catch (err) {
      if (generation !== pollGeneration.current) return;
      setError(
        err instanceof ApiError ? err.message : labelsRef.current.requestFailed,
      );
    } finally {
      if (generation === pollGeneration.current) setBusy("");
    }
  }

  async function confirm() {
    if (!preview || busy || acceptedActionID) return;
    setBusy("confirm");
    setError("");
    // Persist the already-known durable action identity before the mutation.
    // If the server accepts but its HTTP response is lost, reload can still
    // converge through the owner-scoped status endpoint.
    persistAccepted(preview.id);
    setAcceptedActionID(preview.id);
    try {
      const accepted = await api.confirmTaskAction(preview.id);
      setReply(accepted.message);
      await pollRef.current(preview.id);
    } catch (err) {
      setError(
        err instanceof ApiError ? err.message : labelsRef.current.requestFailed,
      );
      await pollRef.current(preview.id);
    }
  }

  async function cancel() {
    if (!preview || busy || acceptedActionID) return;
    setBusy("cancel");
    setError("");
    persistAccepted(preview.id);
    setAcceptedActionID(preview.id);
    try {
      const accepted = await api.cancelTaskAction(preview.id);
      setReply(accepted.message);
      await pollRef.current(preview.id);
    } catch (err) {
      setError(
        err instanceof ApiError ? err.message : labelsRef.current.requestFailed,
      );
      await pollRef.current(preview.id);
    }
  }

  const terminal = status?.terminal === true;
  const visible = open || terminal;
  return (
    <Dialog open={visible} onOpenChange={(value) => !value && close()}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>{labels.title}</DialogTitle>
          <DialogDescription>{labels.description}</DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          {!preview && !terminal && !acceptedActionID && (
            <div className="flex gap-2">
              <Input
                value={input}
                onChange={(event) => setInput(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter") void draft();
                }}
                aria-label={labels.inputLabel}
                placeholder={labels.placeholder}
                disabled={busy !== ""}
                autoFocus
              />
              <Button
                size="sm"
                onClick={() => void draft()}
                disabled={!normalizeTaskActionField(input) || busy !== ""}
              >
                {busy === "draft" && <Loader2 className="size-4 animate-spin" />}
                {busy === "draft" ? labels.drafting : labels.draft}
              </Button>
            </div>
          )}

          <div role="status" aria-live="polite" aria-atomic="true">
            {reply && (
              <p className="text-sm text-muted-foreground whitespace-pre-wrap">
                {reply}
              </p>
            )}
            {status && (
              <p className="text-xs text-muted-foreground">
                {labels.resultStatus}:{" "}
                {labels.status[status.status] ?? status.status}
              </p>
            )}
            {waiting && (
              <Alert>
                <AlertDescription>{labels.waiting}</AlertDescription>
              </Alert>
            )}
          </div>
          {preview && (
            <div className="rounded-lg border bg-muted/30 p-4">
              <p className="mb-2 text-xs font-medium text-muted-foreground">
                {labels.preview}
              </p>
              <p className="whitespace-pre-wrap text-sm">{preview.summary}</p>
            </div>
          )}
          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}
        </div>

        {(preview || acceptedActionID || terminal) && (
          <DialogFooter>
            {terminal ? (
              <Button onClick={close}>{labels.close}</Button>
            ) : acceptedActionID ? (
              waiting && (
                <Button
                  variant="outline"
                  onClick={() => void pollRef.current(acceptedActionID)}
                >
                  {labels.checkAgain}
                </Button>
              )
            ) : (
              <>
                <Button
                  variant="outline"
                  onClick={() => void cancel()}
                  disabled={busy !== ""}
                >
                  {labels.cancel}
                </Button>
                <Button
                  onClick={() => void confirm()}
                  disabled={busy !== ""}
                >
                  {busy === "confirm" && (
                    <Loader2 className="size-4 animate-spin" />
                  )}
                  {busy === "confirm" ? labels.confirming : labels.confirm}
                </Button>
              </>
            )}
          </DialogFooter>
        )}
      </DialogContent>
    </Dialog>
  );
}

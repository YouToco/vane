import { useState } from "react";
import { Loader2 } from "lucide-react";
import { api, ApiError } from "@/shared/api/client";
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
} from "@/shared/api/task-action-canonical";
import { TASK_EXECUTION_STORAGE_PREFIX } from "@/shared/runtime/task-action-session";

export interface TaskActionDialogLabels {
  title: string;
  description: string;
  placeholder: string;
  inputLabel: string;
  execute: string;
  executing: string;
  close: string;
  requestFailed: string;
}

interface TaskActionDialogProps {
  open: boolean;
  actorScope: string;
  taskID?: string;
  labels: TaskActionDialogLabels;
  onClose: () => void;
  onComplete: () => void;
}

interface StoredExecutionAttempt {
  version: 2;
  requestID: string;
  payloadHash: string;
}

function executionStorageKey(actorScope: string, taskID?: string): string {
  const actor = encodeURIComponent(actorScope);
  return taskID
    ? `${TASK_EXECUTION_STORAGE_PREFIX}:${actor}:edit:${encodeURIComponent(taskID)}`
    : `${TASK_EXECUTION_STORAGE_PREFIX}:${actor}:create`;
}

function readExecutionAttempt(key: string): StoredExecutionAttempt | null {
  if (typeof window === "undefined") return null;
  try {
    const parsed = JSON.parse(window.sessionStorage.getItem(key) ?? "null") as
      | Partial<StoredExecutionAttempt>
      | null;
    if (
      parsed?.version !== 2 ||
      typeof parsed.requestID !== "string" ||
      typeof parsed.payloadHash !== "string"
    ) {
      return null;
    }
    return parsed as StoredExecutionAttempt;
  } catch {
    return null;
  }
}

function writeExecutionAttempt(
  key: string,
  attempt: StoredExecutionAttempt,
): void {
  if (typeof window === "undefined") return;
  try {
    window.sessionStorage.setItem(key, JSON.stringify(attempt));
  } catch {
    // The request remains idempotent for this mounted dialog.
  }
}

function clearExecutionAttempt(key: string): void {
  if (typeof window === "undefined") return;
  try {
    window.sessionStorage.removeItem(key);
  } catch {
    // A successful response is already authoritative.
  }
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
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [executing, setExecuting] = useState(false);
  const normalizedTaskID = normalizeTaskActionField(taskID ?? "");
  const mode = normalizedTaskID ? "edit" : "create";
  const storageKey = executionStorageKey(
    actorScope,
    normalizedTaskID || undefined,
  );

  function close() {
    if (executing) return;
    setInput("");
    setMessage("");
    setError("");
    onClose();
  }

  async function execute() {
    const text = normalizeTaskActionField(input);
    if (!text || executing) return;
    setExecuting(true);
    setError("");
    setMessage("");
    try {
      const payloadHash = await taskActionPayloadHash(
        mode,
        normalizedTaskID,
        text,
      );
      const previous = readExecutionAttempt(storageKey);
      const requestID =
        previous?.payloadHash === payloadHash
          ? previous.requestID
          : `${globalThis.crypto.randomUUID()}.${payloadHash}`;
      writeExecutionAttempt(storageKey, {
        version: 2,
        requestID,
        payloadHash,
      });
      const result = await api.executeTaskAction(
        text,
        normalizedTaskID || undefined,
        requestID,
      );
      clearExecutionAttempt(storageKey);
      setMessage(result.message);
      onComplete();
    } catch (err) {
      setError(err instanceof ApiError ? err.message : labels.requestFailed);
    } finally {
      setExecuting(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={(value) => !value && close()}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>{labels.title}</DialogTitle>
          <DialogDescription>{labels.description}</DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          <div className="flex gap-2">
            <Input
              value={input}
              onChange={(event) => setInput(event.target.value)}
              onKeyDown={(event) => {
                if (
                  event.key === "Enter" &&
                  !event.nativeEvent.isComposing
                ) {
                  void execute();
                }
              }}
              aria-label={labels.inputLabel}
              placeholder={labels.placeholder}
              disabled={executing}
              autoFocus
            />
            <Button
              size="sm"
              onClick={() => void execute()}
              disabled={!normalizeTaskActionField(input) || executing}
            >
              {executing && <Loader2 className="size-4 animate-spin" />}
              {executing ? labels.executing : labels.execute}
            </Button>
          </div>

          <div role="status" aria-live="polite" aria-atomic="true">
            {message && (
              <p className="whitespace-pre-wrap text-sm text-muted-foreground">
                {message}
              </p>
            )}
          </div>
          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}
        </div>

        {message && (
          <DialogFooter>
            <Button onClick={close}>{labels.close}</Button>
          </DialogFooter>
        )}
      </DialogContent>
    </Dialog>
  );
}

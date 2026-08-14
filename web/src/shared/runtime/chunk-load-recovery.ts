export const CHUNK_RELOAD_STORAGE_KEY = "vane:chunk-reload-at";
export const CHUNK_RELOAD_WINDOW_MS = 60_000;
const VITE_PRELOAD_ERROR_CONTEXT_MS = 5_000;
export const RELEASE_ID =
  typeof __VANE_RELEASE__ === "string" && __VANE_RELEASE__.trim()
    ? __VANE_RELEASE__
    : "unknown";

export type ChunkRecoveryFallbackReason =
  | "offline"
  | "repeated"
  | "storage-unavailable";

export interface ChunkRecoveryRuntime {
  now: () => number;
  online: () => boolean;
  readReloadAt: () => number | null;
  writeReloadAt: (value: number) => boolean;
  reload: () => void;
  showFallback: (reason: ChunkRecoveryFallbackReason) => void;
}

export interface VitePreloadErrorEvent extends Event {
  payload?: unknown;
}

export type AutomaticChunkRecoveryResult =
  | "reloading"
  | ChunkRecoveryFallbackReason;

let lastVitePreloadErrorAt: number | null = null;
const chunkErrorClassifications = new WeakMap<object, boolean>();

function recoveryCopy(reason: ChunkRecoveryFallbackReason) {
  if (reason === "offline") {
    return {
      title: "网络连接已断开 / You are offline",
      body: "恢复网络后，请手动刷新以载入最新版本。",
    };
  }
  return {
    title: "页面版本已更新 / A new version is available",
    body: "自动恢复未能完成。请手动刷新；如果问题持续，请将下方版本信息发给支持人员。",
  };
}

function showStaticRecoveryScreen(reason: ChunkRecoveryFallbackReason) {
  const mount = document.getElementById("root") ?? document.body;
  const copy = recoveryCopy(reason);
  const panel = document.createElement("main");
  panel.setAttribute("role", "alert");
  panel.setAttribute("aria-live", "assertive");
  panel.dataset.vaneChunkRecovery = reason;
  panel.style.cssText =
    "box-sizing:border-box;min-height:100vh;display:grid;place-items:center;" +
    "padding:24px;background:#0b1017;color:#edf5ff;font-family:system-ui,sans-serif;";

  const card = document.createElement("section");
  card.style.cssText =
    "width:min(100%,520px);padding:28px;border:1px solid #2c3b4d;border-radius:16px;" +
    "background:#111923;box-shadow:0 18px 60px rgba(0,0,0,.35);";

  const title = document.createElement("h1");
  title.textContent = copy.title;
  title.style.cssText = "margin:0 0 12px;font-size:22px;line-height:1.35;";

  const body = document.createElement("p");
  body.textContent = copy.body;
  body.style.cssText = "margin:0 0 20px;color:#b9c8d8;line-height:1.65;";

  const button = document.createElement("button");
  button.type = "button";
  button.textContent = "刷新页面 / Reload";
  button.style.cssText =
    "cursor:pointer;border:0;border-radius:10px;padding:10px 16px;" +
    "background:#6bdcff;color:#071018;font:inherit;font-weight:700;";
  button.addEventListener("click", () => window.location.reload());

  const release = document.createElement("p");
  release.textContent = `Release: ${RELEASE_ID}`;
  release.style.cssText =
    "margin:18px 0 0;color:#7f93a8;font:12px/1.5 ui-monospace,monospace;overflow-wrap:anywhere;";

  card.append(title, body, button, release);
  panel.append(card);
  mount.replaceChildren(panel);
}

export function createBrowserChunkRecoveryRuntime(): ChunkRecoveryRuntime {
  return {
    now: () => Date.now(),
    online: () => navigator.onLine,
    readReloadAt: () => {
      try {
        const raw = window.sessionStorage.getItem(CHUNK_RELOAD_STORAGE_KEY);
        if (raw === null) return null;
        const value = Number(raw);
        return Number.isFinite(value) ? value : null;
      } catch {
        return null;
      }
    },
    writeReloadAt: (value) => {
      try {
        window.sessionStorage.setItem(CHUNK_RELOAD_STORAGE_KEY, String(value));
        return true;
      } catch {
        return false;
      }
    },
    reload: () => window.location.reload(),
    showFallback: showStaticRecoveryScreen,
  };
}

export function attemptAutomaticChunkRecovery(
  runtime: ChunkRecoveryRuntime = createBrowserChunkRecoveryRuntime(),
): AutomaticChunkRecoveryResult {
  if (!runtime.online()) return "offline";

  const now = runtime.now();
  const lastReloadAt = runtime.readReloadAt();
  if (
    lastReloadAt !== null &&
    now - lastReloadAt < CHUNK_RELOAD_WINDOW_MS
  ) {
    return "repeated";
  }
  if (!runtime.writeReloadAt(now)) return "storage-unavailable";

  runtime.reload();
  return "reloading";
}

export function handleVitePreloadError(
  event: VitePreloadErrorEvent,
  runtime: ChunkRecoveryRuntime = createBrowserChunkRecoveryRuntime(),
) {
  // Preventing Vite's original rejection can make React.lazy surface a
  // secondary, browser-specific "module is undefined" error. Keep a very
  // short context marker so the root boundary can still attribute that error
  // to this verified preload failure without classifying ordinary app errors.
  lastVitePreloadErrorAt = Date.now();
  event.preventDefault();
  const result = attemptAutomaticChunkRecovery(runtime);
  if (result !== "reloading") runtime.showFallback(result);
}

export function installChunkLoadRecovery(
  runtime: ChunkRecoveryRuntime = createBrowserChunkRecoveryRuntime(),
) {
  const listener = (event: Event) =>
    handleVitePreloadError(event as VitePreloadErrorEvent, runtime);
  window.addEventListener("vite:preloadError", listener);
  return () => window.removeEventListener("vite:preloadError", listener);
}

const CHUNK_ERROR_PATTERNS = [
  /chunkloaderror/i,
  /loading chunk [\w-]+ failed/i,
  /failed to fetch dynamically imported module/i,
  /error loading dynamically imported module/i,
  /importing a module script failed/i,
  /unable to preload css/i,
];

export function isChunkLoadError(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const candidate = error as { name?: unknown; message?: unknown };
  const name = typeof candidate.name === "string" ? candidate.name : "";
  const message =
    typeof candidate.message === "string" ? candidate.message : "";
  return CHUNK_ERROR_PATTERNS.some((pattern) =>
    pattern.test(`${name}: ${message}`),
  );
}

function consumeRecentVitePreloadError(): boolean {
  const observedAt = lastVitePreloadErrorAt;
  lastVitePreloadErrorAt = null;
  return (
    observedAt !== null &&
    Date.now() - observedAt <= VITE_PRELOAD_ERROR_CONTEXT_MS
  );
}

export function classifyChunkLoadError(error: unknown): boolean {
  if (error !== null && typeof error === "object") {
    const classified = chunkErrorClassifications.get(error);
    if (classified !== undefined) return classified;
  }

  // Always consume the marker, including when the error already has a direct
  // chunk signature. One verified Vite event may explain at most one
  // browser-specific secondary error.
  const followsVitePreloadError = consumeRecentVitePreloadError();
  const recoverable = isChunkLoadError(error) || followsVitePreloadError;
  if (error !== null && typeof error === "object") {
    chunkErrorClassifications.set(error, recoverable);
  }
  return recoverable;
}

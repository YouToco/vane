// @vitest-environment jsdom

import React, { lazy, Suspense } from "react";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, test, vi } from "vitest";
import {
  CHUNK_RELOAD_WINDOW_MS,
  attemptAutomaticChunkRecovery,
  classifyChunkLoadError,
  handleVitePreloadError,
  installChunkLoadRecovery,
  isChunkLoadError,
  type ChunkRecoveryFallbackReason,
  type ChunkRecoveryRuntime,
} from "@/chunk-load-recovery";
import { ChunkLoadErrorBoundary } from "@/components/ChunkLoadErrorBoundary";

function runtime(overrides: Partial<ChunkRecoveryRuntime> = {}) {
  const fallback = vi.fn<(reason: ChunkRecoveryFallbackReason) => void>();
  const value: ChunkRecoveryRuntime = {
    now: () => 100_000,
    online: () => true,
    readReloadAt: () => null,
    writeReloadAt: () => true,
    reload: vi.fn(),
    showFallback: fallback,
    ...overrides,
  };
  return { value, fallback };
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("Vite chunk load recovery", () => {
  test("ordinary business errors escape the chunk boundary without reloading", async () => {
    const { value } = runtime();
    vi.spyOn(console, "error").mockImplementation(() => {});

    class OuterBoundary extends React.Component<
      { children: React.ReactNode },
      { error: Error | null }
    > {
      state = { error: null };

      static getDerivedStateFromError(error: Error) {
        return { error };
      }

      render() {
        return this.state.error ? (
          <p>outer: {this.state.error.message}</p>
        ) : (
          this.props.children
        );
      }
    }

    function BrokenBusinessView(): never {
      throw new Error("profile request failed");
    }

    render(
      <OuterBoundary>
        <ChunkLoadErrorBoundary runtime={value}>
          <BrokenBusinessView />
        </ChunkLoadErrorBoundary>
      </OuterBoundary>,
    );

    await screen.findByText("outer: profile request failed");
    expect(value.reload).not.toHaveBeenCalled();
  });

  test("prevents the preload error and reloads once after persisting the guard", () => {
    const writes: number[] = [];
    const { value, fallback } = runtime({
      writeReloadAt: (timestamp) => {
        writes.push(timestamp);
        return true;
      },
    });
    const event = new Event("vite:preloadError", { cancelable: true });

    handleVitePreloadError(event, value);

    expect(event.defaultPrevented).toBe(true);
    expect(writes).toEqual([100_000]);
    expect(value.reload).toHaveBeenCalledOnce();
    expect(fallback).not.toHaveBeenCalled();
  });

  test("does not reload again inside the guard window", () => {
    const { value, fallback } = runtime({
      readReloadAt: () => 100_000 - CHUNK_RELOAD_WINDOW_MS + 1,
    });

    expect(attemptAutomaticChunkRecovery(value)).toBe("repeated");
    expect(value.reload).not.toHaveBeenCalled();
    handleVitePreloadError(
      new Event("vite:preloadError", { cancelable: true }),
      value,
    );
    expect(fallback).toHaveBeenCalledWith("repeated");
  });

  test("fails safely to a recovery screen when offline or storage is unavailable", () => {
    const offline = runtime({ online: () => false });
    handleVitePreloadError(
      new Event("vite:preloadError", { cancelable: true }),
      offline.value,
    );
    expect(offline.value.reload).not.toHaveBeenCalled();
    expect(offline.fallback).toHaveBeenCalledWith("offline");

    const noStorage = runtime({ writeReloadAt: () => false });
    handleVitePreloadError(
      new Event("vite:preloadError", { cancelable: true }),
      noStorage.value,
    );
    expect(noStorage.value.reload).not.toHaveBeenCalled();
    expect(noStorage.fallback).toHaveBeenCalledWith("storage-unavailable");
  });

  test("installs and removes only the Vite preload listener", () => {
    const { value } = runtime();
    const remove = installChunkLoadRecovery(value);

    window.dispatchEvent(
      new Event("vite:preloadError", { cancelable: true }),
    );
    expect(value.reload).toHaveBeenCalledOnce();

    remove();
    window.dispatchEvent(
      new Event("vite:preloadError", { cancelable: true }),
    );
    expect(value.reload).toHaveBeenCalledOnce();
  });

  test("recognizes chunk failures without treating ordinary app errors as chunks", () => {
    expect(
      isChunkLoadError(
        new TypeError(
          "Failed to fetch dynamically imported module: /assets/page-old.js",
        ),
      ),
    ).toBe(true);
    expect(isChunkLoadError(new Error("profile request failed"))).toBe(false);
  });

  test("React lazy rejection shows a recoverable boundary with release info", async () => {
    const { value } = runtime({
      readReloadAt: () => 99_999,
    });
    const BrokenLazy = lazy(() =>
      Promise.reject(
        new TypeError(
          "Failed to fetch dynamically imported module: /assets/lazy-old.js",
        ),
      ),
    );
    const user = userEvent.setup();

    render(
      <ChunkLoadErrorBoundary runtime={value}>
        <Suspense fallback={<span>loading</span>}>
          <BrokenLazy />
        </Suspense>
      </ChunkLoadErrorBoundary>,
    );

    const alert = await screen.findByRole("alert");
    expect(alert.textContent).toContain("页面版本已更新");
    expect(alert.textContent).toContain("Release:");
    expect(value.reload).not.toHaveBeenCalled();

    await user.click(screen.getByRole("button", { name: /Reload/ }));
    await waitFor(() => expect(value.reload).toHaveBeenCalledOnce());
  });

  test("keeps the boundary recoverable for React's secondary error after a Vite event", async () => {
    const { value } = runtime({ online: () => false });
    handleVitePreloadError(
      new Event("vite:preloadError", { cancelable: true }),
      value,
    );
    const SecondaryLazyError = lazy(() =>
      Promise.reject(new TypeError("Cannot read properties of undefined")),
    );

    render(
      <ChunkLoadErrorBoundary runtime={value}>
        <Suspense fallback={<span>loading</span>}>
          <SecondaryLazyError />
        </Suspense>
      </ChunkLoadErrorBoundary>,
    );

    const alert = await screen.findByRole("alert");
    await waitFor(() =>
      expect(alert.dataset.vaneChunkRecovery).toBe("offline"),
    );
    expect(alert.textContent).toContain("You are offline");
  });

  test("a preload marker explains only the first secondary generic error", () => {
    const { value } = runtime({ online: () => false });
    handleVitePreloadError(
      new Event("vite:preloadError", { cancelable: true }),
      value,
    );

    const secondary = new TypeError("Cannot read properties of undefined");
    expect(classifyChunkLoadError(secondary)).toBe(true);
    expect(classifyChunkLoadError(secondary)).toBe(true);
    expect(
      classifyChunkLoadError(new Error("ordinary business failure")),
    ).toBe(false);
  });
});

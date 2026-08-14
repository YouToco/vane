import {
  Component,
  type ErrorInfo,
  type ReactNode,
} from "react";
import {
  attemptAutomaticChunkRecovery,
  classifyChunkLoadError,
  createBrowserChunkRecoveryRuntime,
  RELEASE_ID,
  type ChunkRecoveryFallbackReason,
  type ChunkRecoveryRuntime,
} from "@/shared/runtime/chunk-load-recovery";

interface ChunkLoadErrorBoundaryProps {
  children: ReactNode;
  runtime?: ChunkRecoveryRuntime;
}

interface ChunkLoadErrorBoundaryState {
  error: unknown;
  recoverable: boolean;
  reason: ChunkRecoveryFallbackReason;
}

function ChunkRecoveryScreen({
  reason,
  reload,
}: {
  reason: ChunkRecoveryFallbackReason;
  reload: () => void;
}) {
  const offline = reason === "offline";
  return (
    <main
      role="alert"
      aria-live="assertive"
      data-vane-chunk-recovery={reason}
      className="grid min-h-screen place-items-center bg-[#0b1017] p-6 text-[#edf5ff]"
    >
      <section className="w-full max-w-lg rounded-2xl border border-[#2c3b4d] bg-[#111923] p-7 shadow-2xl">
        <h1 className="mb-3 text-xl font-semibold leading-snug">
          {offline
            ? "网络连接已断开 / You are offline"
            : "页面版本已更新 / A new version is available"}
        </h1>
        <p className="mb-5 leading-relaxed text-[#b9c8d8]">
          {offline
            ? "恢复网络后，请手动刷新以载入最新版本。"
            : "自动恢复未能完成。请手动刷新；如果问题持续，请将下方版本信息发给支持人员。"}
        </p>
        <button
          type="button"
          onClick={reload}
          className="rounded-lg bg-[#6bdcff] px-4 py-2.5 font-semibold text-[#071018]"
        >
          刷新页面 / Reload
        </button>
        <p className="mt-5 break-words font-mono text-xs leading-relaxed text-[#7f93a8]">
          Release: {RELEASE_ID}
        </p>
      </section>
    </main>
  );
}

export class ChunkLoadErrorBoundary extends Component<
  ChunkLoadErrorBoundaryProps,
  ChunkLoadErrorBoundaryState
> {
  state: ChunkLoadErrorBoundaryState = {
    error: null,
    recoverable: false,
    reason: "repeated",
  };

  private runtime: ChunkRecoveryRuntime;

  constructor(props: ChunkLoadErrorBoundaryProps) {
    super(props);
    this.runtime = props.runtime ?? createBrowserChunkRecoveryRuntime();
  }

  static getDerivedStateFromError(error: unknown) {
    return {
      error,
      recoverable: classifyChunkLoadError(error),
    };
  }

  componentDidCatch(_error: unknown, _info: ErrorInfo) {
    if (!this.state.recoverable) return;
    const result = attemptAutomaticChunkRecovery(this.runtime);
    if (result !== "reloading") this.setState({ reason: result });
  }

  render() {
    if (this.state.error !== null) {
      if (!this.state.recoverable) throw this.state.error;
      return (
        <ChunkRecoveryScreen
          reason={this.state.reason}
          reload={this.runtime.reload}
        />
      );
    }
    return this.props.children;
  }
}

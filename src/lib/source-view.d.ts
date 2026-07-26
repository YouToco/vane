export type SourceFormKind = "rss" | "exa" | "tikhub_xhs" | "web_contents";

export type AddSubscriptionRequest =
  | { url: string }
  | { type: "exa"; query: string; category?: string }
  | { type: "tikhub_xhs"; keyword: string }
  | { platform: "web"; capability: "contents"; params: { url: string } };

export interface SourceFormValues {
  url: string;
  query: string;
  category: string;
  keyword: string;
}

export interface SourceViewInput {
  type?: string;
  platform?: string;
  capability?: string;
  url: string;
  config?: unknown;
}

export type SourceViewKind =
  | "rss"
  | "webSearch"
  | "webContents"
  | "xhsSearch"
  | "platformCapability";

export interface SourceView {
  kind: SourceViewKind;
  term: string;
  category: string;
  platformCapability: string;
}

export function normalizeCategory(value: string | null | undefined): string;
export function buildSubscriptionRequest(
  kind: SourceFormKind,
  values: SourceFormValues,
): AddSubscriptionRequest;
export function safeHttpHref(raw: string): string | null;
export function sourceView(source: SourceViewInput): SourceView;

export function safeBriefURL(
  raw: string | null | undefined,
): string | null {
  if (!raw) return null;
  try {
    const parsed = new URL(raw);
    if (parsed.username || parsed.password) return null;
    return parsed.protocol === "https:" || parsed.protocol === "http:"
      ? parsed.href
      : null;
  } catch {
    return null;
  }
}

export function safeBriefMarkdownURL(raw: string): string {
  return safeBriefURL(raw) ?? "";
}

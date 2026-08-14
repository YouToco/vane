// Go strings.TrimSpace uses unicode.IsSpace. Its exact current set differs
// from JavaScript trim(): Go includes U+0085 but excludes U+FEFF.
const GO_TRIM_START =
  /^[\u0009-\u000d\u0020\u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]+/;
const GO_TRIM_END =
  /[\u0009-\u000d\u0020\u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]+$/;

function replaceLoneSurrogates(value: string): string {
  let normalized = "";
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        normalized += value[index] + value[index + 1];
        index += 1;
      } else {
        normalized += "\ufffd";
      }
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      normalized += "\ufffd";
    } else {
      normalized += value[index];
    }
  }
  return normalized;
}

export function normalizeTaskActionField(value: string): string {
  return replaceLoneSurrogates(value)
    .replace(GO_TRIM_START, "")
    .replace(GO_TRIM_END, "");
}

export function canonicalTaskActionPayload(
  mode: "create" | "edit",
  taskID: string,
  text: string,
): string {
  return JSON.stringify({
    mode,
    task_id: normalizeTaskActionField(taskID),
    text: normalizeTaskActionField(text),
  })
    .replace(/\u2028/g, "\\u2028")
    .replace(/\u2029/g, "\\u2029");
}

export async function taskActionPayloadHash(
  mode: "create" | "edit",
  taskID: string,
  text: string,
): Promise<string> {
  const canonical = canonicalTaskActionPayload(mode, taskID, text);
  const bytes = new TextEncoder().encode(canonical);
  const digest = await globalThis.crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest), (value) =>
    value.toString(16).padStart(2, "0"),
  ).join("");
}

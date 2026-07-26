export function normalizeCategory(value) {
  return value === "__none__" ? "" : (value ?? "");
}

export function buildSubscriptionRequest(kind, values) {
  switch (kind) {
    case "rss":
      return { url: values.url.trim() };
    case "exa": {
      const category = normalizeCategory(values.category).trim();
      return category
        ? { type: "exa", query: values.query.trim(), category }
        : { type: "exa", query: values.query.trim() };
    }
    case "tikhub_xhs":
      return { type: "tikhub_xhs", keyword: values.keyword.trim() };
    case "web_contents":
      return {
        platform: "web",
        capability: "contents",
        params: { url: values.url.trim() },
      };
  }
  throw new TypeError(`unsupported source form kind: ${kind}`);
}

export function safeHttpHref(raw) {
  try {
    const parsed = new URL(raw);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return null;
    if (!parsed.hostname || parsed.username || parsed.password) return null;
    return parsed.href;
  } catch {
    return null;
  }
}

function sourceConfig(value) {
  if (typeof value === "string") {
    try {
      return sourceConfig(JSON.parse(value));
    } catch {
      return {};
    }
  }
  return value !== null && typeof value === "object" ? value : {};
}

function stringField(value) {
  return typeof value === "string" ? value : "";
}

function syntheticParam(rawURL, param) {
  try {
    return new URL(rawURL).searchParams.get(param) ?? "";
  } catch {
    return "";
  }
}

const SUMMARY_FIELDS = [
  "uid",
  "user_id",
  "page_id",
  "keyword",
  "query",
  "screen_name",
  "username",
  "url",
  "title",
];

function shortValue(value, maxRunes = 80) {
  if (typeof value !== "string" && typeof value !== "number") return "";
  const text = String(value).trim();
  const runes = [...text];
  return runes.length > maxRunes
    ? `${runes.slice(0, maxRunes).join("")}…`
    : text;
}

function knownConfigSummary(config) {
  for (const key of SUMMARY_FIELDS) {
    const value = shortValue(config[key]);
    if (value) return `${key}=${value}`;
  }
  return "";
}

export function sourceView(source) {
  const platform = source.platform ?? "";
  const capability = source.capability ?? "";
  const config = sourceConfig(source.config);
  const platformCapability =
    platform && capability ? `${platform}/${capability}` : source.type || "unknown";

  if (source.type === "rss" || (platform === "web" && capability === "feed")) {
    return {
      kind: "rss",
      term: source.url,
      category: "",
      platformCapability,
    };
  }
  if (source.type === "exa" || (platform === "web" && capability === "search")) {
    return {
      kind: "webSearch",
      term:
        stringField(config.query) ||
        syntheticParam(source.url, "q") ||
        source.url,
      category:
        stringField(config.category) ||
        syntheticParam(source.url, "category"),
      platformCapability,
    };
  }
  if (
    source.type === "tikhub_xhs" ||
    (platform === "xhs" && capability === "search")
  ) {
    return {
      kind: "xhsSearch",
      term:
        stringField(config.keyword) ||
        syntheticParam(source.url, "keyword") ||
        source.url,
      category: "",
      platformCapability,
    };
  }
  if (
    source.type === "web/contents" ||
    (platform === "web" && capability === "contents")
  ) {
    return {
      kind: "webContents",
      term:
        stringField(config.url) ||
        syntheticParam(source.url, "url") ||
        source.url,
      category: "",
      platformCapability,
    };
  }
  return {
    kind: "platformCapability",
    term: knownConfigSummary(config) || platformCapability,
    category: "",
    platformCapability,
  };
}

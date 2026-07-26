import assert from "node:assert/strict";
import test from "node:test";

import {
  buildSubscriptionRequest,
  normalizeCategory,
  safeHttpHref,
  sourceView,
} from "../src/lib/source-view.js";

const empty = { url: "", query: "", category: "", keyword: "" };

test("Exa category sentinel is never sent to the backend", () => {
  assert.equal(normalizeCategory("__none__"), "");
  assert.deepEqual(
    buildSubscriptionRequest("exa", {
      ...empty,
      query: " AI agents ",
      category: "__none__",
    }),
    { type: "exa", query: "AI agents" },
  );
});

test("web contents uses the current platform/capability contract", () => {
  assert.deepEqual(
    buildSubscriptionRequest("web_contents", {
      ...empty,
      url: " https://example.com/pricing ",
    }),
    {
      platform: "web",
      capability: "contents",
      params: { url: "https://example.com/pricing" },
    },
  );
});

test("only absolute HTTP(S) links are clickable", () => {
  assert.equal(safeHttpHref("https://example.com/a"), "https://example.com/a");
  assert.equal(safeHttpHref("http://example.com/a"), "http://example.com/a");
  for (const value of [
    "https://user@example.com/private",
    "https://user:password@example.com/private",
    "javascript:alert(1)",
    "data:text/html,x",
    "vane://web/search?q=x",
    "/relative",
    "not a url",
  ]) {
    assert.equal(safeHttpHref(value), null);
  }
});

test("source display prefers typed config over synthetic identifiers", () => {
  assert.deepEqual(
    sourceView({
      type: "web/contents",
      platform: "web",
      capability: "contents",
      url: "vane://web/contents?url=https%3A%2F%2Fwrong.example",
      config: { url: "https://example.com/pricing" },
    }),
    {
      kind: "webContents",
      term: "https://example.com/pricing",
      category: "",
      platformCapability: "web/contents",
    },
  );
});

test("new platform capabilities remain readable without becoming links", () => {
  assert.deepEqual(
    sourceView({
      type: "weibo/user_posts",
      platform: "weibo",
      capability: "user_posts",
      url: "vane://weibo/user_posts?uid=123",
      config: { uid: "123" },
    }),
    {
      kind: "platformCapability",
      term: "uid=123",
      category: "",
      platformCapability: "weibo/user_posts",
    },
  );
});

test("generic display only exposes allowlisted scalar config fields", () => {
  const view = sourceView({
    type: "custom/watch",
    platform: "custom",
    capability: "watch",
    url: "vane://custom/watch?secret=should-not-leak",
    config: {
      unknown_secret: "should-not-leak",
      uid: "123456789",
      nested: { token: "also-secret" },
    },
  });
  assert.equal(view.term, "uid=123456789");
  assert.equal(view.term.includes("secret"), false);

  assert.equal(
    sourceView({
      type: "custom/watch",
      platform: "custom",
      capability: "watch",
      url: "vane://custom/watch?secret=should-not-leak",
      config: { unknown_secret: "should-not-leak" },
    }).term,
    "custom/watch",
  );
});

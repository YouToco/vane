import { describe, expect, it } from "vitest";
import { apiBase, PRODUCTION_API_ORIGIN } from "@/shared/api/base";

describe("API base authority", () => {
  it("keeps local Vite development on the same-origin proxy", () => {
    expect(apiBase(true)).toBe("");
  });

  it("binds every production build to the public API origin", () => {
    expect(apiBase(false)).toBe("https://api.vane.zhuoqidev.com");
    expect(apiBase(false)).toBe(PRODUCTION_API_ORIGIN);
  });
});

// @vitest-environment jsdom

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, test, vi } from "vitest";

import { api } from "@/api";
import type { ProviderPriceRule } from "@/api";
import Pricing from "@/pages/Pricing";

const current: ProviderPriceRule = {
  id: 7,
  provider: "kimi",
  resource: "kimi-k2.6",
  meter: "llm_tokens",
  currency: "USD",
  input_cache_hit_per_million: 0.16,
  input_cache_miss_per_million: 0.95,
  output_per_million: 4,
  request_included_quantity: undefined,
  request_additional_unit_price: undefined,
  effective_from: "2026-07-30T00:00:00Z",
  source_url: "https://platform.kimi.ai/docs/pricing/chat-k26",
  note: "official",
  created_at: "2026-07-30T00:00:00Z",
};

afterEach(() => vi.restoreAllMocks());

describe("provider pricing admin", () => {
  test("prefills an active rule and creates a new immutable version", async () => {
    vi.spyOn(api, "adminListProviderPrices").mockResolvedValue([current]);
    const replace = vi
      .spyOn(api, "adminReplaceProviderPrice")
      .mockResolvedValue({ ...current, id: 8, input_cache_hit_per_million: 0.2 });

    render(<Pricing />);
    expect(await screen.findByText("kimi-k2.6")).toBeTruthy();
    await userEvent.click(screen.getByRole("button", { name: "更新" }));

    const hit = screen.getByLabelText("输入缓存命中 / 百万 token");
    expect((hit as HTMLInputElement).value).toBe("0.16");
    fireEvent.change(hit, { target: { value: "0.2" } });
    await userEvent.click(screen.getByRole("button", { name: "保存并生效" }));

    await waitFor(() => expect(replace).toHaveBeenCalledTimes(1));
    const [input, key] = replace.mock.calls[0];
    expect(input).toMatchObject({
      provider: "kimi",
      resource: "kimi-k2.6",
      meter: "llm_tokens",
      currency: "USD",
      input_cache_hit_per_million: 0.2,
      input_cache_miss_per_million: 0.95,
      output_per_million: 4,
      source_url: current.source_url,
    });
    expect(key).toBeTruthy();
  });
});

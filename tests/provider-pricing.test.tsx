// @vitest-environment jsdom

import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
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

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("provider pricing admin", () => {
  test("prefills an active rule and creates a new immutable version", async () => {
    vi.spyOn(api, "adminListProviderPrices").mockResolvedValue([current]);
    const replace = vi
      .spyOn(api, "adminReplaceProviderPrice")
      .mockResolvedValue({
        ...current,
        id: 8,
        input_cache_hit_per_million: 0.2,
      });

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

  test("reuses one idempotency key after an ambiguous save failure", async () => {
    vi.spyOn(api, "adminListProviderPrices").mockResolvedValue([current]);
    const replace = vi
      .spyOn(api, "adminReplaceProviderPrice")
      .mockRejectedValueOnce(new Error("response lost"))
      .mockResolvedValue({ ...current, id: 8 });

    render(<Pricing />);
    await screen.findByText("kimi-k2.6");
    await userEvent.click(screen.getByRole("button", { name: "更新" }));
    await userEvent.click(screen.getByRole("button", { name: "保存并生效" }));
    await waitFor(() => expect(replace).toHaveBeenCalledTimes(1));
    await userEvent.click(screen.getByRole("button", { name: "保存并生效" }));
    await waitFor(() => expect(replace).toHaveBeenCalledTimes(2));

    expect(replace.mock.calls[0][1]).toBe(replace.mock.calls[1][1]);
  });

  test("separates future and historical prices from the active catalog", async () => {
    const upcoming = {
      ...current,
      id: 8,
      resource: "future-model",
      effective_from: "2099-01-01T00:00:00Z",
    };
    const historical = {
      ...current,
      id: 6,
      resource: "old-model",
      effective_from: "2025-01-01T00:00:00Z",
      effective_to: "2025-02-01T00:00:00Z",
    };
    vi.spyOn(api, "adminListProviderPrices").mockResolvedValue([
      current,
      upcoming,
      historical,
    ]);

    render(<Pricing />);
    expect(await screen.findByText("待生效价格")).toBeTruthy();
    expect(screen.getByText("future-model")).toBeTruthy();
    expect(screen.getByText("历史价格版本")).toBeTruthy();
    expect(screen.getByText("old-model")).toBeTruthy();
    expect(screen.getAllByRole("link", { name: "官方文档" }).length).toBe(3);
  });
});

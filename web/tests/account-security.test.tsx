// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, test, vi } from "vitest";

import AccountSecurity from "@/pages/AccountSecurity";
import { I18nProvider } from "@/i18n";
import { api } from "@/shared/api/client";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("AccountSecurity", () => {
  function renderPage(hash: string) {
    return render(
      <I18nProvider>
        <AccountSecurity hash={hash} />
      </I18nProvider>,
    );
  }

  test("keeps password reset request enumeration-safe", async () => {
    const request = vi.spyOn(api, "requestPasswordReset").mockResolvedValue({
      ok: true,
      message: "如果该邮箱已注册，重置邮件将很快送达",
    });
    renderPage("#/forgot-password");
    fireEvent.change(screen.getByLabelText("Email"), { target: { value: "person@example.com" } });
    fireEvent.click(screen.getByRole("button", { name: "Send reset email" }));
    await waitFor(() => expect(request).toHaveBeenCalledWith("person@example.com"));
    expect(await screen.findByText("如果该邮箱已注册，重置邮件将很快送达")).toBeTruthy();
  });

  test("forwards the exact reset token only after matching password input", async () => {
    const complete = vi.spyOn(api, "completePasswordReset").mockResolvedValue({ ok: true });
    renderPage("#/reset-password?token=exact-token");
    fireEvent.change(screen.getByLabelText("New password"), { target: { value: "strong-password" } });
    fireEvent.change(screen.getByLabelText("Confirm password"), { target: { value: "strong-password" } });
    fireEvent.click(screen.getByRole("button", { name: "Update password" }));
    await waitFor(() => expect(complete).toHaveBeenCalledWith("exact-token", "strong-password"));
  });

  test("does not consume an email verification token before explicit confirmation", async () => {
    const verify = vi.spyOn(api, "verifyEmail").mockResolvedValue({ ok: true });
    renderPage("#/verify-email?token=verify-token");
    expect(verify).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "Verify" }));
    await waitFor(() => expect(verify).toHaveBeenCalledWith("verify-token"));
  });
});

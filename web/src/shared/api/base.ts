export const PRODUCTION_API_ORIGIN = "https://api.vane.zhuoqidev.com";

/**
 * Production Web is static and always talks to the public Vane API origin.
 * Local Vite development keeps the relative base so its proxy remains useful.
 *
 * This is deliberately derived from Vite's mode rather than an ambient
 * VITE_API_BASE value: omitting that value once produced a valid-looking
 * release whose login POST went to Pages/OSS and failed with HTTP 405.
 */
export function apiBase(isDevelopment: boolean): string {
  return isDevelopment ? "" : PRODUCTION_API_ORIGIN;
}

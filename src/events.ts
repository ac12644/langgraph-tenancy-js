/**
 * Observability for tenancy enforcement. A denial in production is a
 * security-relevant event; these hooks make every refusal visible to
 * logging / metrics / OpenTelemetry without coupling to any of them.
 */

import type { QuotaExceededError, TenancyError } from "./errors.js";

export type TenancyEvent =
  /** A tenancy rule refused an operation (missing/invalid tenant, unscoped access). */
  | {
      type: "denied";
      operation: string;
      error: TenancyError;
      tenantId?: string;
    }
  /** A tenant hit one of its configured limits. */
  | {
      type: "quota_exceeded";
      operation: string;
      tenantId: string;
      error: QuotaExceededError;
    }
  /** The usage ledger threw while recording (see `ledgerErrors` option). */
  | {
      type: "ledger_error";
      operation: string;
      tenantId: string;
      error: unknown;
    };

export type TenancyEventHandler = (event: TenancyEvent) => void;

/** Observers must never break the data path, so handler errors are swallowed. */
export function emitEvent(
  handler: TenancyEventHandler | undefined,
  event: TenancyEvent
): void {
  if (!handler) return;
  try {
    handler(event);
  } catch {
    // intentionally ignored
  }
}

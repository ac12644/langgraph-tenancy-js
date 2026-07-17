/**
 * Per-tenant quota enforcement at the persistence boundary.
 *
 * The check runs in the checkpointer's `put()` before anything is written:
 * a run's very first checkpoint (the input checkpoint, saved before the
 * first node executes) fails with `QuotaExceededError` when the tenant is
 * at or over a limit. The turn that *crosses* a limit still completes —
 * enforcement is check-then-write, so a tenant can overshoot by at most one
 * in-flight turn.
 */

import type { QuotaViolation } from "./errors.js";

export interface TenantLimits {
  maxInputTokens?: number;
  maxOutputTokens?: number;
  maxTotalTokens?: number;
  maxMessages?: number;
}

/** The usage counters quota checks need (a subset of `TenantUsage`). */
export interface UsageSnapshot {
  inputTokens: number;
  outputTokens: number;
  totalTokens: number;
  messages: number;
}

export interface QuotaConfig {
  /**
   * Resolve a tenant's limits. Return `undefined` for "no limits". Called on
   * every `put()`, so cache internally if resolution is expensive.
   */
  limits: (
    tenantId: string
  ) => TenantLimits | undefined | Promise<TenantLimits | undefined>;
  /**
   * Where current usage comes from. Defaults to the checkpointer's
   * `usageLedger.totals()`; required here if the ledger has no `totals()`.
   */
  usage?: (tenantId: string) => UsageSnapshot | Promise<UsageSnapshot>;
}

const CHECKS = [
  ["maxInputTokens", "inputTokens"],
  ["maxOutputTokens", "outputTokens"],
  ["maxTotalTokens", "totalTokens"],
  ["maxMessages", "messages"],
] as const;

export function findViolation(
  tenantId: string,
  usage: UsageSnapshot,
  limits: TenantLimits
): QuotaViolation | undefined {
  for (const [limitKey, usageKey] of CHECKS) {
    const limit = limits[limitKey];
    if (limit != null && usage[usageKey] >= limit) {
      return { tenantId, field: usageKey, used: usage[usageKey], limit };
    }
  }
  return undefined;
}

/**
 * Tenant isolation for LangGraph.js persistence.
 *
 * LangGraph's own threat model: "Without application-level auth, any caller
 * with a valid thread_id can access that thread's state." This package is
 * that application-level wall, as a drop-in wrapper.
 *
 * ```ts
 * import {
 *   TenantScopedCheckpointer,
 *   TenantScopedStore,
 *   getTenantStore,
 *   InMemoryUsageLedger,
 * } from "langgraph-tenancy";
 *
 * const ledger = new InMemoryUsageLedger();
 * const checkpointer = new TenantScopedCheckpointer(new PostgresSaver(...), {
 *   usageLedger: ledger,
 *   quota: { limits: (tenant) => plans[tenant] },
 *   onEvent: (e) => logger.warn("tenancy", e),
 * });
 * const store = new TenantScopedStore(new InMemoryStore());
 *
 * const graph = builder.compile({ checkpointer, store });
 *
 * // tenant_id is now REQUIRED on every invocation
 * await graph.invoke(input, {
 *   configurable: { thread_id: "t1", tenant_id: "acme" },
 * });
 *
 * ledger.totals("acme"); // per-tenant token usage, for free
 * ```
 */

export {
  TenantCheckpointerHandle,
  TenantScopedCheckpointer,
  type TenantScopedCheckpointerOptions,
} from "./checkpointer.js";
export {
  InvalidTenantError,
  QuotaExceededError,
  TenancyError,
  TenantRequiredError,
  UnscopedAccessError,
  type QuotaViolation,
} from "./errors.js";
export {
  emitEvent,
  type TenancyEvent,
  type TenancyEventHandler,
} from "./events.js";
export {
  findViolation,
  type QuotaConfig,
  type TenantLimits,
  type UsageSnapshot,
} from "./quota.js";
export {
  TENANT_NS_SENTINEL,
  TenantScopedStore,
  TenantStoreView,
  getTenantStore,
  type TenantScopedStoreOptions,
  type TenantStoreTarget,
} from "./store.js";
export {
  SEP,
  TENANT_ID_PATTERN,
  validateTenant,
  validateThreadId,
} from "./tenant.js";
export {
  BoundedStringSet,
  InMemoryUsageLedger,
  extractUsage,
  extractUsageFromValues,
  type TenantUsage,
  type UsageLedger,
  type UsageRecord,
} from "./usage.js";

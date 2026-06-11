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
} from "./checkpointer.js";
export {
  InvalidTenantError,
  TenancyError,
  TenantRequiredError,
  UnscopedAccessError,
} from "./errors.js";
export {
  TENANT_NS_SENTINEL,
  TenantScopedStore,
  TenantStoreView,
  getTenantStore,
  type TenantStoreTarget,
} from "./store.js";
export { SEP, validateTenant } from "./tenant.js";
export {
  InMemoryUsageLedger,
  extractUsage,
  type TenantUsage,
  type UsageLedger,
  type UsageRecord,
} from "./usage.js";

/**
 * Errors raised by langgraph-tenancy. Every error here exists to turn a
 * silent cross-tenant leak into a loud failure.
 */

export class TenancyError extends Error {
  name = "TenancyError";
}

/**
 * An operation ran without a tenant in scope. There is no unscoped fallback:
 * no tenant, no data.
 */
export class TenantRequiredError extends TenancyError {
  name = "TenantRequiredError";

  constructor(where: string) {
    super(
      `${where} requires a tenant. Pass it in the run config: ` +
        `{ configurable: { thread_id, tenant_id } }, use getTenantStore(config) ` +
        `inside nodes, or .forTenant(tenantId) for out-of-band access.`
    );
  }
}

/**
 * An operation would touch data across tenant boundaries (e.g. a store call
 * that bypassed tenant scoping, or deleteThread without a tenant handle).
 */
export class UnscopedAccessError extends TenancyError {
  name = "UnscopedAccessError";
}

/**
 * A tenant id that could be used to escape its scope. Tenant ids become part
 * of storage keys AND store namespace segments, and physical backends flatten
 * namespaces with their own separators, so only a conservative character set
 * is accepted.
 */
export class InvalidTenantError extends TenancyError {
  name = "InvalidTenantError";
}

/** Which quota a tenant ran over, and by how much. */
export interface QuotaViolation {
  tenantId: string;
  field: "inputTokens" | "outputTokens" | "totalTokens" | "messages";
  used: number;
  limit: number;
}

/**
 * A tenant is at or over one of its configured limits. Thrown from the
 * checkpointer's `put()`, i.e. at the persistence boundary: the over-budget
 * tenant's next run fails at its first checkpoint, before any model call.
 */
export class QuotaExceededError extends TenancyError {
  name = "QuotaExceededError";

  constructor(readonly violation: QuotaViolation) {
    super(
      `Tenant ${JSON.stringify(violation.tenantId)} is over quota: ` +
        `${violation.field} ${violation.used} >= limit ${violation.limit}.`
    );
  }
}

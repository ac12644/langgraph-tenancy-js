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
 * of storage keys, so they must not contain the separator or be empty.
 */
export class InvalidTenantError extends TenancyError {
  name = "InvalidTenantError";
}

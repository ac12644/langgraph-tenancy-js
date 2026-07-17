import {
  InvalidTenantError,
  TenancyError,
  TenantRequiredError,
} from "./errors.js";

/** Separator between tenant id and thread id in physical storage keys. */
export const SEP = "::";

/**
 * Allowed shape of a tenant id. Deliberately strict: tenant ids are embedded
 * in checkpoint thread keys and store namespace segments, and physical
 * backends flatten namespaces with their own separators (`.`, `/`, ...).
 * A blocklist can't anticipate every backend, so this is an allowlist.
 */
export const TENANT_ID_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;

export function validateTenant(tenant: unknown, where: string): string {
  if (typeof tenant !== "string" || tenant.length === 0) {
    throw new TenantRequiredError(where);
  }
  if (!TENANT_ID_PATTERN.test(tenant)) {
    throw new InvalidTenantError(
      `tenant_id must match ${TENANT_ID_PATTERN} (it becomes part of ` +
        `storage keys and namespace segments): ${JSON.stringify(tenant)}`
    );
  }
  return tenant;
}

export function validateThreadId(threadId: unknown, where: string): string {
  if (typeof threadId !== "string" || threadId.length === 0) {
    throw new TenancyError(
      `${where} requires a non-empty string thread_id in ` +
        `config.configurable; got ${JSON.stringify(threadId)}.`
    );
  }
  return threadId;
}

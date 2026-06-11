import { InvalidTenantError, TenantRequiredError } from "./errors.js";

/** Separator between tenant id and thread id in physical storage keys. */
export const SEP = "::";

export function validateTenant(tenant: unknown, where: string): string {
  if (typeof tenant !== "string" || tenant.length === 0) {
    throw new TenantRequiredError(where);
  }
  if (tenant.includes(SEP)) {
    throw new InvalidTenantError(
      `tenant_id may not contain '${SEP}': ${JSON.stringify(tenant)}`
    );
  }
  return tenant;
}

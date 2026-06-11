/**
 * Tenant-scoped wrapper around any `BaseStore`.
 *
 * Raw `BaseStore` namespaces are pure convention — any caller can `get()`,
 * `search()`, or `listNamespaces()` across all of them. This wrapper roots
 * every operation at the tenant id, so two tenants using the identical
 * namespace tuple (e.g. `["memories"]`) land in physically distinct
 * locations.
 *
 * Why this differs from the Python package: LangGraph.js wraps the compiled
 * graph's store in an `AsyncBatchedStore` whose background queue loses the
 * run's AsyncLocalStorage context, so the tenant CANNOT be resolved ambiently
 * inside `batch()`. Instead, tenant scoping happens at the call site — where
 * the config is available — and `TenantScopedStore.batch()` fails closed on
 * any operation that did not go through a scoped entry point:
 *
 * - inside nodes: `getTenantStore(config)` (reads `tenant_id` from config,
 *   delegates to `config.store` so batching still applies)
 * - outside runs: `store.forTenant(tenantId)`
 * - anything else — including raw `config.store.put(...)` in a node —
 *   throws `UnscopedAccessError` instead of silently writing unscoped.
 */

import {
  BaseStore,
  type Item,
  type MatchCondition,
  type Operation,
  type OperationResults,
  type SearchItem,
} from "@langchain/langgraph-checkpoint";

import { TenancyError, UnscopedAccessError } from "./errors.js";
import { validateTenant } from "./tenant.js";

/**
 * Marker label proving an operation went through a tenant-scoped entry
 * point. Stripped before the operation reaches the inner store.
 */
export const TENANT_NS_SENTINEL = "~tenant~";

const UNSCOPED_MESSAGE =
  "Store accessed without tenant scoping. Use getTenantStore(config) inside " +
  "nodes, or store.forTenant(tenantId) outside runs. Raw store access is " +
  "refused so it cannot silently read or write across tenants.";

export class TenantScopedStore extends BaseStore {
  readonly inner: BaseStore;

  constructor(inner: BaseStore) {
    super();
    this.inner = inner;
  }

  /** A view of the store pinned to one tenant, for use outside a run. */
  forTenant(tenantId: string): TenantStoreView {
    return new TenantStoreView(this, validateTenant(tenantId, "forTenant()"));
  }

  async batch<Op extends Operation[]>(
    operations: Op
  ): Promise<OperationResults<Op>> {
    const scoped = operations.map((op) => this.requireScoped(op)) as Op;
    return this.inner.batch(scoped);
  }

  private requireScoped(op: Operation): Operation {
    if ("namespacePrefix" in op) {
      this.assertSentinel(op.namespacePrefix);
      return { ...op, namespacePrefix: op.namespacePrefix.slice(1) };
    }
    if ("namespace" in op) {
      this.assertSentinel(op.namespace);
      return { ...op, namespace: op.namespace.slice(1) };
    }
    // ListNamespacesOperation: the prefix condition must carry the sentinel.
    const conditions = op.matchConditions ?? [];
    const prefixIndex = conditions.findIndex((c) => c.matchType === "prefix");
    if (prefixIndex < 0) throw new UnscopedAccessError(UNSCOPED_MESSAGE);
    this.assertSentinel(conditions[prefixIndex].path as string[]);
    const next = conditions.map((c, i) =>
      i === prefixIndex ? { ...c, path: c.path.slice(1) } : c
    ) as MatchCondition[];
    return { ...op, matchConditions: next };
  }

  private assertSentinel(namespace: string[]): void {
    if (
      namespace[0] !== TENANT_NS_SENTINEL ||
      typeof namespace[1] !== "string" ||
      namespace[1].length === 0
    ) {
      throw new UnscopedAccessError(UNSCOPED_MESSAGE);
    }
  }

  async start(): Promise<void> {
    await this.inner.start();
  }

  async stop(): Promise<void> {
    await this.inner.stop();
  }
}

/**
 * The store surface a tenant view can delegate to: either the
 * `TenantScopedStore` itself (out-of-band) or `config.store` inside a node
 * (an `AsyncBatchedStore`, which implements get/search/put/delete only).
 */
export interface TenantStoreTarget {
  get(namespace: string[], key: string): Promise<Item | null>;
  search(
    namespacePrefix: string[],
    options?: {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      filter?: Record<string, any>;
      limit?: number;
      offset?: number;
      query?: string;
    }
  ): Promise<SearchItem[]>;
  put(
    namespace: string[],
    key: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    value: Record<string, any>
  ): Promise<void>;
  delete(namespace: string[], key: string): Promise<void>;
  listNamespaces?(options?: {
    prefix?: string[];
    suffix?: string[];
    maxDepth?: number;
    limit?: number;
    offset?: number;
  }): Promise<string[][]>;
}

/** All operations rooted at one tenant; namespaces in results come back unprefixed. */
export class TenantStoreView {
  constructor(
    private readonly target: TenantStoreTarget,
    readonly tenant: string
  ) {}

  private scopeNs(namespace: string[]): string[] {
    return [TENANT_NS_SENTINEL, this.tenant, ...namespace];
  }

  private unscopeItem<T extends Item | null>(item: T): T {
    // the inner store returns [tenant, ...rest]; the tenant never leaks out
    if (item) item.namespace = item.namespace.slice(1);
    return item;
  }

  async get(namespace: string[], key: string): Promise<Item | null> {
    return this.unscopeItem(await this.target.get(this.scopeNs(namespace), key));
  }

  async search(
    namespacePrefix: string[],
    options?: Parameters<TenantStoreTarget["search"]>[1]
  ): Promise<SearchItem[]> {
    const items = await this.target.search(
      this.scopeNs(namespacePrefix),
      options
    );
    for (const item of items) this.unscopeItem(item);
    return items;
  }

  async put(
    namespace: string[],
    key: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    value: Record<string, any>
  ): Promise<void> {
    return this.target.put(this.scopeNs(namespace), key, value);
  }

  async delete(namespace: string[], key: string): Promise<void> {
    return this.target.delete(this.scopeNs(namespace), key);
  }

  async listNamespaces(
    options: {
      prefix?: string[];
      suffix?: string[];
      maxDepth?: number;
      limit?: number;
      offset?: number;
    } = {}
  ): Promise<string[][]> {
    if (!this.target.listNamespaces) {
      throw new TenancyError(
        "listNamespaces is not available through config.store (LangGraph " +
          "batches in-graph store access); call it on " +
          "store.forTenant(tenantId) instead."
      );
    }
    const results = await this.target.listNamespaces({
      ...options,
      prefix: this.scopeNs(options.prefix ?? []),
      maxDepth: options.maxDepth == null ? undefined : options.maxDepth + 1,
    });
    return results
      .filter((ns) => ns[0] === this.tenant)
      .map((ns) => ns.slice(1));
  }
}

/**
 * Tenant-scoped store access inside a node. Reads `tenant_id` from the
 * node's config and delegates to `config.store`, so LangGraph's operation
 * batching still applies.
 *
 * ```ts
 * const node = async (state, config) => {
 *   const store = getTenantStore(config);
 *   await store.put(["memories"], "k", { note: "..." });
 * };
 * ```
 */
export function getTenantStore(config: {
  configurable?: Record<string, unknown>;
  store?: TenantStoreTarget;
}): TenantStoreView {
  const tenant = validateTenant(
    config?.configurable?.tenant_id,
    "getTenantStore()"
  );
  if (!config.store) {
    throw new TenancyError(
      "getTenantStore(config): config.store is missing — was the graph " +
        "compiled with a store?"
    );
  }
  return new TenantStoreView(config.store, tenant);
}

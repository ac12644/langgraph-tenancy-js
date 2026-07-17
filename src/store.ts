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
import { emitEvent, type TenancyEventHandler } from "./events.js";
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

export interface TenantScopedStoreOptions {
  /** Called on every refused (unscoped) operation. */
  onEvent?: TenancyEventHandler;
}

export class TenantScopedStore extends BaseStore {
  readonly inner: BaseStore;

  private readonly onEvent?: TenancyEventHandler;

  constructor(inner: BaseStore, options?: TenantScopedStoreOptions) {
    super();
    this.inner = inner;
    this.onEvent = options?.onEvent;
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

  private deny(operation: string): never {
    const error = new UnscopedAccessError(UNSCOPED_MESSAGE);
    emitEvent(this.onEvent, { type: "denied", operation, error });
    throw error;
  }

  private requireScoped(op: Operation): Operation {
    if ("namespacePrefix" in op) {
      this.assertSentinel(op.namespacePrefix, "search");
      return { ...op, namespacePrefix: op.namespacePrefix.slice(1) };
    }
    if ("namespace" in op) {
      this.assertSentinel(op.namespace, "get/put/delete");
      return { ...op, namespace: op.namespace.slice(1) };
    }
    // ListNamespacesOperation: the prefix condition must carry the sentinel.
    const conditions = op.matchConditions ?? [];
    const prefixIndex = conditions.findIndex((c) => c.matchType === "prefix");
    if (prefixIndex < 0) this.deny("listNamespaces");
    this.assertSentinel(
      conditions[prefixIndex].path as string[],
      "listNamespaces"
    );
    const next = conditions.map((c, i) =>
      i === prefixIndex ? { ...c, path: c.path.slice(1) } : c
    ) as MatchCondition[];
    return { ...op, matchConditions: next };
  }

  private assertSentinel(namespace: string[], operation: string): void {
    if (
      namespace[0] !== TENANT_NS_SENTINEL ||
      typeof namespace[1] !== "string" ||
      namespace[1].length === 0
    ) {
      this.deny(operation);
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
    value: Record<string, any>,
    index?: false | string[]
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
    if (!item) return item;
    return { ...item, namespace: item.namespace.slice(1) };
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
    return items.map((item) => this.unscopeItem(item));
  }

  async put(
    namespace: string[],
    key: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    value: Record<string, any>,
    index?: false | string[]
  ): Promise<void> {
    // AsyncBatchedStore.put (what nodes get as config.store) takes no index
    // argument and would drop it silently; refuse instead.
    if (index !== undefined && this.target.put.length < 4) {
      throw new TenancyError(
        "put(..., index): config.store batches operations and drops the " +
          "index argument. Call it on store.forTenant(tenantId) instead."
      );
    }
    return this.target.put(this.scopeNs(namespace), key, value, index);
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

  /**
   * GDPR-style erasure: delete every item stored for this tenant. Works in
   * batches so it stays memory-bounded; returns the number of items deleted.
   * Note: some stores (e.g. `InMemoryStore`) keep now-empty namespace labels
   * visible in `listNamespaces()` — the items themselves are gone.
   */
  async purge(): Promise<number> {
    let deleted = 0;
    for (;;) {
      const items = await this.search([], { limit: 100 });
      if (items.length === 0) return deleted;
      for (const item of items) {
        await this.delete(item.namespace, item.key);
      }
      deleted += items.length;
    }
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

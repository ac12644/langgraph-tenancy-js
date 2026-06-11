/**
 * Tenant-scoped wrapper around any `BaseCheckpointSaver`.
 *
 * - Reads `tenant_id` from `config.configurable` on every call. Missing
 *   tenant -> `TenantRequiredError`. There is no unscoped fallback.
 * - Physically prefixes `thread_id` with the tenant (`acme::thread-1`) before
 *   it reaches the inner saver, and strips the prefix from everything
 *   returned. A wrong-thread_id bug in app code therefore cannot cross a
 *   tenant boundary: the key the database sees is always tenant-qualified.
 * - Blocks the dangerous raw-API escape hatch: `deleteThread()` takes a bare
 *   thread id with no config, so it is refused and redirected to an explicit
 *   `forTenant()` handle.
 * - Optionally records per-tenant token usage from checkpointed messages
 *   into a `UsageLedger` — same integration point, free metering.
 */

import type { RunnableConfig } from "@langchain/core/runnables";
import {
  BaseCheckpointSaver,
  type ChannelVersions,
  type Checkpoint,
  type CheckpointListOptions,
  type CheckpointMetadata,
  type CheckpointTuple,
  type PendingWrite,
} from "@langchain/langgraph-checkpoint";

import { UnscopedAccessError } from "./errors.js";
import { SEP, validateTenant } from "./tenant.js";
import { extractUsage, type UsageLedger } from "./usage.js";

export class TenantScopedCheckpointer extends BaseCheckpointSaver {
  readonly inner: BaseCheckpointSaver;

  readonly usageLedger?: UsageLedger;

  constructor(
    inner: BaseCheckpointSaver,
    options?: { usageLedger?: UsageLedger }
  ) {
    super(inner.serde);
    this.inner = inner;
    this.usageLedger = options?.usageLedger;
  }

  private scope(
    config: RunnableConfig,
    where: string
  ): [string, RunnableConfig] {
    const conf = config?.configurable ?? {};
    const tenant = validateTenant(conf.tenant_id, where);
    return [
      tenant,
      {
        ...config,
        configurable: { ...conf, thread_id: `${tenant}${SEP}${conf.thread_id}` },
      },
    ];
  }

  private unscopeConfig(tenant: string, config: RunnableConfig): RunnableConfig;

  private unscopeConfig(
    tenant: string,
    config?: RunnableConfig
  ): RunnableConfig | undefined;

  private unscopeConfig(
    tenant: string,
    config?: RunnableConfig
  ): RunnableConfig | undefined {
    if (!config) return config;
    const conf = { ...(config.configurable ?? {}) };
    const threadId = conf.thread_id;
    const prefix = `${tenant}${SEP}`;
    if (typeof threadId === "string" && threadId.startsWith(prefix)) {
      conf.thread_id = threadId.slice(prefix.length);
      conf.tenant_id = tenant;
    }
    return { ...config, configurable: conf };
  }

  private unscopeTuple(
    tenant: string,
    tuple?: CheckpointTuple
  ): CheckpointTuple | undefined {
    if (!tuple) return tuple;
    return {
      ...tuple,
      config: this.unscopeConfig(tenant, tuple.config),
      parentConfig: this.unscopeConfig(tenant, tuple.parentConfig),
    };
  }

  async getTuple(config: RunnableConfig): Promise<CheckpointTuple | undefined> {
    const [tenant, scoped] = this.scope(config, "getTuple()");
    return this.unscopeTuple(tenant, await this.inner.getTuple(scoped));
  }

  async *list(
    config: RunnableConfig,
    options?: CheckpointListOptions
  ): AsyncGenerator<CheckpointTuple> {
    // A raw saver lists EVERY tenant's threads when no thread filter is set;
    // here a tenant is mandatory before anything is read.
    const [tenant, scoped] = this.scope(config ?? {}, "list()");
    const scopedOptions = options?.before
      ? { ...options, before: this.scope(options.before, "list({before})")[1] }
      : options;
    for await (const tuple of this.inner.list(scoped, scopedOptions)) {
      yield this.unscopeTuple(tenant, tuple)!;
    }
  }

  async put(
    config: RunnableConfig,
    checkpoint: Checkpoint,
    metadata: CheckpointMetadata,
    newVersions: ChannelVersions
  ): Promise<RunnableConfig> {
    const [tenant, scoped] = this.scope(config, "put()");
    if (this.usageLedger) {
      for (const record of extractUsage(checkpoint)) {
        this.usageLedger.record(tenant, record);
      }
    }
    return this.unscopeConfig(
      tenant,
      await this.inner.put(scoped, checkpoint, metadata, newVersions)
    );
  }

  async putWrites(
    config: RunnableConfig,
    writes: PendingWrite[],
    taskId: string
  ): Promise<void> {
    const [, scoped] = this.scope(config, "putWrites()");
    return this.inner.putWrites(scoped, writes, taskId);
  }

  async deleteThread(_threadId: string): Promise<void> {
    throw new UnscopedAccessError(
      "deleteThread() has no tenant context; " +
        "use forTenant(tenantId).deleteThread(threadId)."
    );
  }

  getNextVersion(current: number | undefined): number {
    return this.inner.getNextVersion(current);
  }

  /** Admin/maintenance handle pinned to one tenant. */
  forTenant(tenantId: string): TenantCheckpointerHandle {
    return new TenantCheckpointerHandle(
      this.inner,
      validateTenant(tenantId, "forTenant()")
    );
  }
}

/**
 * Maintenance operations pre-bound to a single tenant. Exists because
 * `deleteThread` takes a bare thread id with no config, so there is no
 * per-call tenant to read.
 */
export class TenantCheckpointerHandle {
  constructor(
    private readonly inner: BaseCheckpointSaver,
    private readonly tenant: string
  ) {}

  async deleteThread(threadId: string): Promise<void> {
    return this.inner.deleteThread(`${this.tenant}${SEP}${threadId}`);
  }
}

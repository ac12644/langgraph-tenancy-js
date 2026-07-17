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
 *   into a `UsageLedger`, and enforces per-tenant quotas at the same
 *   boundary.
 */

import type { RunnableConfig } from "@langchain/core/runnables";
import {
  BaseCheckpointSaver,
  type ChannelVersions,
  type Checkpoint,
  type CheckpointListOptions,
  type CheckpointMetadata,
  type CheckpointTuple,
  type DeltaChannelHistory,
  type PendingWrite,
} from "@langchain/langgraph-checkpoint";

import {
  QuotaExceededError,
  TenancyError,
  UnscopedAccessError,
} from "./errors.js";
import { emitEvent, type TenancyEventHandler } from "./events.js";
import { findViolation, type QuotaConfig } from "./quota.js";
import { SEP, validateTenant, validateThreadId } from "./tenant.js";
import {
  BoundedStringSet,
  extractUsage,
  extractUsageFromValues,
  type UsageLedger,
  type UsageRecord,
} from "./usage.js";

export interface TenantScopedCheckpointerOptions {
  /** Receives per-tenant usage extracted from every checkpoint. */
  usageLedger?: UsageLedger;
  /** Per-tenant limits, enforced in `put()` before anything is written. */
  quota?: QuotaConfig;
  /** Called on every denial, quota hit, and ledger failure. */
  onEvent?: TenancyEventHandler;
  /**
   * What happens when `usageLedger.record()` throws. `"swallow"` (default)
   * emits a `ledger_error` event and lets the checkpoint write proceed —
   * metering must not take down persistence. `"throw"` fails the write.
   */
  ledgerErrors?: "swallow" | "throw";
  /**
   * Capacity of the message-id dedup cache that keeps `record()` from being
   * called again for messages already attributed (checkpoints re-serialize
   * the whole conversation every turn). Default 50,000.
   */
  usageDedupCapacity?: number;
}

export class TenantScopedCheckpointer extends BaseCheckpointSaver {
  readonly inner: BaseCheckpointSaver;

  readonly usageLedger?: UsageLedger;

  private readonly quota?: QuotaConfig;

  private readonly onEvent?: TenancyEventHandler;

  private readonly ledgerErrors: "swallow" | "throw";

  private readonly recorded: BoundedStringSet;

  constructor(
    inner: BaseCheckpointSaver,
    options?: TenantScopedCheckpointerOptions
  ) {
    super(inner.serde);
    this.inner = inner;
    this.usageLedger = options?.usageLedger;
    this.quota = options?.quota;
    this.onEvent = options?.onEvent;
    this.ledgerErrors = options?.ledgerErrors ?? "swallow";
    this.recorded = new BoundedStringSet(options?.usageDedupCapacity ?? 50_000);
    if (this.quota && !this.quota.usage && !this.usageLedger?.totals) {
      throw new TenancyError(
        "quota enforcement needs a usage source: pass quota.usage, or a " +
          "usageLedger that implements totals()."
      );
    }
  }

  /** Run a validation, emitting a `denied` event if it refuses. */
  private validate<T>(operation: string, fn: () => T): T {
    try {
      return fn();
    } catch (error) {
      if (error instanceof TenancyError) {
        emitEvent(this.onEvent, { type: "denied", operation, error });
      }
      throw error;
    }
  }

  private scope(
    config: RunnableConfig,
    where: string
  ): [string, RunnableConfig] {
    const conf = config?.configurable ?? {};
    const tenant = this.validate(where, () =>
      validateTenant(conf.tenant_id, where)
    );
    const threadId = this.validate(where, () =>
      validateThreadId(conf.thread_id, where)
    );
    return [
      tenant,
      {
        ...config,
        configurable: { ...conf, thread_id: `${tenant}${SEP}${threadId}` },
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

  /**
   * With a `thread_id` in the config: that tenant thread's checkpoints.
   * Without one: every checkpoint belonging to the tenant, across all of its
   * threads (a raw saver would list EVERY tenant's threads here; a tenant is
   * mandatory before anything is read).
   */
  async *list(
    config: RunnableConfig,
    options?: CheckpointListOptions
  ): AsyncGenerator<CheckpointTuple> {
    const conf = config?.configurable ?? {};
    const tenant = this.validate("list()", () =>
      validateTenant(conf.tenant_id, "list()")
    );
    const scopedOptions = options?.before
      ? { ...options, before: this.scope(options.before, "list({before})")[1] }
      : options;

    if (conf.thread_id != null) {
      const [, scoped] = this.scope(config, "list()");
      for await (const tuple of this.inner.list(scoped, scopedOptions)) {
        yield this.unscopeTuple(tenant, tuple)!;
      }
      return;
    }

    // Tenant-wide listing: enumerate the saver and keep only this tenant's
    // physical prefix. The inner limit must not apply pre-filter, so it is
    // re-applied after filtering.
    const prefix = `${tenant}${SEP}`;
    let remaining = scopedOptions?.limit ?? Infinity;
    if (remaining <= 0) return;
    const innerOptions = scopedOptions
      ? { ...scopedOptions, limit: undefined }
      : undefined;
    for await (const tuple of this.inner.list(config, innerOptions)) {
      const threadId = tuple.config?.configurable?.thread_id;
      if (typeof threadId !== "string" || !threadId.startsWith(prefix)) {
        continue;
      }
      yield this.unscopeTuple(tenant, tuple)!;
      if (--remaining <= 0) return;
    }
  }

  async put(
    config: RunnableConfig,
    checkpoint: Checkpoint,
    metadata: CheckpointMetadata,
    newVersions: ChannelVersions
  ): Promise<RunnableConfig> {
    const [tenant, scoped] = this.scope(config, "put()");
    await this.enforceQuota(tenant, "put()", metadata);
    await this.recordUsage(tenant, extractUsage(checkpoint), "put()");
    return this.unscopeConfig(
      tenant,
      await this.inner.put(scoped, checkpoint, metadata, newVersions)
    );
  }

  private async enforceQuota(
    tenant: string,
    operation: string,
    metadata?: CheckpointMetadata
  ): Promise<void> {
    if (!this.quota) return;
    // Never kill a run mid-flight: by the time a "loop" checkpoint arrives
    // the model spend already happened, and that state must not be lost.
    // New runs ("input") and manual updates are the enforcement points.
    if (metadata?.source === "loop") return;
    const limits = await this.quota.limits(tenant);
    if (!limits) return;
    const usage = this.quota.usage
      ? await this.quota.usage(tenant)
      : await this.usageLedger!.totals!(tenant);
    const violation = findViolation(tenant, usage, limits);
    if (!violation) return;
    const error = new QuotaExceededError(violation);
    emitEvent(this.onEvent, {
      type: "quota_exceeded",
      operation,
      tenantId: tenant,
      error,
    });
    throw error;
  }

  private async recordUsage(
    tenant: string,
    records: UsageRecord[],
    operation: string
  ): Promise<void> {
    if (!this.usageLedger) return;
    for (const record of records) {
      const key = `${tenant}${SEP}${record.messageId}`;
      if (this.recorded.has(key)) continue;
      // reserve BEFORE awaiting: putWrites() and put() can carry the same
      // message concurrently, and a late-added key would let both record it
      this.recorded.add(key);
      try {
        await this.usageLedger.record(tenant, record);
      } catch (error) {
        // un-reserve so a failed ledger call is retried on the next
        // checkpoint instead of being lost
        this.recorded.delete(key);
        emitEvent(this.onEvent, {
          type: "ledger_error",
          operation,
          tenantId: tenant,
          error,
        });
        if (this.ledgerErrors === "throw") throw error;
      }
    }
  }

  async putWrites(
    config: RunnableConfig,
    writes: PendingWrite[],
    taskId: string
  ): Promise<void> {
    const [tenant, scoped] = this.scope(config, "putWrites()");
    // Usage is extracted from writes as well as checkpoints: delta channels
    // (beta) store only a sentinel in checkpoint.channel_values, so messages
    // may ONLY ever appear here. Message-id dedup prevents double counting
    // when the same message later shows up in a full checkpoint.
    await this.recordUsage(
      tenant,
      extractUsageFromValues(writes.map(([, value]) => value)),
      "putWrites()"
    );
    return this.inner.putWrites(scoped, writes, taskId);
  }

  async deleteThread(_threadId: string): Promise<void> {
    const error = new UnscopedAccessError(
      "deleteThread() has no tenant context; " +
        "use forTenant(tenantId).deleteThread(threadId)."
    );
    emitEvent(this.onEvent, {
      type: "denied",
      operation: "deleteThread()",
      error,
    });
    throw error;
  }

  /**
   * Delegates to the inner saver (which may override the base-class walk
   * with direct storage access) under a tenant-scoped config. The result
   * contains only channel writes and seeds — no thread ids to unscope.
   */
  async getDeltaChannelHistory(options: {
    config: RunnableConfig;
    channels: string[];
  }): Promise<Record<string, DeltaChannelHistory>> {
    const [, scoped] = this.scope(options.config, "getDeltaChannelHistory()");
    return this.inner.getDeltaChannelHistory({ ...options, config: scoped });
  }

  getNextVersion(current: number | undefined): number {
    return this.inner.getNextVersion(current);
  }

  /** Admin/maintenance handle pinned to one tenant. */
  forTenant(tenantId: string): TenantCheckpointerHandle {
    return new TenantCheckpointerHandle(
      this.inner,
      this.validate("forTenant()", () =>
        validateTenant(tenantId, "forTenant()")
      )
    );
  }
}

/**
 * Maintenance operations pre-bound to a single tenant: deletion, tenant-wide
 * enumeration, GDPR-style purge, and migration of pre-tenancy threads.
 * Exists because these operations have no per-call config to read a tenant
 * from.
 */
export class TenantCheckpointerHandle {
  constructor(
    private readonly inner: BaseCheckpointSaver,
    readonly tenant: string
  ) {}

  private scopedId(threadId: string): string {
    return `${this.tenant}${SEP}${threadId}`;
  }

  async deleteThread(threadId: string): Promise<void> {
    return this.inner.deleteThread(this.scopedId(threadId));
  }

  /**
   * All thread ids belonging to this tenant. Enumerates the saver's full
   * checkpoint listing and filters by physical prefix — O(all checkpoints),
   * intended for admin/maintenance paths, not per-request use.
   */
  async listThreads(): Promise<string[]> {
    const prefix = `${this.tenant}${SEP}`;
    const threads = new Set<string>();
    for await (const tuple of this.inner.list({})) {
      const threadId = tuple.config?.configurable?.thread_id;
      if (typeof threadId === "string" && threadId.startsWith(prefix)) {
        threads.add(threadId.slice(prefix.length));
      }
    }
    return [...threads];
  }

  /**
   * GDPR-style erasure: delete every checkpointed thread for this tenant.
   * Returns the (logical) thread ids that were deleted.
   */
  async purge(): Promise<string[]> {
    const threads = await this.listThreads();
    for (const threadId of threads) {
      await this.inner.deleteThread(this.scopedId(threadId));
    }
    return threads;
  }

  /**
   * Migrate a pre-tenancy (unprefixed) thread into this tenant, checkpoint
   * by checkpoint — order, parentage, metadata, and pending writes are
   * preserved. This is the adoption path for deployments that already have
   * data written without tenant scoping. Returns the number of checkpoints
   * migrated.
   */
  async adoptThread(
    threadId: string,
    options?: { deleteSource?: boolean }
  ): Promise<number> {
    if (threadId.includes(SEP)) {
      throw new TenancyError(
        `adoptThread(): source thread id ${JSON.stringify(threadId)} ` +
          `contains ${JSON.stringify(SEP)} — it already looks tenant-scoped.`
      );
    }
    const target = this.scopedId(threadId);
    if (await this.inner.getTuple({ configurable: { thread_id: target } })) {
      throw new TenancyError(
        `adoptThread(): tenant ${JSON.stringify(this.tenant)} already has ` +
          `a thread ${JSON.stringify(threadId)}.`
      );
    }

    const tuples: CheckpointTuple[] = [];
    for await (const tuple of this.inner.list({
      configurable: { thread_id: threadId },
    })) {
      tuples.push(tuple);
    }
    tuples.reverse(); // savers list newest-first; replay oldest-first

    for (const tuple of tuples) {
      const conf = tuple.config.configurable ?? {};
      const checkpointNs = (conf.checkpoint_ns as string) ?? "";
      await this.inner.put(
        {
          configurable: {
            thread_id: target,
            checkpoint_ns: checkpointNs,
            // the saver contract: config.checkpoint_id on put() is the PARENT
            checkpoint_id: tuple.parentConfig?.configurable?.checkpoint_id,
          },
        },
        tuple.checkpoint,
        tuple.metadata ?? ({} as CheckpointMetadata),
        tuple.checkpoint.channel_versions ?? {}
      );
      if (tuple.pendingWrites?.length) {
        const writeConfig = {
          configurable: {
            thread_id: target,
            checkpoint_ns: checkpointNs,
            checkpoint_id: tuple.checkpoint.id,
          },
        };
        const byTask = new Map<string, PendingWrite[]>();
        for (const [taskId, channel, value] of tuple.pendingWrites) {
          const writes = byTask.get(taskId) ?? [];
          writes.push([channel, value]);
          byTask.set(taskId, writes);
        }
        for (const [taskId, writes] of byTask) {
          await this.inner.putWrites(writeConfig, writes, taskId);
        }
      }
    }

    if (options?.deleteSource && tuples.length > 0) {
      await this.inner.deleteThread(threadId);
    }
    return tuples.length;
  }
}

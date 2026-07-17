/**
 * Production-feature tests: quota enforcement, observability events, ledger
 * error isolation, the admin/GDPR surface, and store index passthrough.
 */

import { describe, expect, it } from "vitest";
import { AIMessage } from "@langchain/core/messages";
import {
  InMemoryStore,
  MemorySaver,
} from "@langchain/langgraph-checkpoint";

import {
  BoundedStringSet,
  InMemoryUsageLedger,
  QuotaExceededError,
  TenancyError,
  TenantScopedCheckpointer,
  TenantScopedStore,
  type TenancyEvent,
  type UsageLedger,
} from "../src/index.js";
import { cfg, makeGraph } from "./helpers.js";

describe("quota enforcement", () => {
  it("blocks an over-quota tenant's next run, not the run that crossed the line", async () => {
    const ledger = new InMemoryUsageLedger();
    const events: TenancyEvent[] = [];
    const graph = makeGraph({
      checkpointer: new TenantScopedCheckpointer(new MemorySaver(), {
        usageLedger: ledger,
        quota: {
          limits: (tenant) =>
            tenant === "acme" ? { maxMessages: 1 } : undefined,
        },
        onEvent: (e) => events.push(e),
      }),
    });

    // the turn that reaches the limit still completes...
    await graph.invoke({ messages: ["q1"] }, cfg("acme"));
    expect(ledger.totals("acme").messages).toBe(1);

    // ...the next run is refused at its first checkpoint
    await expect(
      graph.invoke({ messages: ["q2"] }, cfg("acme"))
    ).rejects.toThrow(QuotaExceededError);

    // unlimited tenants are unaffected
    await graph.invoke({ messages: ["q1"] }, cfg("globex"));

    const quotaEvents = events.filter((e) => e.type === "quota_exceeded");
    expect(quotaEvents).toHaveLength(1);
    expect(quotaEvents[0].tenantId).toBe("acme");
    expect(quotaEvents[0].error.violation).toEqual({
      tenantId: "acme",
      field: "messages",
      used: 1,
      limit: 1,
    });
  });

  it("enforces token budgets", async () => {
    const ledger = new InMemoryUsageLedger();
    const graph = makeGraph({
      checkpointer: new TenantScopedCheckpointer(new MemorySaver(), {
        usageLedger: ledger,
        quota: { limits: () => ({ maxTotalTokens: 20 }) },
      }),
    });
    await graph.invoke({ messages: ["q1"] }, cfg("acme")); // 15 tokens
    await graph.invoke({ messages: ["q2"] }, cfg("acme")); // 15 < 20, allowed -> 30
    await expect(
      graph.invoke({ messages: ["q3"] }, cfg("acme")) // 30 >= 20, blocked
    ).rejects.toThrow(QuotaExceededError);
  });

  it("supports an external usage source instead of the ledger", async () => {
    const graph = makeGraph({
      checkpointer: new TenantScopedCheckpointer(new MemorySaver(), {
        quota: {
          limits: () => ({ maxTotalTokens: 100 }),
          usage: () => ({
            inputTokens: 0,
            outputTokens: 0,
            totalTokens: 500,
            messages: 3,
          }),
        },
      }),
    });
    await expect(
      graph.invoke({ messages: ["hi"] }, cfg("acme"))
    ).rejects.toThrow(QuotaExceededError);
  });

  it("refuses quota config without any usage source", () => {
    const ledgerWithoutTotals: UsageLedger = { record: () => {} };
    expect(
      () =>
        new TenantScopedCheckpointer(new MemorySaver(), {
          usageLedger: ledgerWithoutTotals,
          quota: { limits: () => ({ maxMessages: 1 }) },
        })
    ).toThrow(TenancyError);
  });
});

describe("observability events", () => {
  it("emits a denied event when a tenant is missing", async () => {
    const events: TenancyEvent[] = [];
    const saver = new TenantScopedCheckpointer(new MemorySaver(), {
      onEvent: (e) => events.push(e),
    });
    await expect(
      saver.getTuple({ configurable: { thread_id: "t1" } })
    ).rejects.toThrow();
    await expect(saver.deleteThread("t1")).rejects.toThrow();

    expect(events.map((e) => e.type)).toEqual(["denied", "denied"]);
    expect(events.map((e) => (e.type === "denied" ? e.operation : ""))).toEqual(
      ["getTuple()", "deleteThread()"]
    );
  });

  it("emits a denied event on unscoped store access", async () => {
    const events: TenancyEvent[] = [];
    const store = new TenantScopedStore(new InMemoryStore(), {
      onEvent: (e) => events.push(e),
    });
    await expect(store.get(["memories"], "k")).rejects.toThrow();
    expect(events).toHaveLength(1);
    expect(events[0].type).toBe("denied");
  });

  it("a throwing event handler never breaks the data path", async () => {
    const graph = makeGraph({
      checkpointer: new TenantScopedCheckpointer(new MemorySaver(), {
        onEvent: () => {
          throw new Error("observer bug");
        },
      }),
    });
    await graph.invoke({ messages: ["hi"] }, cfg("acme")); // must not throw
  });
});

describe("ledger error isolation", () => {
  const failingLedger = (): UsageLedger & { calls: number } => ({
    calls: 0,
    record() {
      this.calls += 1;
      throw new Error("metering backend down");
    },
  });

  it("a throwing ledger does not fail checkpoint writes by default", async () => {
    const ledger = failingLedger();
    const events: TenancyEvent[] = [];
    const graph = makeGraph({
      checkpointer: new TenantScopedCheckpointer(new MemorySaver(), {
        usageLedger: ledger,
        onEvent: (e) => events.push(e),
      }),
    });
    await graph.invoke({ messages: ["hi"] }, cfg("acme")); // survives
    expect(ledger.calls).toBeGreaterThan(0);
    expect(events.some((e) => e.type === "ledger_error")).toBe(true);
  });

  it("ledgerErrors: 'throw' fails the write instead", async () => {
    const graph = makeGraph({
      checkpointer: new TenantScopedCheckpointer(new MemorySaver(), {
        usageLedger: failingLedger(),
        ledgerErrors: "throw",
      }),
    });
    await expect(
      graph.invoke({ messages: ["hi"] }, cfg("acme"))
    ).rejects.toThrow("metering backend down");
  });

  it("supports async ledgers", async () => {
    const recorded: string[] = [];
    const asyncLedger: UsageLedger = {
      async record(tenantId) {
        await new Promise((r) => setTimeout(r, 1));
        recorded.push(tenantId);
      },
    };
    const graph = makeGraph({
      checkpointer: new TenantScopedCheckpointer(new MemorySaver(), {
        usageLedger: asyncLedger,
      }),
    });
    await graph.invoke({ messages: ["hi"] }, cfg("acme"));
    expect(recorded).toEqual(["acme"]);
  });

  it("BoundedStringSet evicts oldest entries at capacity", () => {
    const set = new BoundedStringSet(2);
    set.add("a");
    set.add("b");
    set.add("c");
    expect(set.size).toBe(2);
    expect(set.has("a")).toBe(false);
    expect(set.has("b")).toBe(true);
    expect(set.has("c")).toBe(true);
  });
});

describe("admin / GDPR surface", () => {
  it("listThreads and purge only touch the handle's tenant", async () => {
    const saver = new TenantScopedCheckpointer(new MemorySaver());
    const graph = makeGraph({ checkpointer: saver });
    await graph.invoke({ messages: ["a"] }, cfg("acme", "t1"));
    await graph.invoke({ messages: ["b"] }, cfg("acme", "t2"));
    await graph.invoke({ messages: ["c"] }, cfg("globex", "t1"));

    const acme = saver.forTenant("acme");
    expect((await acme.listThreads()).sort()).toEqual(["t1", "t2"]);

    const purged = await acme.purge();
    expect(purged.sort()).toEqual(["t1", "t2"]);
    expect(await acme.listThreads()).toEqual([]);
    expect(await saver.getTuple(cfg("acme", "t1"))).toBeUndefined();
    // the other tenant's data is intact
    expect(await saver.getTuple(cfg("globex", "t1"))).toBeDefined();
  });

  it("adoptThread migrates a pre-tenancy thread under the tenant", async () => {
    // a deployment that ran WITHOUT tenancy: raw saver, unprefixed thread
    const inner = new MemorySaver();
    const legacyGraph = makeGraph({ checkpointer: inner });
    const legacyCfg = { configurable: { thread_id: "legacy" } };
    await legacyGraph.invoke({ messages: ["old-1"] }, legacyCfg);
    await legacyGraph.invoke({ messages: ["old-2"] }, legacyCfg);

    // now tenancy is adopted
    const saver = new TenantScopedCheckpointer(inner);
    const migrated = await saver
      .forTenant("acme")
      .adoptThread("legacy", { deleteSource: true });
    expect(migrated).toBeGreaterThan(0);

    // full history is visible through the tenant scope...
    const graph = makeGraph({ checkpointer: saver });
    const state = await graph.getState(cfg("acme", "legacy"));
    expect(state.values.messages[0]).toBe("old-1");
    expect(state.values.messages).toHaveLength(4);

    // ...the conversation continues where it left off...
    await graph.invoke({ messages: ["new-1"] }, cfg("acme", "legacy"));
    const after = await graph.getState(cfg("acme", "legacy"));
    expect(after.values.messages).toHaveLength(6);

    // ...and the unscoped original is gone
    expect(
      await inner.getTuple({ configurable: { thread_id: "legacy" } })
    ).toBeUndefined();
  });

  it("adoptThread refuses suspicious sources and existing targets", async () => {
    const inner = new MemorySaver();
    const saver = new TenantScopedCheckpointer(inner);
    const graph = makeGraph({ checkpointer: saver });
    await graph.invoke({ messages: ["hi"] }, cfg("acme", "taken"));

    const acme = saver.forTenant("acme");
    await expect(acme.adoptThread("x::y")).rejects.toThrow(TenancyError);
    await expect(acme.adoptThread("taken")).rejects.toThrow(TenancyError);
  });

  it("store purge erases exactly one tenant", async () => {
    const store = new TenantScopedStore(new InMemoryStore());
    const acme = store.forTenant("acme");
    for (let i = 0; i < 7; i += 1) {
      await acme.put(["memories"], `k${i}`, { i });
      await acme.put(["profile", "deep"], `p${i}`, { i });
    }
    await store.forTenant("globex").put(["memories"], "keep", { safe: true });

    expect(await acme.purge()).toBe(14);
    expect(await acme.search(["memories"])).toEqual([]);
    // every item is gone (some stores keep empty namespace labels around)
    expect(await acme.search([])).toEqual([]);
    expect(
      (await store.forTenant("globex").get(["memories"], "keep"))?.value
    ).toEqual({ safe: true });
  });
});

describe("store index passthrough", () => {
  it("forwards the index argument outside runs", async () => {
    const seen: unknown[] = [];
    const inner = new InMemoryStore();
    const originalBatch = inner.batch.bind(inner);
    inner.batch = async (ops) => {
      seen.push(...ops.map((op) => ("index" in op ? op.index : undefined)));
      return originalBatch(ops);
    };
    const store = new TenantScopedStore(inner);
    await store.forTenant("acme").put(["docs"], "k", { text: "x" }, ["text"]);
    expect(seen).toEqual([["text"]]);
  });

  it("fails loudly when index would be dropped by a batched store", async () => {
    // config.store inside nodes is an AsyncBatchedStore whose put() takes no
    // index argument; simulate that 3-arity surface
    const batchedLike = {
      get: async () => null,
      search: async () => [],
      put: async (_ns: string[], _key: string, _value: object) => {},
      delete: async () => {},
    };
    const { TenantStoreView } = await import("../src/store.js");
    const scoped = new TenantStoreView(batchedLike, "acme");
    await expect(
      scoped.put(["docs"], "k", { text: "x" }, ["text"])
    ).rejects.toThrow(TenancyError);
    // without index it works fine
    await scoped.put(["docs"], "k", { text: "x" });
  });

  it("meters usage arriving only via putWrites (delta-channel checkpoints)", async () => {
    // DeltaChannel (beta) stores a sentinel in checkpoint.channel_values;
    // messages then flow ONLY through putWrites. Simulate that path directly.
    const ledger = new InMemoryUsageLedger();
    const saver = new TenantScopedCheckpointer(new MemorySaver(), {
      usageLedger: ledger,
    });
    const message = new AIMessage({
      id: "delta-msg-1",
      content: "x",
      usage_metadata: { input_tokens: 7, output_tokens: 3, total_tokens: 10 },
    });
    const config = {
      configurable: {
        thread_id: "t1",
        tenant_id: "acme",
        checkpoint_ns: "",
        checkpoint_id: "ckpt-1",
      },
    };
    await saver.putWrites(config, [["messages", [message]]], "task-1");
    expect(ledger.totals("acme").totalTokens).toBe(10);

    // the same message appearing later in a full checkpoint is not
    // double-counted
    await saver.put(
      config,
      {
        v: 4,
        id: "ckpt-2",
        ts: "2026-01-01T00:00:00.000Z",
        channel_values: { messages: [message] },
        channel_versions: {},
        versions_seen: {},
      },
      { source: "loop", step: 1, parents: {} },
      {}
    );
    expect(ledger.totals("acme").totalTokens).toBe(10);
    expect(ledger.totals("acme").messages).toBe(1);
  });

  it("checkpoint metadata written by the graph never gains extra puts", async () => {
    // guard: AIMessage extraction still works end to end after refactors
    const ledger = new InMemoryUsageLedger();
    const saver = new TenantScopedCheckpointer(new MemorySaver(), {
      usageLedger: ledger,
    });
    const graph = makeGraph({ checkpointer: saver });
    await graph.invoke({ messages: [new AIMessage({ id: "m1", content: "x" })] }, cfg("acme"));
    expect(ledger.totals("acme").messages).toBe(1); // only the agent's reply has usage
  });
});

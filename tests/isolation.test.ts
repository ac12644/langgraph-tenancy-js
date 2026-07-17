/**
 * Adversarial isolation tests. Every test attempts a cross-tenant access the
 * raw LangGraph.js API allows, and asserts the wrapper makes it either
 * impossible or an explicit error. Runs against the real MemorySaver /
 * InMemoryStore from @langchain/langgraph-checkpoint.
 */

import { describe, expect, it } from "vitest";
import {
  InMemoryStore,
  MemorySaver,
} from "@langchain/langgraph-checkpoint";

import {
  InMemoryUsageLedger,
  InvalidTenantError,
  TenancyError,
  TenantRequiredError,
  TenantScopedCheckpointer,
  TenantScopedStore,
  UnscopedAccessError,
  getTenantStore,
} from "../src/index.js";
import { cfg, makeGraph } from "./helpers.js";

describe("checkpointer isolation", () => {
  it("same thread_id for different tenants does not collide", async () => {
    const graph = makeGraph({
      checkpointer: new TenantScopedCheckpointer(new MemorySaver()),
    });
    await graph.invoke({ messages: ["from acme"] }, cfg("acme"));
    await graph.invoke({ messages: ["from globex"] }, cfg("globex"));

    const acme = (await graph.getState(cfg("acme"))).values.messages;
    const globex = (await graph.getState(cfg("globex"))).values.messages;
    expect(acme[0]).toBe("from acme");
    expect(acme).toHaveLength(2);
    expect(globex[0]).toBe("from globex");
    expect(globex).toHaveLength(2);
    expect(JSON.stringify(acme)).not.toContain("globex");
  });

  it("missing tenant_id raises instead of leaking", async () => {
    const graph = makeGraph({
      checkpointer: new TenantScopedCheckpointer(new MemorySaver()),
    });
    await expect(
      graph.invoke({ messages: ["hi"] }, { configurable: { thread_id: "t1" } })
    ).rejects.toThrow(TenantRequiredError);
  });

  it("missing thread_id raises instead of writing to 'tenant::undefined'", async () => {
    const saver = new TenantScopedCheckpointer(new MemorySaver());
    await expect(
      saver.getTuple({ configurable: { tenant_id: "acme" } })
    ).rejects.toThrow(TenancyError);
  });

  it("tenant_id cannot contain the separator", async () => {
    const graph = makeGraph({
      checkpointer: new TenantScopedCheckpointer(new MemorySaver()),
    });
    await expect(
      graph.invoke({ messages: ["hi"] }, cfg("acme::evil"))
    ).rejects.toThrow(InvalidTenantError);
  });

  it("tenant_id is restricted to a storage-safe charset", async () => {
    const graph = makeGraph({
      checkpointer: new TenantScopedCheckpointer(new MemorySaver()),
    });
    // '.' is a namespace separator in some physical backends
    for (const evil of ["acme.evil", "acme/evil", "acme evil", "a".repeat(65)]) {
      await expect(
        graph.invoke({ messages: ["hi"] }, cfg(evil))
      ).rejects.toThrow(InvalidTenantError);
    }
  });

  it("list() without a tenant is refused", async () => {
    const saver = new TenantScopedCheckpointer(new MemorySaver());
    await makeGraph({ checkpointer: saver }).invoke(
      { messages: ["hi"] },
      cfg("acme")
    );
    const drain = async () => {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      for await (const _ of saver.list({})) {
        // should never yield
      }
    };
    await expect(drain()).rejects.toThrow(TenantRequiredError);
  });

  it("list() only sees the caller's tenant", async () => {
    const saver = new TenantScopedCheckpointer(new MemorySaver());
    const graph = makeGraph({ checkpointer: saver });
    await graph.invoke({ messages: ["a"] }, cfg("acme"));
    await graph.invoke({ messages: ["b"] }, cfg("globex"));

    const seen: string[] = [];
    for await (const tuple of saver.list(cfg("acme"))) {
      seen.push(tuple.config.configurable?.thread_id as string);
      expect(JSON.stringify(tuple.checkpoint.channel_values)).not.toContain(
        "globex"
      );
    }
    expect(seen.length).toBeGreaterThan(0);
    expect(seen.every((t) => t === "t1")).toBe(true);
  });

  it("tenant-wide list() (no thread_id) spans the tenant's threads and only those", async () => {
    const saver = new TenantScopedCheckpointer(new MemorySaver());
    const graph = makeGraph({ checkpointer: saver });
    await graph.invoke({ messages: ["a"] }, cfg("acme", "t1"));
    await graph.invoke({ messages: ["b"] }, cfg("acme", "t2"));
    await graph.invoke({ messages: ["c"] }, cfg("globex", "t3"));

    const threads = new Set<string>();
    for await (const tuple of saver.list({
      configurable: { tenant_id: "acme" },
    })) {
      threads.add(tuple.config.configurable?.thread_id as string);
      expect(tuple.config.configurable?.tenant_id).toBe("acme");
      expect(JSON.stringify(tuple.checkpoint.channel_values)).not.toContain(
        "globex"
      );
    }
    expect([...threads].sort()).toEqual(["t1", "t2"]);

    // limit applies AFTER tenant filtering
    const limited: unknown[] = [];
    for await (const tuple of saver.list(
      { configurable: { tenant_id: "acme" } },
      { limit: 2 }
    )) {
      limited.push(tuple);
    }
    expect(limited).toHaveLength(2);
  });

  it("deleteThread requires a tenant handle and only touches that tenant", async () => {
    const saver = new TenantScopedCheckpointer(new MemorySaver());
    const graph = makeGraph({ checkpointer: saver });
    await graph.invoke({ messages: ["a"] }, cfg("acme"));
    await graph.invoke({ messages: ["b"] }, cfg("globex"));

    await expect(saver.deleteThread("t1")).rejects.toThrow(UnscopedAccessError);

    await saver.forTenant("acme").deleteThread("t1");
    expect(await saver.getTuple(cfg("acme"))).toBeUndefined();
    expect(await saver.getTuple(cfg("globex"))).toBeDefined();
  });

  it("getDeltaChannelHistory requires a tenant and stays scoped", async () => {
    const inner = new MemorySaver();
    const saver = new TenantScopedCheckpointer(inner);
    const graph = makeGraph({ checkpointer: saver });
    await graph.invoke({ messages: ["a"] }, cfg("acme"));
    await graph.invoke({ messages: ["b"] }, cfg("globex"));

    await expect(
      saver.getDeltaChannelHistory({
        config: { configurable: { thread_id: "t1" } },
        channels: ["messages"],
      })
    ).rejects.toThrow(TenantRequiredError);

    // delegates to the inner saver's (possibly optimized) implementation
    // under the tenant-prefixed key
    const viaWrapper = await saver.getDeltaChannelHistory({
      config: cfg("acme"),
      channels: ["messages"],
    });
    const viaInner = await inner.getDeltaChannelHistory({
      config: { configurable: { thread_id: "acme::t1" } },
      channels: ["messages"],
    });
    expect(viaWrapper).toEqual(viaInner);
    expect(JSON.stringify(viaWrapper)).not.toContain("globex");
  });

  it("storage keys are physically tenant-prefixed", async () => {
    const inner = new MemorySaver();
    await makeGraph({
      checkpointer: new TenantScopedCheckpointer(inner),
    }).invoke({ messages: ["a"] }, cfg("acme"));

    const rawThreads = new Set<string>();
    for await (const tuple of inner.list({})) {
      rawThreads.add(tuple.config.configurable?.thread_id as string);
    }
    expect([...rawThreads]).toEqual(["acme::t1"]);
  });
});

describe("store isolation", () => {
  it("namespaces are isolated per tenant", async () => {
    const store = new TenantScopedStore(new InMemoryStore());
    const graph = makeGraph({
      checkpointer: new TenantScopedCheckpointer(new MemorySaver()),
      store,
    });
    await graph.invoke({ messages: ["secret-acme"] }, cfg("acme"));
    await graph.invoke({ messages: ["secret-globex"] }, cfg("globex"));

    const acmeItems = await store.forTenant("acme").search(["memories"]);
    const globexItems = await store.forTenant("globex").search(["memories"]);
    expect(acmeItems.map((i) => i.value.last)).toEqual(["secret-acme"]);
    expect(globexItems.map((i) => i.value.last)).toEqual(["secret-globex"]);
    // returned namespaces are unprefixed — the tenant segment never leaks out
    expect(acmeItems[0].namespace).toEqual(["memories"]);
  });

  it("raw config.store access inside a node fails closed", async () => {
    const store = new TenantScopedStore(new InMemoryStore());
    const graph = makeGraph({
      checkpointer: new TenantScopedCheckpointer(new MemorySaver()),
      store,
      rawStoreAccess: true,
    });
    await expect(
      graph.invoke({ messages: ["hi"] }, cfg("acme"))
    ).rejects.toThrow(UnscopedAccessError);
    // and nothing was written
    expect(await store.forTenant("acme").get(["memories"], "leak")).toBeNull();
  });

  it("direct store access without a tenant is refused", async () => {
    const store = new TenantScopedStore(new InMemoryStore());
    await expect(store.get(["memories"], "k")).rejects.toThrow(
      UnscopedAccessError
    );
    await expect(store.search(["memories"])).rejects.toThrow(
      UnscopedAccessError
    );
    await expect(store.listNamespaces()).rejects.toThrow(UnscopedAccessError);
  });

  it("listNamespaces only sees the caller's tenant", async () => {
    const store = new TenantScopedStore(new InMemoryStore());
    await store.forTenant("acme").put(["memories", "work"], "k", { v: 1 });
    await store.forTenant("globex").put(["memories", "home"], "k", { v: 2 });
    expect(await store.forTenant("acme").listNamespaces()).toEqual([
      ["memories", "work"],
    ]);
  });

  it("getTenantStore requires a tenant in config", () => {
    expect(() =>
      getTenantStore({ configurable: {}, store: new InMemoryStore() })
    ).toThrow(TenantRequiredError);
  });
});

describe("usage metering", () => {
  it("attributes usage per tenant and dedupes re-checkpointed messages", async () => {
    const ledger = new InMemoryUsageLedger();
    const graph = makeGraph({
      checkpointer: new TenantScopedCheckpointer(new MemorySaver(), {
        usageLedger: ledger,
      }),
    });

    await graph.invoke({ messages: ["q1"] }, cfg("acme"));
    await graph.invoke({ messages: ["q2"] }, cfg("acme")); // 2nd turn, same thread
    await graph.invoke({ messages: ["q1"] }, cfg("globex"));

    const acme = ledger.totals("acme");
    const globex = ledger.totals("globex");
    expect(acme.messages).toBe(2);
    expect(acme.totalTokens).toBe(30);
    expect(globex.messages).toBe(1);
    expect(globex.totalTokens).toBe(15);
    expect(acme.byModel).toEqual({ "claude-sonnet-4-6": 30 });
  });
});

/**
 * Real-backend integration tests. The full isolation matrix runs against
 * SqliteSaver (in-process, always) and PostgresSaver (when POSTGRES_URL is
 * set — CI provides a service container). MemorySaver coverage lives in
 * isolation.test.ts; these prove tenant scoping survives real SQL storage,
 * serialization, and each saver's own list()/deleteThread() implementation.
 */

import { describe, expect, it } from "vitest";
import type { BaseCheckpointSaver } from "@langchain/langgraph-checkpoint";
import { SqliteSaver } from "@langchain/langgraph-checkpoint-sqlite";
import { PostgresSaver } from "@langchain/langgraph-checkpoint-postgres";

import {
  InMemoryUsageLedger,
  TenantScopedCheckpointer,
  UnscopedAccessError,
} from "../src/index.js";
import { cfg, makeGraph } from "./helpers.js";

const POSTGRES_URL = process.env.POSTGRES_URL;

interface Backend {
  name: string;
  /** Fresh saver per test; tenants/threads are also uniquified per test. */
  make: () => Promise<BaseCheckpointSaver>;
}

const backends: Backend[] = [
  {
    name: "SqliteSaver",
    make: async () => SqliteSaver.fromConnString(":memory:"),
  },
];

if (POSTGRES_URL) {
  let pgSaver: PostgresSaver | undefined;
  backends.push({
    name: "PostgresSaver",
    make: async () => {
      // one shared saver: Postgres state persists across tests, so tests use
      // unique tenants/threads instead of a fresh database
      if (!pgSaver) {
        pgSaver = PostgresSaver.fromConnString(POSTGRES_URL);
        await pgSaver.setup();
      }
      return pgSaver;
    },
  });
}

let unique = 0;
const fresh = () => `${Date.now().toString(36)}-${(unique += 1)}`;

describe.each(backends)("$name", ({ make }) => {
  it("same thread_id for different tenants does not collide", async () => {
    const saver = new TenantScopedCheckpointer(await make());
    const graph = makeGraph({ checkpointer: saver });
    const [acme, globex, thread] = [`acme${fresh()}`, `globex${fresh()}`, "t1"];

    await graph.invoke({ messages: ["from acme"] }, cfg(acme, thread));
    await graph.invoke({ messages: ["from globex"] }, cfg(globex, thread));

    const acmeState = (await graph.getState(cfg(acme, thread))).values.messages;
    const globexState = (await graph.getState(cfg(globex, thread))).values
      .messages;
    expect(acmeState[0]).toBe("from acme");
    expect(globexState[0]).toBe("from globex");
    expect(JSON.stringify(acmeState)).not.toContain("globex");
  });

  it("list() stays inside the tenant; tenant-wide list works", async () => {
    const saver = new TenantScopedCheckpointer(await make());
    const graph = makeGraph({ checkpointer: saver });
    const [acme, globex] = [`acme${fresh()}`, `globex${fresh()}`];

    await graph.invoke({ messages: ["a"] }, cfg(acme, "t1"));
    await graph.invoke({ messages: ["b"] }, cfg(acme, "t2"));
    await graph.invoke({ messages: ["c"] }, cfg(globex, "t1"));

    const threads = new Set<string>();
    for await (const tuple of saver.list({
      configurable: { tenant_id: acme },
    })) {
      threads.add(tuple.config.configurable?.thread_id as string);
      expect(JSON.stringify(tuple.checkpoint.channel_values)).not.toContain(
        globex
      );
    }
    expect([...threads].sort()).toEqual(["t1", "t2"]);
  });

  it("deleteThread/purge only touch the handle's tenant", async () => {
    const saver = new TenantScopedCheckpointer(await make());
    const graph = makeGraph({ checkpointer: saver });
    const [acme, globex] = [`acme${fresh()}`, `globex${fresh()}`];

    await graph.invoke({ messages: ["a"] }, cfg(acme, "t1"));
    await graph.invoke({ messages: ["b"] }, cfg(acme, "t2"));
    await graph.invoke({ messages: ["c"] }, cfg(globex, "t1"));

    await expect(saver.deleteThread("t1")).rejects.toThrow(
      UnscopedAccessError
    );

    const purged = await saver.forTenant(acme).purge();
    expect(purged.sort()).toEqual(["t1", "t2"]);
    expect(await saver.getTuple(cfg(acme, "t1"))).toBeUndefined();
    expect(await saver.getTuple(cfg(globex, "t1"))).toBeDefined();
  });

  it("adoptThread migrates history on a real backend", async () => {
    const inner = await make();
    const legacyThread = `legacy${fresh()}`;
    const legacyGraph = makeGraph({ checkpointer: inner });
    const legacyCfg = { configurable: { thread_id: legacyThread } };
    await legacyGraph.invoke({ messages: ["old-1"] }, legacyCfg);
    await legacyGraph.invoke({ messages: ["old-2"] }, legacyCfg);

    const saver = new TenantScopedCheckpointer(inner);
    const acme = `acme${fresh()}`;
    await saver
      .forTenant(acme)
      .adoptThread(legacyThread, { deleteSource: true });

    const graph = makeGraph({ checkpointer: saver });
    const state = await graph.getState(cfg(acme, legacyThread));
    expect(state.values.messages[0]).toBe("old-1");
    expect(state.values.messages).toHaveLength(4);

    await graph.invoke({ messages: ["new-1"] }, cfg(acme, legacyThread));
    expect(
      (await graph.getState(cfg(acme, legacyThread))).values.messages
    ).toHaveLength(6);
    expect(
      await inner.getTuple({ configurable: { thread_id: legacyThread } })
    ).toBeUndefined();
  });

  it("usage metering works end to end", async () => {
    const ledger = new InMemoryUsageLedger();
    const saver = new TenantScopedCheckpointer(await make(), {
      usageLedger: ledger,
    });
    const graph = makeGraph({ checkpointer: saver });
    const acme = `acme${fresh()}`;

    await graph.invoke({ messages: ["q1"] }, cfg(acme));
    await graph.invoke({ messages: ["q2"] }, cfg(acme));
    expect(ledger.totals(acme).messages).toBe(2);
    expect(ledger.totals(acme).totalTokens).toBe(30);
  });
});

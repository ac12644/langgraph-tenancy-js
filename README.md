# langgraph-tenancy

[![CI](https://github.com/ac12644/langgraph-tenancy-js/actions/workflows/ci.yml/badge.svg)](https://github.com/ac12644/langgraph-tenancy-js/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/langgraph-tenancy.svg)](https://www.npmjs.com/package/langgraph-tenancy)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Tenant isolation for LangGraph.js persistence — as a drop-in wrapper.**

> Python version: [langgraph-tenancy on PyPI](https://pypi.org/project/langgraph-tenancy/)

LangGraph's own [threat model](https://github.com/langchain-ai/langgraph/blob/main/.github/THREAT_MODEL.md) says it plainly:

> Checkpoint savers index by `thread_id`. Without application-level auth, any
> caller with a valid thread_id can access that thread's state. [...] Users
> embedding LangGraph directly must implement their own access controls.

If you run a multi-tenant product on open-source LangGraph.js, the only thing
between Customer A's agent state and Customer B's is a query filter in your
application code. This package replaces that convention with enforcement.

## Install

```bash
npm install langgraph-tenancy
```

## Usage

Wrap your existing checkpointer and store. `tenant_id` becomes required.

```ts
import {
  TenantScopedCheckpointer,
  TenantScopedStore,
  getTenantStore,
  InMemoryUsageLedger,
} from "langgraph-tenancy";

const ledger = new InMemoryUsageLedger();
const checkpointer = new TenantScopedCheckpointer(new PostgresSaver(...), {
  usageLedger: ledger,
});
const store = new TenantScopedStore(new InMemoryStore());

const graph = builder.compile({ checkpointer, store });

await graph.invoke(input, {
  configurable: { thread_id: "t1", tenant_id: "acme" },
});

// free per-tenant token metering, extracted from checkpointed messages
ledger.totals("acme"); // { inputTokens, outputTokens, totalTokens, byModel }
```

Inside nodes, access the store through `getTenantStore(config)`:

```ts
const myNode = async (state, config) => {
  const store = getTenantStore(config); // reads tenant_id from the run config
  await store.put(["memories"], "k", { note: "..." });
  const items = await store.search(["memories"]);
  // namespaces in results come back unprefixed — the tenant never leaks out
};
```

Outside a run (admin scripts, background jobs):

```ts
const acme = store.forTenant("acme");
await acme.search(["memories"]);
await checkpointer.forTenant("acme").deleteThread("t1");
```

## What it enforces

| Raw LangGraph.js behavior | With `langgraph-tenancy` |
|---|---|
| Any caller with a `thread_id` reads that thread | Threads are physically keyed `tenant::thread`; wrong-thread_id bugs cannot cross tenants |
| Missing filter → silent unscoped query | Missing `tenant_id` → `TenantRequiredError`, nothing read or written |
| `saver.list({})` enumerates **every** tenant's threads | Refused — a tenant is mandatory before anything is read |
| Store namespaces are convention; any node can read any namespace | Every operation must go through a tenant-scoped entry point |
| Raw `config.store.put(...)` in a node writes unscoped | **Fails closed** with `UnscopedAccessError` — it cannot silently leak |
| `deleteThread("t1")` deletes whoever owns `t1` | Requires an explicit `forTenant("acme").deleteThread("t1")` handle |
| `usage_metadata` buried in checkpoint blobs, unqueryable | Aggregated per tenant (and per model), deduped by message id |

## Why the store API differs from the Python package

In Python, the store wrapper resolves the tenant ambiently from the run
config on every call. That design cannot work in LangGraph.js: the compiled
graph wraps your store in an `AsyncBatchedStore` whose background queue
processes operations **outside** the run's `AsyncLocalStorage` context, so
the run config is unavailable by the time operations reach the wrapped store.

Instead, scoping happens at the call site, where the config *is* available
(`getTenantStore(config)` in nodes, `forTenant()` outside runs), and
`TenantScopedStore` **refuses any operation that did not go through a scoped
entry point**. The guarantee is the same — unscoped access is impossible, not
merely discouraged — the entry point is just explicit.

## No magic

The entire mechanism is key prefixing plus mandatory-context checks:

- thread ids become `"{tenant_id}::{thread_id}"` before reaching your
  database; the prefix is stripped from everything returned.
- store namespaces `["memories"]` become `["{tenant_id}", "memories"]`.
- tenant ids containing the separator are rejected, so `acme` can never craft
  a key that collides with another tenant's space.

It composes with any `BaseCheckpointSaver` / `BaseStore` implementation —
Postgres, SQLite, MongoDB, Redis, in-memory — because it never touches
storage itself.

## What it is not

- Not authentication. You decide which tenant a request belongs to; this
  package guarantees that decision is enforced everywhere downstream.
- Not a replacement for database-level controls in high-assurance setups
  (RLS, schema-per-tenant) — it's the layer that makes your *application*
  unable to leak, whatever the database allows.

## Tested

The adversarial test suite — every test attempts a cross-tenant access the
raw LangGraph.js API allows — runs against the real `MemorySaver` and
`InMemoryStore` in CI, including a test that proves raw `config.store`
access inside a node fails closed.

## Status

Early (0.1.x). Covered today: checkpointer paths, store paths via
`getTenantStore`/`forTenant`, in-memory backends. Not yet covered:
`PostgresSaver` integration tests, subgraph `checkpoint_ns` edge cases,
semantic search (`index`/`query`) pass-through. Issues and PRs welcome.

## License

[MIT](LICENSE)

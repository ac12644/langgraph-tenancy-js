# langgraph-tenancy

[![CI](https://github.com/ac12644/langgraph-tenancy-js/actions/workflows/ci.yml/badge.svg)](https://github.com/ac12644/langgraph-tenancy-js/actions/workflows/ci.yml)
[![NPM Version](https://img.shields.io/npm/v/langgraph-tenancy)](https://www.npmjs.com/package/langgraph-tenancy)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Tenant isolation for LangGraph.js persistence — as a drop-in wrapper.**

> Using Python? Same package, same guarantees:
> [ac12644/langgraph-tenancy](https://github.com/ac12644/langgraph-tenancy) ·
> [PyPI](https://pypi.org/project/langgraph-tenancy/)

LangGraph's own [threat model](https://github.com/langchain-ai/langgraph/blob/main/.github/THREAT_MODEL.md) says it plainly:

> Checkpoint savers index by `thread_id`. Without application-level auth, any
> caller with a valid thread_id can access that thread's state. [...] Users
> embedding LangGraph directly must implement their own access controls.

If you run a multi-tenant product on open-source LangGraph.js, the only thing
between Customer A's agent state and Customer B's is a query filter in your
application code. This package replaces that convention with enforcement —
plus the operational surface a multi-tenant product needs: per-tenant usage
metering, quota enforcement, GDPR-style erasure, migration of pre-tenancy
data, and observability hooks for every denial.

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
ledger.totals("acme"); // { inputTokens, outputTokens, totalTokens, messages, byModel }
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
| Missing `thread_id` → writes keyed `"undefined"` | Refused with a loud `TenancyError` |
| `saver.list({})` enumerates **every** tenant's threads | Refused — a tenant is mandatory before anything is read |
| Store namespaces are convention; any node can read any namespace | Every operation must go through a tenant-scoped entry point |
| Raw `config.store.put(...)` in a node writes unscoped | **Fails closed** with `UnscopedAccessError` — it cannot silently leak |
| `deleteThread("t1")` deletes whoever owns `t1` | Requires an explicit `forTenant("acme").deleteThread("t1")` handle |
| `usage_metadata` buried in checkpoint blobs, unqueryable | Aggregated per tenant (and per model), deduped by message id |
| Tenant ids are arbitrary strings | Restricted to `[A-Za-z0-9_-]{1,64}` — safe in every backend's key/namespace encoding |

## Quotas

Give the checkpointer per-tenant limits and it enforces them at the
persistence boundary — an over-quota tenant's next run fails at its first
checkpoint, before any model call spends money:

```ts
import { QuotaExceededError } from "langgraph-tenancy";

const checkpointer = new TenantScopedCheckpointer(inner, {
  usageLedger: ledger, // quota reads current usage from ledger.totals()
  quota: {
    limits: async (tenantId) => plans.lookup(tenantId),
    // e.g. { maxTotalTokens: 1_000_000, maxMessages: 10_000 }
    // return undefined for "no limits"
  },
});
```

Semantics: check-then-write. The turn that *crosses* a limit still completes
(that spend already happened and must not be lost); every run started after
that fails with `QuotaExceededError`. Runs are never killed mid-flight.
Supply `quota.usage` to read usage from your own billing system instead of
the ledger.

## Usage metering in production

`InMemoryUsageLedger` is the reference implementation (bounded memory,
deduped by message id). For a real backend, implement the two-method
interface — `record()` may be async:

```ts
const ledger: UsageLedger = {
  async record(tenantId, { messageId, model, totalTokens }) {
    await db.insertUsage({ tenantId, messageId, model, totalTokens });
  },
  async totals(tenantId) { return db.usageTotals(tenantId); }, // enables quotas
};
```

A throwing ledger does **not** fail checkpoint writes: the error is reported
through `onEvent` (type `"ledger_error"`) and the record is retried on the
next checkpoint. Set `ledgerErrors: "throw"` if you'd rather fail the write.

## Observability

Every denial is a security-relevant event. Wire them to your logger, metrics,
or OpenTelemetry:

```ts
const onEvent = (e: TenancyEvent) => {
  // e.type: "denied" | "quota_exceeded" | "ledger_error"
  logger.warn("tenancy", e);
};
const checkpointer = new TenantScopedCheckpointer(inner, { onEvent });
const store = new TenantScopedStore(innerStore, { onEvent });
```

Handler errors are swallowed — observers can never break the data path.

## Admin, GDPR, and migration

Everything out-of-band goes through an explicit per-tenant handle:

```ts
const acme = checkpointer.forTenant("acme");

await acme.listThreads();          // every thread id belonging to acme
await acme.deleteThread("t1");     // one thread
await acme.purge();                // GDPR erasure: every acme thread

await store.forTenant("acme").purge(); // every acme store item

// Adopting langgraph-tenancy on an existing deployment? Migrate pre-tenancy
// (unprefixed) threads under their rightful tenant, history intact:
await acme.adoptThread("legacy-thread-id", { deleteSource: true });
```

`adoptThread` replays every checkpoint — order, parentage, metadata, and
pending writes — under the tenant-scoped key, so conversations continue
exactly where they left off.

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

One consequence: semantic-search indexing (`put(..., index)`) passes through
on `forTenant()` views, but `config.store` inside nodes batches operations
and drops the `index` argument upstream — so the view refuses it loudly there
instead of losing it silently.

## No magic

The entire mechanism is key prefixing plus mandatory-context checks:

- thread ids become `"{tenant_id}::{thread_id}"` before reaching your
  database; the prefix is stripped from everything returned.
- store namespaces `["memories"]` become `["{tenant_id}", "memories"]`.
- tenant ids are restricted to `[A-Za-z0-9_-]{1,64}` — an allowlist, because
  tenant ids end up inside storage keys and namespace encodings of whatever
  backend you use, and a blocklist can't anticipate all of them.

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
raw LangGraph.js API allows — runs in CI against the real `MemorySaver` /
`InMemoryStore` **and real SQL backends**: `SqliteSaver` in-process and
`PostgresSaver` against a Postgres 17 service container. Coverage includes
raw `config.store` access failing closed, tenant-wide listing, quota
enforcement, GDPR purge, and `adoptThread` migration on every backend.

## License

[MIT](LICENSE)

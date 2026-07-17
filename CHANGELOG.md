# Changelog

## 0.2.0 — 2026-07-17

Production-hardening release. Everything below is additive except the
stricter tenant-id validation.

### Breaking

- **Tenant ids are now restricted to `[A-Za-z0-9_-]{1,64}`** (previously only
  `::` was rejected). Tenant ids are embedded in storage keys and store
  namespace segments, and physical backends flatten namespaces with their own
  separators — an allowlist is the only safe contract. Tenant ids that were
  already plain slugs/UUID-safe strings are unaffected.
- **Missing `thread_id` now throws** instead of silently producing a
  `"tenant::undefined"` storage key.

### Fixed

- `list()` with a tenant but no `thread_id` previously matched nothing;
  it now correctly enumerates every checkpoint belonging to that tenant
  (and only that tenant), with `limit` applied after tenant filtering.
- Store items returned from searches are no longer mutated in place when
  the tenant prefix is stripped.

### Added

- **Quota enforcement** — `quota: { limits, usage? }` on
  `TenantScopedCheckpointer`. Over-quota tenants fail with
  `QuotaExceededError` at the first checkpoint of their next run; runs are
  never killed mid-flight.
- **Async-safe usage ledgers** — `UsageLedger.record()` may return a
  promise. A throwing ledger no longer fails checkpoint writes by default:
  the error surfaces as a `ledger_error` event and the record is retried on
  the next checkpoint (`ledgerErrors: "throw"` opts into failing).
- **Bounded memory** — message-id dedup in both the checkpointer and
  `InMemoryUsageLedger` uses capacity-bounded sets (`usageDedupCapacity`,
  `dedupCapacity`) instead of growing forever.
- **Observability** — `onEvent` on both wrappers receives typed
  `TenancyEvent`s (`denied`, `quota_exceeded`, `ledger_error`) for every
  refusal; handler errors never break the data path.
- **Admin / GDPR surface** — `forTenant(tenant)` handles gained
  `listThreads()`, `purge()` (checkpointer), and `purge()` (store view).
- **Migration** — `forTenant(tenant).adoptThread(threadId)` replays a
  pre-tenancy (unprefixed) thread under the tenant, preserving checkpoint
  order, parentage, metadata, and pending writes.
- **Semantic-search indexing** — `put(..., index)` passes through on
  `forTenant()` views and fails loudly through `config.store` (whose
  batching drops the argument upstream).
- **Real-backend integration tests** — the isolation matrix now also runs
  against `SqliteSaver` (in-process) and `PostgresSaver` (Postgres 17
  service container in CI).
- **Delta-channel-ready metering** — usage is now also extracted from
  `putWrites()`, not just full checkpoints. `DeltaChannel` (beta) stores only
  a sentinel in `checkpoint.channel_values`, so for delta-channel graphs
  messages only ever flow through writes; message-id dedup (reserved before
  the ledger call, released on failure) prevents double counting when the
  same message appears on both paths concurrently.
- **`getDeltaChannelHistory` support** — the new (beta) delta-channel walk
  introduced in `@langchain/langgraph-checkpoint` 1.1.x is delegated to the
  inner saver's (possibly storage-optimized) implementation under a
  tenant-scoped config, instead of silently falling back to the base-class
  walk. Requires a tenant like every other read.

### Dependencies

- Peer dependency raised to `@langchain/langgraph-checkpoint >= 1.1.3`
  (the published types reference `DeltaChannelHistory`).
- Dev/test toolchain updated: `@langchain/core` 1.2, `@langchain/langgraph`
  1.4.8, `vitest` 4, `@types/node` 26. TypeScript stays on 5.9 — tsup's
  declaration bundler does not support the TypeScript 7 compiler API yet.

## 0.1.0 — 2026-07-15

Initial release: `TenantScopedCheckpointer`, `TenantScopedStore`,
`getTenantStore`, per-tenant usage metering with `InMemoryUsageLedger`.

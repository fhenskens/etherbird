# Changelog

## Unreleased

- Share named setter/value registration mechanics between Supervisor and Pool
  while retaining their locks and application ordering.
- Use the common call-deadline wrapper for retry scopes, including queued calls
  and held leases; observable shutdown wins races with completion/expiry.
- Implement bounded FIFO admission around the existing unbounded FIFO storage.
- Add retry regressions for queueing deadlines, cancellation and shutdown without replay.

## 0.4.2 — 2026-10-14

- Standardize lowercase `etherbird` branding in guides, examples and links.
- Add optional `RetireOnDrop<T>` adapter plumbing: incomplete scopes clear a
  resource slot and publish connection loss; adapters explicitly complete valid
  replies, including healthy device rejections.
- Reuse the helper in serial and Modbus/WebSocket adapters, with cancellation,
  timeout, drop-order and completion regressions.
- Separate lifecycle driving, call timeout policy, pool dispatch and tracked jobs
  into internal modules without changing dispatch or lifecycle contracts.

## 0.4.1 â€” 2026-10-09

- Add `BoundedFifoQueue` for generic bounded FIFO admission with explicit capacity,
  `QueueError::Full`, cancellation-compatible retention and zero-capacity rejection.
- Add single-attempt `execute_with_timeout` to Supervisor, direct/pooled proxies,
  Pool and ResourceLease; Pool also supports explicit priority with a timeout.
  Add `acquire_with_timeout` on Supervisor and `connected_with_timeout` on proxies
  and Pool. Deadlines include readiness/recovery and queueing where applicable.
  Zero timeouts never start work, shutdown takes precedence, and calls never replay.
  Adapter-specific interruption safety remains required; pooled cancellation is
  signalled to tracked jobs and awaited stop drains them.

## 0.4.0 — 2026-10-09

- Add `Lifecycle::lifecycle_failure` with retry/fail policy, latched typed causes,
  `ResourceState::Failed` and `Error::Lifecycle(Arc<E>)`. Initialization and watch
  failures can suspend attempts until explicit `reset_failure` / `reset_failed`.
  Healthy pool slots remain usable; all-failed pools fail admission without growth.
- Add `Lifecycle::operation_failure` with recover/retain policy. Healthy operation
  errors can return normally without resource replacement or nested results.
  Logging classification and explicit replay predicates remain independent.
- Add `Config::validate`, `PoolConfig::validate`, `RetryPolicy::validate`,
  `ConfigError` and checked Supervisor/Pool constructors. Legacy constructors keep
  their accepted configurations and panic behavior. Checked lifecycle backoff must
  be positive and ordered; zero hook deadlines remain immediate deadlines.
- Release framework resource caches and pool entries after awaited shutdown.
  Resource-derived attributes return None; stored configuration and registrations
  remain. External handles, user captures and abandoned timed-out hooks can retain
  resources. Adapters still explicitly close streams and destroy session state.

Migration from 0.3: update exhaustive Error/ResourceState matches for the new
variants; use stored values for post-stop configuration reads rather than resource
attributes. New hooks default to existing recovery behavior. See
[the lifecycle guide](docs/LIFECYCLE_POLICIES.md) for reset, ownership and validation.

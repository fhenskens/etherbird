# Outstanding work

## Current handoff — 2026-10-15

- **Published:** 0.4.3 adds generation-bound adapter values and incomplete
  exclusive-scope disconnection, alongside the previously committed internal
  registration/deadline/FIFO cleanup. Both helpers are optional to use in the main
  library; protocol validation and remote effects remain adapter responsibilities.
- **Release evidence:** all seven hosted platform/serial jobs, the clean dry-run,
  crates.io publication, pushed tag and downloaded-package provenance are verified;
  see [RELEASE.md](RELEASE.md). Rocsteady adopts registry 0.4.3 and its helpers.
- **Open:** the pool-dispatch performance investigation below. Retained incremental
  improvements have not resolved the main scheduling cost. The inline-polling
  experiment was discarded; no such feature or implementation is in progress.
- **Evidence:** the published release and subsequent cleanup each passed the seven
  hosted platform/serial jobs recorded in [RELEASE.md](RELEASE.md). Measurements
  retain their original platform/workload limits; successful CI is not new
  performance evidence.
- **Development policy:** current stable Rust; protocol validation, authentication
  procedures and remote-outcome semantics belong in adapters. Repository Git email
  is fhenskens@yahoo.com. Historical Rust 1.88 checks are not a current support promise.

## Recommended next investigation

Extend the existing allocation/CPU profiles with a non-WSL Linux baseline and
realistic slow operations or independently scheduled borrowers. Use the existing
manual/direct/pooled benchmark paths before selecting another optimization.
Preserve pooled independent cancellation/draining, exclusive ownership, queue
ordering and deadlines; direct caller-owned execution has different guarantees.
Record platform variation and improve the implementation only where the evidence
supports it. No new optimization has been selected or started.

## Reduce pooled operation dispatch overhead

Status: open. Scope: `Pool::execute` and the typed managed proxy's pooled call path.
Small operations show substantial coordination costs in recorded WSL workloads;
Windows behaves differently. This does not establish the cost for every workload.
No new optimization has been selected or started.

### Remaining work

- [ ] Reproduce a baseline on non-WSL Linux; extend CPU/allocation measurements to
  macOS and Windows CPU where available. Separate platform effects from the
  coordination cost before choosing an implementation.
- [ ] Profile remaining notifications, cancellation scans, boxed jobs, result
  channels, task-registry synchronization and repeated subscriptions within the
  existing Tokio task model.
- [ ] Exercise real slow transports, multiple slots and independently scheduled
  borrowers. Measure concurrency and fairness as well as tiny-operation throughput.
- [ ] Consider caller-side execution only with a design that preserves independent
  pooled cancellation and draining; an unpolled caller must not block teardown.
- [ ] Compare any candidate against manual, direct and pooled baselines with 1, 3
  and 16 callers on current-thread, two-worker and default runtimes. Report
  throughput, tail latency, CPU, context switches and allocations per platform.

### Evidence to retain

The [MQTT performance report](MQTT_PERFORMANCE.md) records scheduler comparisons,
CPU profiles, retained incremental changes and the discarded inline-polling spike.
The [admission report](POOL_DISPATCH_PERFORMANCE.md) records allocations,
uncontended admission and remaining slow-operation/borrower measurements.
Raw measurements remain under `docs/benchmarks/` with their reproduction context.

Disarming completion notifications did not resolve the slowdown. Targeted wakeups
and uncontended admission improved some captured cases but left coordination costs
open. Inline polling regressed Windows and was removed. The direct proxy is an
available alternative for concurrency-safe resources, with caller-owned futures
and different shutdown guarantees; it does not replace pooled exclusivity/draining.

### Acceptance for a retained change

Preserve exclusive leases and capacity limits, FIFO/priority ordering, custom queue
rejection, readiness, queued/running cancellation, retry deadlines, recovery
isolation and cancellation-safe shutdown. Demonstrate slow-operation concurrency
and fairness alongside the comparison matrix above. Pass regression tests and
recovery demos, add focused tests for changed execution behavior and record
measurements and limitations in the performance reports.

Implemented features and cleanup belong in the [changelog](../CHANGELOG.md),
current contracts in [lifecycle policies](LIFECYCLE_POLICIES.md), and release
verification in [RELEASE.md](RELEASE.md).

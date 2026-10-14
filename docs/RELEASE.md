# Release 0.4.2

Release date: 2026-10-14. This compatible release adds the optional public
`RetireOnDrop<T>` adapter helper and standardizes lowercase `etherbird` branding.
Internal lifecycle driving, call timeout policy, pool dispatch and tracked jobs
now live in focused modules. Existing public signatures and dispatch/recovery
contracts are unchanged.

## Adapter contract

The adapter holds `RetireOnDrop` across the exchange and reply validation.
An incomplete drop clears its `Option<T>` slot before publishing connection loss.
The adapter explicitly calls `complete()` for a valid exchange, including a healthy
device rejection. The helper does not acquire locks, retry work, infer remote
outcomes or run asynchronous lifecycle cleanup. See
[the lifecycle guide](LIFECYCLE_POLICIES.md) and public API doctest.

The serial and Modbus/WebSocket adapters use the helper. Rocsteady is the external
acceptance case for malformed-reply retirement, healthy rejection, timeouts,
cancellation, generation invalidation and no replay.

## Recorded release evidence

Local macOS ARM64 / stable Rust 1.99 checks cover default/all-feature/all-target
suites, doctests, strict Clippy, formatting and package verification. Four new
helper tests cover cancellation, timeout, drop-before-notification and explicitly
completed device rejection. The real serial pseudo-terminal regression and
Modbus/WebSocket recovery demo pass. Rocsteady's 76-test suite and its extracted
package consumer passed with the local dependency before publication. Subsequently,
rocsteady 0.1.0 passed 122 all-feature tests, 14 doctests and its packaged consumer
using registry etherbird 0.4.2. These are downstream protocol checks, separate from
etherbird's own suite.

Hosted CI for 0.4.2 passed all seven jobs: stable Linux/macOS/Windows direct/pooled
builds and the Linux serial pseudo-terminal fixture. See the
[release run](https://github.com/fhenskens/etherbird/actions/runs/38539621339).
Publication, the tag and the registry package's source provenance were verified.
Rocsteady removed its temporary sibling path and adopted the registry crate.

The subsequent internal registration/deadline/FIFO cleanup also passed
[all seven hosted jobs](https://github.com/fhenskens/etherbird/actions/runs/38547036690).
It remains unreleased; the crate version is still 0.4.2. No public feature or
contract change requires a separate release for current rocsteady consumers.

## Checks for the next release

```sh
cargo fmt --check
cargo test --locked --all-targets
cargo test --locked --all-features --all-targets
cargo test --locked --all-features --doc
cargo clippy --locked --all-features --all-targets -- -D warnings
cargo doc --locked --all-features --no-deps
cargo publish --dry-run --locked
```

For a future release, select a new version, review its payload and publish only
a clean commit after dry-run and CI success. Push the corresponding tag and verify
the registry package. Repeat downstream consumer checks before recommending an
adapter dependency update; completed 0.4.2 adoption is not an outstanding task.

[crates.io](https://crates.io/crates/etherbird/0.4.2) ·
[API docs](https://docs.rs/etherbird/0.4.2/etherbird/) ·
[CI](https://github.com/fhenskens/etherbird/actions/workflows/ci.yml)

A successful dry-run alone is not proof of publication. The links above describe
the published 0.4.2 release; repository-only cleanup is listed separately.

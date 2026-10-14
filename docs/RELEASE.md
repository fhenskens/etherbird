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

## Validation and release procedure

Local macOS ARM64 / stable Rust 1.99 checks cover default/all-feature/all-target
suites, doctests, strict Clippy, formatting and package verification. Four new
helper tests cover cancellation, timeout, drop-before-notification and explicitly
completed device rejection. The real serial pseudo-terminal regression and
Modbus/WebSocket recovery demo pass. Rocsteady's 76-test suite and its extracted
package consumer pass with the local dependency before publication.

Hosted CI checks stable Rust on Linux/macOS/Windows with direct/pooled builds and
an additional Linux serial pseudo-terminal fixture. Validate the exact release
commit's hosted run before publication. Local results do not establish hosted
platform coverage.

```sh
cargo fmt --check
cargo test --locked --all-targets
cargo test --locked --all-features --all-targets
cargo test --locked --all-features --doc
cargo clippy --locked --all-features --all-targets -- -D warnings
cargo doc --locked --all-features --no-deps
cargo publish --dry-run --locked
```

Publish only the reviewed clean commit after dry-run and CI success; then push
its `v0.4.2` tag. Verify the registry version and packaged VCS commit. Rocsteady
must remove its temporary sibling path, regenerate the registry lockfile and
repeat its package/consumer checks against the published crate.

[crates.io](https://crates.io/crates/etherbird/0.4.2) ·
[API docs](https://docs.rs/etherbird/0.4.2/etherbird/) ·
[CI](https://github.com/fhenskens/etherbird/actions/workflows/ci.yml)

These endpoints and the release tag are verified after publication; a successful
dry-run alone is not proof of publication.

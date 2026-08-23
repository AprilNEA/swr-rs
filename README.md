# swr-rs

UI-agnostic stale-while-revalidate async cache for Rust, aligned with the
semantics of [SWR](https://swr.vercel.app) and TanStack Query. The core is a
headless `SwrClient` over a pure synchronous state machine (sans-io); async
runtimes and UI frameworks plug in as thin layers.

The normative spec lives in [`handoff.md`](handoff.md); implementation
deviations are tracked in its D-x table and in
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md).

## Crates

- `swr` — the batteries-included entry point: re-exports the full `swr-core`
  API and picks the default runtime per platform (`swr::client()`).
- `swr-core` — state machine, cache, and the public client API. Compiles on
  native and `wasm32-unknown-unknown`.
- `swr-runtime-tokio` — tokio `Runtime` implementation for native targets.
- `swr-runtime-web` — browser `Runtime` (`spawn_local` + `setTimeout` timers)
  plus a reference-counted focus/online event source (`WebEventSource::attach`
  forwards `focus`/`visibilitychange`/`online` to `SwrClient::broadcast`).
  wasm32-only; an empty crate on native targets.

## Usage

```rust
use swr::ReadPolicy;

#[tokio::main]
async fn main() {
    let client = swr::client();

    let user = client
        .fetch(
            ("user", 1u64),
            |(_, id): (&str, u64)| async move { load_user(id).await },
            ReadPolicy::StaleWhileRevalidate,
        )
        .await
        .unwrap();
    println!("{user}");
}

async fn load_user(id: u64) -> Result<String, String> {
    Ok(format!("user-{id}"))
}
```

To supply your own [`Runtime`] (clock/spawn/timers), depend on `swr-core`
plus a runtime crate directly and use `SwrClient::builder()`.

A fuller walkthrough (caching, background refresh, optimistic mutation, prefix
invalidation) is in `crates/swr-runtime-tokio/examples/bff.rs`:

```sh
cargo run -p swr-runtime-tokio --example bff
```

## Development

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo check -p swr-core -p swr-runtime-web --target wasm32-unknown-unknown

# wasm smoke tests (IT5); needs wasm-bindgen-cli matching the locked
# wasm-bindgen version, plus Node for smoke.rs / a browser for browser.rs
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
  cargo test -p swr-runtime-web --target wasm32-unknown-unknown --test smoke
```

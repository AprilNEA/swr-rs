# swr-rs

UI-agnostic stale-while-revalidate async cache for Rust, aligned with the
semantics of [SWR](https://swr.vercel.app) and TanStack Query. The core is a
headless `SwrClient` over a pure synchronous state machine (sans-io); async
runtimes and UI frameworks plug in as thin layers.

A source-verified feature matrix against vercel/swr is kept in
[`docs/swr-parity.md`](docs/swr-parity.md) — including focus throttling,
error retry with exponential backoff (`Retry`/`RetryPolicy`), and
`QueryOptions::immutable()`.

## Crates

- `swr` — the batteries-included entry point: re-exports the full `swr-core`
  API and picks the default runtime per platform (`swr::client()`).
- `swr-core` — state machine, cache, and the public client API. Compiles on
  native and `wasm32-unknown-unknown`.
- `swr-runtime-tokio` — tokio `Runtime` implementation for native targets.
- `swr-reqwest` — reqwest fetchers: `JsonFetcher` maps cache keys to HTTP
  requests with JSON decoding (`reqwest::Error` as the query error type).
- `swr-ureq` — the same for the blocking ureq client, bridged onto
  per-request worker threads (runtime-agnostic; `ureq::Error` as the query
  error type). This worker-thread + oneshot bridge is the general answer for
  any blocking fetcher — `Runtime` deliberately has no
  `spawn_blocking`, which would tie the core to tokio.
- `swr-gpui` — [GPUI](https://crates.io/crates/gpui) adapter: `GpuiRuntime`
  runs fetches/timers on GPUI's executors (no tokio needed; virtual-clock
  test support via `advance_clock`), and `Query` bridges watch changes into
  an entity that views observe and read lock-free during render. See
  `crates/swr-gpui/examples/status.rs`.
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

Fetchers are plain closures, so any HTTP client works inline. For reqwest
(`swr-reqwest`) and ureq (`swr-ureq`, same shape), the integration crates
remove the boilerplate:

```rust
use swr_reqwest::JsonFetcher;

let users: JsonFetcher<(&str, u64), User> =
    JsonFetcher::get(http, |(_, id)| format!("https://api.example.com/users/{id}"));
let user = client.fetch(("user", 1u64), users.clone(), ReadPolicy::StaleWhileRevalidate).await?;
```

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

# wasm smoke tests; needs wasm-bindgen-cli matching the locked
# wasm-bindgen version, plus Node for smoke.rs / a browser for browser.rs
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
  cargo test -p swr-runtime-web --target wasm32-unknown-unknown --test smoke
```

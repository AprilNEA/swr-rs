//! Stale-while-revalidate async cache for Rust.
//!
//! This is the batteries-included entry point: the whole [`swr_core`] public
//! API re-exported, with runtimes and integrations behind feature flags:
//!
//! | Feature | Default | Provides |
//! |---|---|---|
//! | `tokio` | yes | [`TokioRuntime`] on native targets; [`client()`] / [`default_runtime()`] |
//! | `web` | yes | `WebRuntime` + `WebEventSource` on wasm32; [`client()`] / [`default_runtime()`] |
//! | `reqwest` | no | reqwest fetchers as [`mod@reqwest`] (native targets) |
//! | `ureq` | no | ureq fetchers as [`mod@ureq`] (native targets) |
//! | `gpui` | no | the GPUI adapter as [`mod@gpui`] (native targets) |
//!
//! A GPUI app that brings no tokio disables the defaults:
//!
//! ```toml
//! swr = { version = "0.1", default-features = false, features = ["gpui"] }
//! ```
//!
//! and builds its client with `swr::gpui::client(cx)`. To supply your own
//! [`Runtime`], depend on `swr-core` directly instead.
//!
//! ```
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! use swr::ReadPolicy;
//!
//! let client = swr::client();
//! let greeting = client
//!     .fetch(
//!         ("greeting", 1u64),
//!         |(_, id): (&str, u64)| async move { Ok::<String, String>(format!("hello #{id}")) },
//!         ReadPolicy::StaleWhileRevalidate,
//!     )
//!     .await
//!     .unwrap();
//! assert_eq!(greeting.as_str(), "hello #1");
//! # }
//! ```
#![deny(missing_docs)]

#[cfg(any(
    all(feature = "tokio", not(target_arch = "wasm32")),
    all(feature = "web", target_arch = "wasm32")
))]
use std::sync::Arc;

pub use swr_core::{
    BoxedFuture, Closed, ErasedValue, FetchError, Fetcher, Instant, IntoKeyPrefix, IntoQueryKey,
    IntoSegment, IntoSegments, MaybeSend, MaybeSync, MutateOptions, QueryHandle, QueryKey,
    QueryOptions, QueryState, ReadPolicy, Retry, RetryPolicy, Runtime, RuntimeFuture, Segment,
    Snapshot, SwrClient, SwrClientBuilder, SwrEvent, WeakSwrClient,
};

#[cfg(all(feature = "tokio", not(target_arch = "wasm32")))]
pub use swr_runtime_tokio::TokioRuntime;
#[cfg(all(feature = "web", target_arch = "wasm32"))]
pub use swr_runtime_web::{WebEventSource, WebRuntime};

/// reqwest fetchers: `JsonFetcher` maps cache keys to HTTP requests with JSON
/// decoding (`reqwest::Error` as the query error type).
#[cfg(all(feature = "reqwest", not(target_arch = "wasm32")))]
pub use swr_reqwest as reqwest;

/// ureq fetchers: blocking HTTP exchanges bridged onto per-request worker
/// threads (`ureq::Error` as the query error type).
#[cfg(all(feature = "ureq", not(target_arch = "wasm32")))]
pub use swr_ureq as ureq;

/// GPUI adapter: `GpuiRuntime` runs fetches and timers on GPUI's executors,
/// and `Query` bridges watch changes into entity notifications. Build clients
/// with `swr::gpui::client(cx)`.
#[cfg(all(feature = "gpui", not(target_arch = "wasm32")))]
pub use swr_gpui as gpui;

/// The default [`Runtime`] for this platform: [`TokioRuntime`] on native
/// targets, `WebRuntime` on wasm32. Pass it to [`SwrClientBuilder::build`]
/// when combining custom [`QueryOptions`] defaults with the platform runtime.
///
/// # Panics
///
/// On native targets, panics when called outside a tokio runtime context.
#[cfg(any(
    all(feature = "tokio", not(target_arch = "wasm32")),
    all(feature = "web", target_arch = "wasm32")
))]
pub fn default_runtime() -> Arc<dyn Runtime> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        Arc::new(TokioRuntime::current())
    }
    #[cfg(target_arch = "wasm32")]
    {
        Arc::new(WebRuntime::new())
    }
}

/// Create an [`SwrClient`] with default options on the platform's default
/// runtime (see [`default_runtime`]).
///
/// On `wasm32`, pair it with `WebEventSource::attach` to revalidate on
/// browser focus/online events. In a GPUI app, use `swr::gpui::client(cx)`
/// instead.
///
/// # Panics
///
/// On native targets, panics when called outside a tokio runtime context.
#[cfg(any(
    all(feature = "tokio", not(target_arch = "wasm32")),
    all(feature = "web", target_arch = "wasm32")
))]
pub fn client() -> SwrClient {
    SwrClient::new(default_runtime())
}

#[cfg(all(test, feature = "tokio", not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    /// The facade wires the platform runtime into a working client.
    #[tokio::test(start_paused = true)]
    async fn client_runs_on_the_default_runtime() {
        let client = client();
        let fetcher = |key: &'static str| async move { Ok::<String, String>(key.to_uppercase()) };

        let value = client
            .fetch("swr", fetcher, ReadPolicy::StaleWhileRevalidate)
            .await
            .expect("first load");
        assert_eq!(value.as_str(), "SWR");

        let handle =
            client.subscribe::<_, String, String, _>("swr", fetcher, QueryOptions::default());
        assert_eq!(handle.snapshot().data.expect("cached").as_str(), "SWR");
    }

    /// Custom defaults compose with the platform runtime via the builder.
    #[tokio::test(start_paused = true)]
    async fn builder_composes_with_default_runtime() {
        let client = SwrClient::builder()
            .default_options(QueryOptions {
                stale_time: std::time::Duration::from_secs(60),
                ..QueryOptions::default()
            })
            .build(default_runtime());
        client.set::<_, u32, String>("k", 5);
        let cached = client
            .fetch(
                "k",
                |_key: &'static str| std::future::ready(Ok::<u32, String>(0)),
                ReadPolicy::EnsureFresh,
            )
            .await
            .expect("fresh local write served from cache");
        assert_eq!(*cached, 5);
    }
}

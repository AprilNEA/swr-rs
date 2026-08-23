//! UI-agnostic stale-while-revalidate cache core.
//!
//! All SWR semantics — deduplication, race resolution, staleness, optimistic
//! updates, GC — live in one pure synchronous state machine (D-1); a thin
//! async shell executes its effects. See `handoff.md` for the normative spec.
//!
//! ```no_run
//! # async fn demo(runtime: std::sync::Arc<dyn swr_core::Runtime>) {
//! use swr_core::{ReadPolicy, SwrClient};
//!
//! let client = SwrClient::new(runtime);
//! let user = client
//!     .fetch(
//!         ("user", 1u64),
//!         |(_, id): (&str, u64)| async move { load_user(id).await },
//!         ReadPolicy::StaleWhileRevalidate,
//!     )
//!     .await;
//! # let _ = user;
//! # }
//! # async fn load_user(_id: u64) -> Result<String, String> { Ok(String::new()) }
//! ```
#![deny(missing_docs)]

mod client;
mod erased;
mod error;
mod fetcher;
mod handle;
mod key;
mod machine;
mod marker;
mod options;
mod runtime;
mod snapshot;

#[cfg(test)]
mod integration_tests;

/// The one `Instant` used across the library (spec 3.2). On native targets it
/// is a zero-cost re-export of [`std::time::Instant`]; on `wasm32` it is
/// backed by the Performance API.
pub use web_time::Instant;

pub use client::{SwrClient, SwrClientBuilder};
pub use erased::{BoxedFuture, ErasedValue};
pub use error::{Closed, FetchError};
pub use fetcher::Fetcher;
pub use handle::QueryHandle;
pub use key::{IntoKeyPrefix, IntoQueryKey, IntoSegment, IntoSegments, QueryKey, Segment};
pub use marker::{MaybeSend, MaybeSync};
pub use options::{MutateOptions, QueryOptions, ReadPolicy, SwrEvent};
pub use runtime::{Runtime, RuntimeFuture};
pub use snapshot::{QueryState, Snapshot};

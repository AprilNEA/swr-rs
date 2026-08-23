//! Type erasure (spec 4.3): one cache stores every query type; typed handles
//! downcast at the boundary (TE-1) and expose values as `Arc<T>` (TE-2).

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::key::QueryKey;
use crate::marker::{MaybeSend, MaybeSync};

/// Boxed future; `Send` on native targets (see [`MaybeSend`](crate::MaybeSend)).
#[cfg(not(target_arch = "wasm32"))]
pub type BoxedFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
/// Boxed future; `Send` on native targets (see [`MaybeSend`](crate::MaybeSend)).
#[cfg(target_arch = "wasm32")]
pub type BoxedFuture<T> = Pin<Box<dyn Future<Output = T> + 'static>>;

/// Type-erased cached value. Values move as `Arc<T>`; user types never need
/// `Clone` (TE-2).
#[cfg(not(target_arch = "wasm32"))]
pub type ErasedValue = Arc<dyn Any + Send + Sync>;
/// Type-erased cached value. Values move as `Arc<T>`; user types never need
/// `Clone` (TE-2).
#[cfg(target_arch = "wasm32")]
pub type ErasedValue = Arc<dyn Any>;

/// Type-erased fetcher stored per entry; last `read`/`subscribe` wins (API-2).
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type ErasedFetcher =
    Arc<dyn Fn(QueryKey) -> BoxedFuture<Result<ErasedValue, ErasedValue>> + Send + Sync>;
/// Type-erased fetcher stored per entry; last `read`/`subscribe` wins (API-2).
#[cfg(target_arch = "wasm32")]
pub(crate) type ErasedFetcher =
    Arc<dyn Fn(QueryKey) -> BoxedFuture<Result<ErasedValue, ErasedValue>>>;

/// TE-1: downcast at the typed boundary. The `(T, E)` type id is part of the
/// key (K-1), so a mismatch is impossible without an internal bug.
pub(crate) fn downcast_value<T>(value: ErasedValue) -> Arc<T>
where
    T: MaybeSend + MaybeSync + 'static,
{
    value
        .downcast::<T>()
        .unwrap_or_else(|_| panic!("swr-core internal bug (TE-1): erased value type mismatch"))
}

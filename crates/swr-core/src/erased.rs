//! Type erasure: one cache stores every query type; typed handles
//! downcast at the boundary and expose values as `Arc<T>`.

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
/// `Clone`.
#[cfg(not(target_arch = "wasm32"))]
pub type ErasedValue = Arc<dyn Any + Send + Sync>;
/// Type-erased cached value. Values move as `Arc<T>`; user types never need
/// `Clone`.
#[cfg(target_arch = "wasm32")]
pub type ErasedValue = Arc<dyn Any>;

/// Type-erased fetcher stored per entry; last `read`/`subscribe` wins.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type ErasedFetcher =
    Arc<dyn Fn(QueryKey) -> BoxedFuture<Result<ErasedValue, ErasedValue>> + Send + Sync>;
/// Type-erased fetcher stored per entry; last `read`/`subscribe` wins.
#[cfg(target_arch = "wasm32")]
pub(crate) type ErasedFetcher =
    Arc<dyn Fn(QueryKey) -> BoxedFuture<Result<ErasedValue, ErasedValue>>>;

/// Type-erased value comparator for structural sharing. Stored per
/// entry; last provider wins, a call without one leaves the stored comparator
/// untouched.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type ErasedCompare = Arc<dyn Fn(&ErasedValue, &ErasedValue) -> bool + Send + Sync>;
/// Type-erased value comparator for structural sharing. Stored per
/// entry; last provider wins, a call without one leaves the stored comparator
/// untouched.
#[cfg(target_arch = "wasm32")]
pub(crate) type ErasedCompare = Arc<dyn Fn(&ErasedValue, &ErasedValue) -> bool>;

/// Wrap `T: PartialEq` into an [`ErasedCompare`]. Both sides come from the
/// same entry, so the key's bound type id guarantees the downcasts succeed.
pub(crate) fn erased_eq<T>() -> ErasedCompare
where
    T: PartialEq + MaybeSend + MaybeSync + 'static,
{
    Arc::new(|a, b| {
        let a = a
            .downcast_ref::<T>()
            .expect("swr-core internal bug: comparator value type mismatch");
        let b = b
            .downcast_ref::<T>()
            .expect("swr-core internal bug: comparator value type mismatch");
        a == b
    })
}

/// Downcast at the typed boundary. The `(T, E)` type id is part of the
/// key, so a mismatch is impossible without an internal bug.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn downcast_value<T>(value: ErasedValue) -> Arc<T>
where
    T: MaybeSend + MaybeSync + 'static,
{
    value
        .downcast::<T>()
        .unwrap_or_else(|_| panic!("swr-core internal bug: erased value type mismatch"))
}

/// Downcast at the typed boundary. The `(T, E)` type id is part of the
/// key, so a mismatch is impossible without an internal bug.
///
/// `std` provides `Arc::downcast` only for `dyn Any + Send + Sync`, so the
/// wasm variant re-implements it over `Arc<dyn Any>`.
#[cfg(target_arch = "wasm32")]
pub(crate) fn downcast_value<T>(value: ErasedValue) -> Arc<T>
where
    T: MaybeSend + MaybeSync + 'static,
{
    assert!(
        value.is::<T>(),
        "swr-core internal bug: erased value type mismatch"
    );
    let ptr = Arc::into_raw(value).cast::<T>();
    // SAFETY: `is::<T>()` above proved the erased pointee is exactly a `T`,
    // and `from_raw` re-adopts the refcount `into_raw` handed over.
    unsafe { Arc::from_raw(ptr) }
}

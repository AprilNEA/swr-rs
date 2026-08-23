//! The typed [`Fetcher`] trait and its erasure into the stored form.

use std::future::Future;
use std::sync::Arc;

use crate::erased::{BoxedFuture, ErasedFetcher, ErasedValue};
use crate::key::QueryKey;
use crate::marker::{MaybeSend, MaybeSync};

/// A typed fetcher. Any `Fn(K) -> impl Future<Output = Result<T, E>>`
/// closure implements it via the blanket impl, so plain closures like
/// `|id| async move { api.load(id).await }` work directly.
///
/// The fetcher provided to each `fetch()`/`subscribe()` call replaces
/// the stored one (last-wins). Using fetchers with different behavior for the
/// same key is a caller error the library does not detect.
pub trait Fetcher<K, T, E>: MaybeSend + MaybeSync + 'static {
    /// The future returned by [`Fetcher::fetch`].
    type Future: Future<Output = Result<T, E>> + MaybeSend + 'static;

    /// Start one fetch for `key`.
    fn fetch(&self, key: K) -> Self::Future;
}

impl<K, T, E, F, Fut> Fetcher<K, T, E> for F
where
    F: Fn(K) -> Fut + MaybeSend + MaybeSync + 'static,
    Fut: Future<Output = Result<T, E>> + MaybeSend + 'static,
{
    type Future = Fut;

    fn fetch(&self, key: K) -> Fut {
        self(key)
    }
}

/// Wrap a typed fetcher into the type-erased, `Arc`-ified form stored
/// per entry. The typed key is captured here; the stored `QueryKey` argument
/// is ignored.
pub(crate) fn erase<K, T, E, F>(key: K, fetcher: F) -> ErasedFetcher
where
    K: Clone + MaybeSend + MaybeSync + 'static,
    T: MaybeSend + MaybeSync + 'static,
    E: MaybeSend + MaybeSync + 'static,
    F: Fetcher<K, T, E>,
{
    Arc::new(move |_query_key: QueryKey| {
        let fut = fetcher.fetch(key.clone());
        Box::pin(async move {
            match fut.await {
                Ok(value) => Ok(Arc::new(value) as ErasedValue),
                Err(error) => Err(Arc::new(error) as ErasedValue),
            }
        }) as BoxedFuture<Result<ErasedValue, ErasedValue>>
    })
}

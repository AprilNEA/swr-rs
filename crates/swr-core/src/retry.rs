//! Error retry with exponential backoff — SWR's `shouldRetryOnError` /
//! `errorRetryInterval` / `errorRetryCount`, provided as a [`Fetcher`]
//! combinator per spec §13 (retry stays out of the state machine).
//!
//! Divergences from SWR's default `onErrorRetry` (D-28): the backoff is the
//! deterministic midpoint of SWR's jittered schedule, retries are not gated
//! on document visibility (headless), and the default retry count is finite —
//! a retrying flight keeps its entry's GC deferred (GC-1), so unlimited
//! retries against a dead endpoint would pin the entry forever.

use std::sync::Arc;
use std::time::Duration;

use crate::erased::BoxedFuture;
use crate::fetcher::Fetcher;
use crate::marker::{MaybeSend, MaybeSync};
use crate::runtime::Runtime;

#[cfg(not(target_arch = "wasm32"))]
type RetryPredicate<E> = Arc<dyn Fn(&E) -> bool + Send + Sync>;
#[cfg(target_arch = "wasm32")]
type RetryPredicate<E> = Arc<dyn Fn(&E) -> bool>;

/// Exponential backoff schedule (SWR's default `onErrorRetry` shape).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Base interval (SWR `errorRetryInterval`). The n-th retry waits
    /// `interval << min(n, 8)`. Default: 5 seconds.
    pub interval: Duration,
    /// Maximum number of retries after the first attempt (SWR
    /// `errorRetryCount`); `None` retries forever. Default: `Some(3)`.
    pub max_retries: Option<u32>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            max_retries: Some(3),
        }
    }
}

impl RetryPolicy {
    /// Delay before the `attempt`-th retry (1-based), matching SWR's
    /// `(1 << min(count, 8)) * errorRetryInterval` midpoint.
    fn delay(&self, attempt: u32) -> Duration {
        self.interval.saturating_mul(1u32 << attempt.min(8))
    }
}

/// [`Fetcher`] combinator: retry failed fetches with exponential backoff
/// before letting the error commit.
///
/// The whole retry loop runs inside one flight, so the entry stays
/// `is_validating` throughout and concurrent readers keep deduplicating onto
/// it. Local writes and invalidations still interrupt it: they discard the
/// flight (SEQ-3/D-5) and its eventual result is dropped.
pub struct Retry<F, E> {
    inner: Arc<F>,
    runtime: Arc<dyn Runtime>,
    policy: RetryPolicy,
    should_retry: RetryPredicate<E>,
}

impl<F, E> Retry<F, E> {
    /// Wrap `inner`, retrying every error per `policy`. The runtime supplies
    /// the backoff timer (RT-1).
    pub fn new(runtime: Arc<dyn Runtime>, inner: F, policy: RetryPolicy) -> Self {
        Self {
            inner: Arc::new(inner),
            runtime,
            policy,
            should_retry: Arc::new(|_| true),
        }
    }

    /// Only retry errors matching `pred` — e.g. skip 4xx responses, which a
    /// retry cannot fix (SWR's default skips 404 the same way).
    pub fn retry_if(mut self, pred: impl Fn(&E) -> bool + MaybeSend + MaybeSync + 'static) -> Self {
        self.should_retry = Arc::new(pred);
        self
    }
}

// Manual impl: cloning must not require `F: Clone` or `E: Clone`.
impl<F, E> Clone for Retry<F, E> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            runtime: Arc::clone(&self.runtime),
            policy: self.policy.clone(),
            should_retry: Arc::clone(&self.should_retry),
        }
    }
}

impl<K, T, E, F> Fetcher<K, T, E> for Retry<F, E>
where
    F: Fetcher<K, T, E>,
    K: Clone + MaybeSend + 'static,
    T: 'static,
    E: 'static,
{
    type Future = BoxedFuture<Result<T, E>>;

    fn fetch(&self, key: K) -> Self::Future {
        let inner = Arc::clone(&self.inner);
        let runtime = Arc::clone(&self.runtime);
        let policy = self.policy.clone();
        let should_retry = Arc::clone(&self.should_retry);
        Box::pin(async move {
            let mut attempt = 0u32;
            loop {
                // The match ends before the backoff sleep so the error (and
                // the attempt's result) is never held across the await.
                match inner.fetch(key.clone()).await {
                    Ok(value) => return Ok(value),
                    Err(error) => {
                        attempt += 1;
                        let exhausted = policy.max_retries.is_some_and(|max| attempt > max);
                        if exhausted || !should_retry(&error) {
                            return Err(error);
                        }
                    }
                }
                let deadline = runtime
                    .now()
                    .checked_add(policy.delay(attempt))
                    .expect("retry deadline representable");
                runtime.sleep_until(deadline).await;
            }
        })
    }
}

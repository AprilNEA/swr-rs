//! [`SwrClient`]: the public headless client (spec 7.1) and the async shell
//! that executes the state machine's effects (LOCK-1..LOCK-4, 5.6).

use std::future::Future;
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use tokio::sync::watch;

use crate::erased::{ErasedCompare, ErasedValue, downcast_value, erased_eq};
use crate::error::FetchError;
use crate::fetcher::{self, Fetcher};
use crate::handle::QueryHandle;
use crate::key::{IntoKeyPrefix, IntoQueryKey, QueryKey};
use crate::machine::{Effect, Event, HandleOutput, Inner, MutationToken, Outcome, ReadOutcome};
use crate::marker::{MaybeSend, MaybeSync};
use crate::options::{MutateFlags, MutateOptions, QueryOptions, ReadPolicy, SwrEvent};
use crate::runtime::Runtime;
use crate::snapshot::Snapshot;

/// Shared core: the single `Inner` lock (LOCK-1), the runtime, and the global
/// default options.
pub(crate) struct Shared {
    inner: Mutex<Inner>,
    runtime: Arc<dyn Runtime>,
    defaults: QueryOptions,
}

impl Shared {
    /// The event pipeline: lock → `handle()` → unlock → execute effects in
    /// order (EFF-1). Effects never run under the lock (LOCK-2, LOCK-3), and
    /// never re-enter it directly (EFF-4) — spawned tasks feed follow-up
    /// events through a fresh `dispatch`.
    pub(crate) fn dispatch(shared: &Arc<Self>, ev: Event) -> Outcome {
        let now = shared.runtime.now();
        let HandleOutput { outcome, effects } = { shared.inner.lock().handle(ev, now) };
        Self::run_effects(shared, effects);
        outcome
    }

    fn run_effects(shared: &Arc<Self>, effects: Vec<Effect>) {
        for effect in effects {
            match effect {
                Effect::StartFetch { key, seq, fetcher } => {
                    // D-3: detached — the fetch outlives its callers and
                    // commits to the cache even if every waiter is dropped.
                    let weak = Arc::downgrade(shared);
                    shared.runtime.spawn(Box::pin(async move {
                        let result = fetcher(key.clone()).await;
                        let Some(shared) = weak.upgrade() else { return };
                        let ev = match result {
                            Ok(value) => Event::CommitOk { key, seq, value },
                            Err(error) => Event::CommitErr { key, seq, error },
                        };
                        Shared::dispatch(&shared, ev);
                    }));
                }
                Effect::Notify { tx, snapshot } => {
                    // LOCK-3: sent outside the lock. A commit racing ahead of
                    // this send may already have published a newer snapshot;
                    // the version guard keeps the channel monotonic.
                    tx.send_if_modified(|current| {
                        if snapshot.version > current.version {
                            *current = snapshot;
                            true
                        } else {
                            false
                        }
                    });
                }
                Effect::ScheduleTimer {
                    key,
                    kind,
                    at,
                    generation,
                } => {
                    let weak = Arc::downgrade(shared);
                    let runtime = Arc::clone(&shared.runtime);
                    shared.runtime.spawn(Box::pin(async move {
                        runtime.sleep_until(at).await;
                        let Some(shared) = weak.upgrade() else { return };
                        Shared::dispatch(
                            &shared,
                            Event::TimerFired {
                                key,
                                kind,
                                generation,
                            },
                        );
                    }));
                }
            }
        }
    }

    /// Test hook: force-remove an entry, simulating a completed GC (IT2).
    #[cfg(test)]
    pub(crate) fn force_remove(&self, key: &QueryKey) {
        self.inner.lock().remove_entry_for_test(key);
    }
}

/// The headless SWR client (spec 7.1). Cheap to clone; all clones share one
/// cache.
#[derive(Clone)]
pub struct SwrClient {
    shared: Arc<Shared>,
}

/// Builder for [`SwrClient`], overriding the global default [`QueryOptions`].
#[derive(Debug, Default)]
pub struct SwrClientBuilder {
    defaults: QueryOptions,
}

impl SwrClientBuilder {
    /// Override the global default options used by `fetch` reads and as the
    /// aggregation fallback.
    pub fn default_options(mut self, defaults: QueryOptions) -> Self {
        self.defaults = defaults;
        self
    }

    /// Build the client on the given runtime.
    pub fn build(self, runtime: Arc<dyn Runtime>) -> SwrClient {
        SwrClient {
            shared: Arc::new(Shared {
                inner: Mutex::new(Inner::new(self.defaults.clone())),
                runtime,
                defaults: self.defaults,
            }),
        }
    }
}

/// Outcome of one pass through the wait loop (5.6).
enum WaitOutcome {
    Data(ErasedValue),
    Error(ErasedValue),
    Closed,
}

impl SwrClient {
    /// Create a client with default options on the given runtime.
    pub fn new(runtime: Arc<dyn Runtime>) -> Self {
        Self::builder().build(runtime)
    }

    /// Start building a client with custom defaults.
    pub fn builder() -> SwrClientBuilder {
        SwrClientBuilder::default()
    }

    /// One-shot read (the headless main interface). Behavior per
    /// [`ReadPolicy`]; see the E1–E3 transition tables.
    ///
    /// The core applies no timeout (WAIT-3): wrap the returned future in
    /// `tokio::time::timeout` or an equivalent if you need one. The fetch
    /// itself is detached and completes even if this future is dropped (D-3).
    pub async fn fetch<K, T, E, F>(
        &self,
        key: K,
        fetcher: F,
        policy: ReadPolicy,
    ) -> Result<Arc<T>, FetchError<E>>
    where
        K: IntoQueryKey<T, E> + Clone + MaybeSend + MaybeSync + 'static,
        T: MaybeSend + MaybeSync + 'static,
        E: MaybeSend + MaybeSync + 'static,
        F: Fetcher<K, T, E>,
    {
        self.fetch_inner(key, fetcher, policy, None).await
    }

    /// [`SwrClient::fetch`] with structural sharing (D-30): commits whose
    /// value equals the cached one (per `T: PartialEq`) keep the existing
    /// `Arc`, so consumers can detect "content unchanged" with
    /// [`Arc::ptr_eq`]. Freshness, seq progression, and notifications are
    /// unaffected (CMP-1).
    pub async fn fetch_eq<K, T, E, F>(
        &self,
        key: K,
        fetcher: F,
        policy: ReadPolicy,
    ) -> Result<Arc<T>, FetchError<E>>
    where
        K: IntoQueryKey<T, E> + Clone + MaybeSend + MaybeSync + 'static,
        T: PartialEq + MaybeSend + MaybeSync + 'static,
        E: MaybeSend + MaybeSync + 'static,
        F: Fetcher<K, T, E>,
    {
        self.fetch_inner(key, fetcher, policy, Some(erased_eq::<T>()))
            .await
    }

    async fn fetch_inner<K, T, E, F>(
        &self,
        key: K,
        fetcher: F,
        policy: ReadPolicy,
        compare: Option<ErasedCompare>,
    ) -> Result<Arc<T>, FetchError<E>>
    where
        K: IntoQueryKey<T, E> + Clone + MaybeSend + MaybeSync + 'static,
        T: MaybeSend + MaybeSync + 'static,
        E: MaybeSend + MaybeSync + 'static,
        F: Fetcher<K, T, E>,
    {
        let query_key = key.clone().into_query_key();
        let erased = fetcher::erase(key, fetcher);
        loop {
            let outcome = Shared::dispatch(
                &self.shared,
                Event::Read {
                    key: query_key.clone(),
                    policy,
                    fetcher: Some(erased.clone()),
                    compare: compare.clone(),
                    opts: self.shared.defaults.clone(),
                },
            );
            let Outcome::Read(read) = outcome else {
                unreachable!("Read events yield Read outcomes");
            };
            match read {
                ReadOutcome::Ready(snapshot) => return ready_result(snapshot),
                ReadOutcome::NoFetcher => return Err(FetchError::NoFetcher),
                ReadOutcome::Wait { target, rx } => {
                    match self.wait(rx, target, &query_key).await {
                        WaitOutcome::Data(value) => return Ok(downcast_value(value)),
                        WaitOutcome::Error(error) => {
                            return Err(FetchError::Fetch(downcast_value(error)));
                        }
                        // 5.6 ★: the entry was GC'd mid-wait. Re-issue the full
                        // read — the rebuilt entry restarts its seq space, so
                        // the old target must be replaced, not reused.
                        WaitOutcome::Closed => continue,
                    }
                }
            }
        }
    }

    /// The wait loop (5.6). Depends only on the watch channel — never on the
    /// `Inner` lock — and never busy-waits (WAIT-1): each iteration either
    /// returns or parks in `changed()`.
    async fn wait(
        &self,
        mut rx: watch::Receiver<Snapshot>,
        target: u64,
        key: &QueryKey,
    ) -> WaitOutcome {
        loop {
            let poke = {
                let snapshot = rx.borrow_and_update();
                if snapshot.data_seq >= target {
                    // `>=`, not `==`: a newer local write satisfies the wait
                    // just as well (D-7).
                    let value = snapshot
                        .data
                        .clone()
                        .expect("data_seq advanced implies data present");
                    return WaitOutcome::Data(value);
                }
                if snapshot.error_seq >= target {
                    // A newer attempt's failure is also a complete result not
                    // older than the target. Mutation errors never set
                    // error_seq (WAIT-4), so they are never returned here.
                    let error = snapshot
                        .error
                        .clone()
                        .expect("error_seq advanced implies error present");
                    return WaitOutcome::Error(error);
                }
                snapshot.inflight.is_none() && !snapshot.is_mutating
            };
            if poke {
                // WAIT-1: idempotent poke (E6 dedups); always fall through to
                // the await — never re-check immediately.
                Shared::dispatch(
                    &self.shared,
                    Event::RevalidateRequested { key: key.clone() },
                );
            }
            if rx.changed().await.is_err() {
                return WaitOutcome::Closed;
            }
        }
    }

    /// Long-lived subscription; returns the RAII [`QueryHandle`].
    pub fn subscribe<K, T, E, F>(&self, key: K, fetcher: F, opts: QueryOptions) -> QueryHandle<T, E>
    where
        K: IntoQueryKey<T, E> + Clone + MaybeSend + MaybeSync + 'static,
        T: MaybeSend + MaybeSync + 'static,
        E: MaybeSend + MaybeSync + 'static,
        F: Fetcher<K, T, E>,
    {
        self.subscribe_inner(key, fetcher, opts, None)
    }

    /// [`SwrClient::subscribe`] with structural sharing (D-30): commits whose
    /// value equals the cached one (per `T: PartialEq`) keep the existing
    /// `Arc`. A subscriber can then skip rebuilding downstream views with an
    /// O(1) [`Arc::ptr_eq`] check on the snapshot data. Notifications still
    /// fire on every commit (CMP-1); only the `Arc` identity is stabilized.
    pub fn subscribe_eq<K, T, E, F>(
        &self,
        key: K,
        fetcher: F,
        opts: QueryOptions,
    ) -> QueryHandle<T, E>
    where
        K: IntoQueryKey<T, E> + Clone + MaybeSend + MaybeSync + 'static,
        T: PartialEq + MaybeSend + MaybeSync + 'static,
        E: MaybeSend + MaybeSync + 'static,
        F: Fetcher<K, T, E>,
    {
        self.subscribe_inner(key, fetcher, opts, Some(erased_eq::<T>()))
    }

    fn subscribe_inner<K, T, E, F>(
        &self,
        key: K,
        fetcher: F,
        opts: QueryOptions,
        compare: Option<ErasedCompare>,
    ) -> QueryHandle<T, E>
    where
        K: IntoQueryKey<T, E> + Clone + MaybeSend + MaybeSync + 'static,
        T: MaybeSend + MaybeSync + 'static,
        E: MaybeSend + MaybeSync + 'static,
        F: Fetcher<K, T, E>,
    {
        let query_key = key.clone().into_query_key();
        let erased = fetcher::erase(key, fetcher);
        let outcome = Shared::dispatch(
            &self.shared,
            Event::Subscribe {
                key: query_key.clone(),
                fetcher: erased,
                compare,
                opts,
            },
        );
        let Outcome::Subscribed { sub_id, rx } = outcome else {
            unreachable!("Subscribe events yield Subscribed outcomes");
        };
        QueryHandle::new(rx, query_key, sub_id, Arc::downgrade(&self.shared))
    }

    /// Synchronous local write — SWR's `mutate(key, data, { revalidate: false })`.
    /// Counts as fresh, authoritative data (D-7) and discards any in-flight
    /// request for the key (SEQ-3).
    pub fn set<K, T, E>(&self, key: K, value: T)
    where
        K: IntoQueryKey<T, E>,
        T: MaybeSend + MaybeSync + 'static,
        E: 'static,
    {
        Shared::dispatch(
            &self.shared,
            Event::MutateSet {
                key: key.into_query_key(),
                value: Arc::new(value),
            },
        );
    }

    /// Async mutation; optimistic updates go through here (E10/E11).
    ///
    /// While the mutation runs, fetch commits for the key are discarded (D-6).
    /// If this future is dropped before `fut` completes, the mutation is
    /// aborted: the optimistic write rolls back per SEQ-4 and the entry
    /// revalidates per `opts.revalidate`.
    pub async fn mutate<K, T, E, Fut>(
        &self,
        key: K,
        opts: MutateOptions<T>,
        fut: Fut,
    ) -> Result<Option<Arc<T>>, Arc<E>>
    where
        K: IntoQueryKey<T, E>,
        T: MaybeSend + MaybeSync + 'static,
        E: MaybeSend + MaybeSync + 'static,
        Fut: Future<Output = Result<Option<T>, E>>,
    {
        let query_key = key.into_query_key();
        let flags = MutateFlags::from(&opts);
        let optimistic = opts.optimistic.map(|v| Arc::new(v) as ErasedValue);
        let outcome = Shared::dispatch(
            &self.shared,
            Event::MutateBegin {
                key: query_key,
                optimistic,
            },
        );
        let Outcome::Mutation(token) = outcome else {
            unreachable!("MutateBegin events yield Mutation outcomes");
        };
        let mut guard = MutationGuard {
            shared: Arc::downgrade(&self.shared),
            token: Some(token),
            flags,
        };

        let result = fut.await;

        let token = guard.token.take().expect("guard token taken exactly once");
        drop(guard);
        let (event_result, ret) = match result {
            Ok(Some(value)) => {
                let value = Arc::new(value);
                (Ok(Some(Arc::clone(&value) as ErasedValue)), Ok(Some(value)))
            }
            Ok(None) => (Ok(None), Ok(None)),
            Err(error) => {
                let error = Arc::new(error);
                (Err(Arc::clone(&error) as ErasedValue), Err(error))
            }
        };
        Shared::dispatch(
            &self.shared,
            Event::MutateCommit {
                token,
                result: event_result,
                flags,
            },
        );
        ret
    }

    /// Mark every entry under `prefix` stale (E12, K-2). Active entries
    /// refetch immediately; idle ones refetch on their next read.
    pub fn invalidate(&self, prefix: impl IntoKeyPrefix) {
        Shared::dispatch(
            &self.shared,
            Event::Invalidate {
                prefix: prefix.into_prefix(),
            },
        );
    }

    /// Request a revalidation for one key (E6; deduplicated).
    pub fn revalidate<K, T, E>(&self, key: K)
    where
        K: IntoQueryKey<T, E>,
        T: 'static,
        E: 'static,
    {
        Shared::dispatch(
            &self.shared,
            Event::RevalidateRequested {
                key: key.into_query_key(),
            },
        );
    }

    /// Feed an environment event (browser focus, connectivity, ...) from the
    /// host (E13).
    pub fn broadcast(&self, ev: SwrEvent) {
        Shared::dispatch(&self.shared, Event::Broadcast { ev });
    }

    /// The configured default options.
    pub fn default_options(&self) -> &QueryOptions {
        &self.shared.defaults
    }

    /// Test hook: force-remove an entry, simulating a completed GC (IT2).
    #[cfg(test)]
    pub(crate) fn force_remove(&self, key: &QueryKey) {
        self.shared.force_remove(key);
    }
}

/// Cancel-safety guard for [`SwrClient::mutate`]: if the mutate future is
/// dropped after `MutateBegin`, aborting releases `mutation_active` instead of
/// wedging the entry forever.
struct MutationGuard {
    shared: Weak<Shared>,
    token: Option<MutationToken>,
    flags: MutateFlags,
}

impl Drop for MutationGuard {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            if let Some(shared) = self.shared.upgrade() {
                Shared::dispatch(
                    &shared,
                    Event::MutateAbort {
                        token,
                        flags: self.flags,
                    },
                );
            }
        }
    }
}

/// Convert an immediate snapshot into the `fetch()` result. Stale data wins
/// over a stored error (stale-while-error); a truly empty `CacheOnly` hit is
/// a [`FetchError::Miss`].
fn ready_result<T, E>(snapshot: Snapshot) -> Result<Arc<T>, FetchError<E>>
where
    T: MaybeSend + MaybeSync + 'static,
    E: MaybeSend + MaybeSync + 'static,
{
    if let Some(value) = snapshot.data {
        return Ok(downcast_value(value));
    }
    if let Some(error) = snapshot.error {
        return Err(FetchError::Fetch(downcast_value(error)));
    }
    Err(FetchError::Miss)
}

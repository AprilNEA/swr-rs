//! [`SwrClient`]: the public headless client and the async shell
//! that executes the state machine's effects.

use std::future::Future;
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use tokio::sync::watch;

use crate::erased::{ErasedCompare, ErasedFetcher, ErasedValue, downcast_value, erased_eq};
use crate::error::FetchError;
use crate::fetcher::{self, Fetcher};
use crate::handle::QueryHandle;
use crate::key::{IntoKeyPrefix, IntoQueryKey, QueryKey};
use crate::machine::{Effect, Event, HandleOutput, Inner, MutationToken, Outcome, ReadOutcome};
use crate::marker::{MaybeSend, MaybeSync};
use crate::options::{MutateFlags, MutateOptions, QueryOptions, ReadPolicy, SwrEvent};
use crate::runtime::Runtime;
use crate::snapshot::Snapshot;

/// Shared core: the single `Inner` lock, the runtime, and the global
/// default options.
pub(crate) struct Shared {
    inner: Mutex<Inner>,
    runtime: Arc<dyn Runtime>,
    defaults: QueryOptions,
}

impl Shared {
    /// The event pipeline: lock → `handle()` → unlock → execute effects in
    /// order. Effects never run under the lock, and
    /// never re-enter it directly — spawned tasks feed follow-up
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
                Effect::StartFetch {
                    key,
                    incarnation,
                    seq,
                    fetcher,
                } => {
                    // detached — the fetch outlives its callers and
                    // commits to the cache even if every waiter is dropped.
                    let weak = Arc::downgrade(shared);
                    shared.runtime.spawn(Box::pin(async move {
                        let result = fetcher(key.clone()).await;
                        let Some(shared) = weak.upgrade() else { return };
                        let ev = match result {
                            Ok(value) => Event::CommitOk {
                                key,
                                incarnation,
                                seq,
                                value,
                            },
                            Err(error) => Event::CommitErr {
                                key,
                                incarnation,
                                seq,
                                error,
                            },
                        };
                        Shared::dispatch(&shared, ev);
                    }));
                }
                Effect::Notify { tx, snapshot } => {
                    // sent outside the lock. A commit racing ahead of
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

    /// Test hook: force-remove an entry, simulating a completed GC.
    #[cfg(test)]
    pub(crate) fn force_remove(&self, key: &QueryKey) {
        self.inner.lock().remove_entry_for_test(key);
    }
}

/// The headless SWR client. Cheap to clone; all clones share one
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

/// Weak counterpart of [`SwrClient`].
///
/// Dependent-query fetchers — fetchers that call back into the client for
/// other keys — must capture this instead of a strong client: fetchers are
/// stored inside the cache, so a strong capture would form a reference cycle
/// (`Shared → fetcher → Shared`) keeping the whole cache and its timers alive
/// after the last external client drops.
#[derive(Clone)]
pub struct WeakSwrClient {
    shared: Weak<Shared>,
}

impl WeakSwrClient {
    /// Upgrade to a usable client. `None` once every strong [`SwrClient`]
    /// has been dropped.
    pub fn upgrade(&self) -> Option<SwrClient> {
        self.shared.upgrade().map(|shared| SwrClient { shared })
    }
}

/// Outcome of one pass through the wait loop.
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

    /// A weak handle for dependent-query fetchers: fetchers
    /// that fetch other keys capture this and [`upgrade`](WeakSwrClient::upgrade)
    /// per call. The dependency graph between keys must stay acyclic — a key
    /// that (transitively) fetches itself parks as a never-completing flight
    /// (the core does not detect cycles; caller timeouts are the backstop).
    pub fn downgrade(&self) -> WeakSwrClient {
        WeakSwrClient {
            shared: Arc::downgrade(&self.shared),
        }
    }

    /// One-shot read (the headless main interface). Behavior per
    /// [`ReadPolicy`].
    ///
    /// The core applies no timeout: wrap the returned future in
    /// `tokio::time::timeout` or an equivalent if you need one. The fetch
    /// itself is detached and completes even if this future is dropped.
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

    /// [`SwrClient::fetch`] with structural sharing: commits whose
    /// value equals the cached one (per `T: PartialEq`) keep the existing
    /// `Arc`, so consumers can detect "content unchanged" with
    /// [`Arc::ptr_eq`]. Freshness, seq progression, and notifications are
    /// unaffected.
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

    /// The wait loop. Depends only on the watch channel — never on the
    /// `Inner` lock — and never busy-waits: each iteration either
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
                    // just as well.
                    let value = snapshot
                        .data
                        .clone()
                        .expect("data_seq advanced implies data present");
                    return WaitOutcome::Data(value);
                }
                if snapshot.error_seq >= target {
                    // A newer attempt's failure is also a complete result not
                    // older than the target. Mutation errors never set
                    // error_seq, so they are never returned here.
                    let error = snapshot
                        .error
                        .clone()
                        .expect("error_seq advanced implies error present");
                    return WaitOutcome::Error(error);
                }
                snapshot.inflight.is_none() && !snapshot.is_mutating
            };
            if poke {
                // idempotent poke (revalidation requests dedup); always fall through to
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
        let query_key = key.clone().into_query_key();
        let erased = fetcher::erase(key, fetcher);
        self.subscribe_inner(query_key, Some(erased), opts, None)
    }

    /// Observer-only subscription: watch a key without providing a
    /// fetcher — for entries fed purely by [`SwrClient::set`] / mutations, or
    /// when another call site already owns the fetcher registration
    /// (fetcher last-wins makes re-supplying closures per subscription a
    /// footgun).
    ///
    /// While the entry has no stored fetcher, revalidation requests are inert
    /// and reads without data yield
    /// [`FetchError::NoFetcher`](crate::FetchError::NoFetcher); the first
    /// `fetch`/`subscribe` that supplies a fetcher makes them live.
    pub fn observe<K, T, E>(&self, key: K, opts: QueryOptions) -> QueryHandle<T, E>
    where
        K: IntoQueryKey<T, E>,
        T: MaybeSend + MaybeSync + 'static,
        E: MaybeSend + MaybeSync + 'static,
    {
        self.subscribe_inner(key.into_query_key(), None, opts, None)
    }

    /// [`SwrClient::subscribe`] with structural sharing: commits whose
    /// value equals the cached one (per `T: PartialEq`) keep the existing
    /// `Arc`. A subscriber can then skip rebuilding downstream views with an
    /// O(1) [`Arc::ptr_eq`] check on the snapshot data. Notifications still
    /// fire on every commit; only the `Arc` identity is stabilized.
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
        let query_key = key.clone().into_query_key();
        let erased = fetcher::erase(key, fetcher);
        self.subscribe_inner(query_key, Some(erased), opts, Some(erased_eq::<T>()))
    }

    fn subscribe_inner<T, E>(
        &self,
        query_key: QueryKey,
        fetcher: Option<ErasedFetcher>,
        opts: QueryOptions,
        compare: Option<ErasedCompare>,
    ) -> QueryHandle<T, E>
    where
        T: MaybeSend + MaybeSync + 'static,
        E: MaybeSend + MaybeSync + 'static,
    {
        let outcome = Shared::dispatch(
            &self.shared,
            Event::Subscribe {
                key: query_key.clone(),
                fetcher,
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
    /// Counts as fresh, authoritative data and discards any in-flight
    /// request for the key.
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

    /// Async mutation; optimistic updates go through here.
    ///
    /// While the mutation runs, fetch commits for the key are discarded.
    /// If this future is dropped before `fut` completes, the mutation is
    /// aborted: the optimistic write rolls back (unless overwritten meanwhile) and the entry
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

    /// Mark every entry under `prefix` stale. Active entries
    /// refetch immediately; idle ones refetch on their next read.
    pub fn invalidate(&self, prefix: impl IntoKeyPrefix) {
        Shared::dispatch(
            &self.shared,
            Event::Invalidate {
                prefix: prefix.into_prefix(),
            },
        );
    }

    /// Request a revalidation for one key (deduplicated against in-flight
    /// requests).
    pub fn revalidate<K, T, E>(&self, key: K)
    where
        K: IntoQueryKey<T, E>,
        T: 'static,
        E: 'static,
    {
        self.revalidate_key(key.into_query_key());
    }

    /// [`revalidate`](SwrClient::revalidate) for an already-built
    /// [`QueryKey`] — for adapters and callers that only hold the
    /// erased key. Safe without type parameters: revalidation never touches
    /// typed values.
    pub fn revalidate_key(&self, key: QueryKey) {
        Shared::dispatch(&self.shared, Event::RevalidateRequested { key });
    }

    /// Feed an environment event (browser focus, connectivity, ...) from the
    /// host.
    pub fn broadcast(&self, ev: SwrEvent) {
        Shared::dispatch(&self.shared, Event::Broadcast { ev });
    }

    /// The configured default options.
    pub fn default_options(&self) -> &QueryOptions {
        &self.shared.defaults
    }

    /// Test hook: force-remove an entry, simulating a completed GC.
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

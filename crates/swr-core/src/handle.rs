//! [`QueryHandle`]: the RAII subscription handle.

use std::marker::PhantomData;
use std::sync::Weak;

use tokio::sync::watch;

use crate::client::Shared;
use crate::error::Closed;
use crate::key::QueryKey;
use crate::machine::Event;
use crate::marker::{MaybeSend, MaybeSync};
use crate::snapshot::{QueryState, Snapshot};

/// A long-lived subscription to one key. Dropping the handle unsubscribes.
///
/// The intended consumption pattern is `snapshot()` + `changed().await`: a
/// server loop awaits `changed` and re-reads the snapshot; a UI adapter sets a
/// signal after each `changed`.
pub struct QueryHandle<T, E> {
    rx: watch::Receiver<Snapshot>,
    key: QueryKey,
    sub_id: u64,
    shared: Weak<Shared>,
    _types: PhantomData<fn() -> (T, E)>,
}

impl<T, E> QueryHandle<T, E>
where
    T: MaybeSend + MaybeSync + 'static,
    E: MaybeSend + MaybeSync + 'static,
{
    pub(crate) fn new(
        rx: watch::Receiver<Snapshot>,
        key: QueryKey,
        sub_id: u64,
        shared: Weak<Shared>,
    ) -> Self {
        Self {
            rx,
            key,
            sub_id,
            shared,
            _types: PhantomData,
        }
    }

    /// The current state. Synchronous and lock-free: render paths are
    /// never blocked by the state machine.
    pub fn snapshot(&self) -> QueryState<T, E> {
        QueryState::from_snapshot(self.rx.borrow().clone())
    }

    /// Wait until the state changed since last seen. Watch semantics: only
    /// "changed" is guaranteed; intermediate states may be skipped.
    ///
    /// Returns `Err(Closed)` once the entry has been garbage-collected;
    /// re-subscribe to keep observing the key.
    ///
    /// Observing many keys: one task per handle is the intended
    /// baseline. To multiplex N handles in a single task, race their
    /// `changed()` futures — they are cancel-safe (nothing is marked seen
    /// unless the future completes), so dropping and re-creating them each
    /// round loses no notifications. `tokio::select!` covers fixed sets;
    /// for dynamic N, box the futures and race them with
    /// `futures::future::select_all` or a small `poll_fn` loop, then re-read
    /// `snapshot()` on whichever woke.
    pub async fn changed(&mut self) -> Result<(), Closed> {
        self.rx.changed().await.map_err(|_| Closed)
    }

    /// Request a revalidation (deduplicated against in-flight requests).
    pub fn revalidate(&self) {
        if let Some(shared) = self.shared.upgrade() {
            Shared::dispatch(
                &shared,
                Event::RevalidateRequested {
                    key: self.key.clone(),
                },
            );
        }
    }

    /// The subscribed key.
    pub fn key(&self) -> &QueryKey {
        &self.key
    }
}

impl<T, E> Drop for QueryHandle<T, E> {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.upgrade() {
            Shared::dispatch(
                &shared,
                Event::Unsubscribe {
                    key: self.key.clone(),
                    sub_id: self.sub_id,
                },
            );
        }
    }
}

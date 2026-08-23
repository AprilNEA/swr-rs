//! GPUI adapter for swr.
//!
//! Two pieces:
//!
//! - [`GpuiRuntime`]: the [`swr_core::Runtime`] on GPUI's own executors — no
//!   tokio required. Fetches and timers run on the [`BackgroundExecutor`];
//!   time comes from the executor's clock, so `advance_clock` drives
//!   staleness, GC, and refresh deterministically under `#[gpui::test]`.
//! - [`Query`]: binds a [`QueryHandle`] to GPUI. An [`Entity`] holds the
//!   latest [`QueryState`]; a foreground watcher task keeps it fresh and
//!   calls `cx.notify()` on every change, so views observe a query exactly
//!   like any other entity ([`gpui::App::observe`]) and read it lock-free
//!   during render.
//!
//! ```ignore
//! struct StatusView { query: Query<Status, ApiError> }
//!
//! impl StatusView {
//!     fn new(client: &SwrClient, cx: &mut Context<Self>) -> Self {
//!         let handle = client.subscribe_eq(("status",), fetch_status, QueryOptions::default());
//!         let query = Query::new(client, handle, cx);
//!         cx.observe(query.state(), |_, _, cx| cx.notify()).detach();
//!         Self { query }
//!     }
//! }
//!
//! impl Render for StatusView {
//!     fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
//!         let state = self.query.read(cx);
//!         div().child(match &state.data {
//!             Some(status) => format!("{status:?}"),
//!             None if state.is_loading => "loading...".to_string(),
//!             None => "no data".to_string(),
//!         })
//!     }
//! }
//! ```
#![deny(missing_docs)]

use std::sync::Arc;

use gpui::{App, AppContext as _, BackgroundExecutor, Entity, Task};
use swr_core::{
    Instant, MaybeSend, MaybeSync, QueryHandle, QueryKey, QueryState, Runtime, RuntimeFuture,
    SwrClient, WeakSwrClient,
};

/// [`Runtime`] backed by GPUI's [`BackgroundExecutor`].
///
/// Time goes through the executor's clock, so
/// [`advance_clock`](BackgroundExecutor::advance_clock) moves staleness, GC,
/// and refresh timers in tests.
#[derive(Clone)]
pub struct GpuiRuntime {
    executor: BackgroundExecutor,
}

impl GpuiRuntime {
    /// Capture the app's background executor.
    pub fn new(cx: &App) -> Self {
        Self {
            executor: cx.background_executor().clone(),
        }
    }
}

impl From<BackgroundExecutor> for GpuiRuntime {
    fn from(executor: BackgroundExecutor) -> Self {
        Self { executor }
    }
}

impl Runtime for GpuiRuntime {
    fn now(&self) -> Instant {
        self.executor.now()
    }

    fn spawn(&self, fut: RuntimeFuture) {
        // GPUI tasks cancel on drop; swr's fetches and timers are detached
        // by contract.
        self.executor.spawn(fut).detach();
    }

    fn sleep_until(&self, at: Instant) -> RuntimeFuture {
        let executor = self.executor.clone();
        Box::pin(async move {
            let delay = at.saturating_duration_since(executor.now());
            executor.timer(delay).await;
        })
    }
}

/// Build an [`SwrClient`] on GPUI's background executor.
pub fn client(cx: &App) -> SwrClient {
    SwrClient::new(Arc::new(GpuiRuntime::new(cx)))
}

/// A query bound to GPUI.
///
/// The latest [`QueryState`] lives in an [`Entity`]; a foreground watcher
/// task applies every change and calls `cx.notify()`, so any view can
/// `cx.observe(query.state(), ...)` and read the state during render.
///
/// Dropping the `Query` cancels the watcher, which drops the underlying
/// [`QueryHandle`] and unsubscribes (the entry then follows normal GC).
pub struct Query<T: 'static, E: 'static> {
    state: Entity<QueryState<T, E>>,
    key: QueryKey,
    client: WeakSwrClient,
    _watcher: Task<()>,
}

impl<T, E> Query<T, E>
where
    T: MaybeSend + MaybeSync + 'static,
    E: MaybeSend + MaybeSync + 'static,
{
    /// Bind `handle` (from `subscribe`/`subscribe_eq`/`observe`) to GPUI.
    pub fn new(client: &SwrClient, handle: QueryHandle<T, E>, cx: &mut App) -> Self {
        let key = handle.key().clone();
        let state = cx.new(|_| handle.snapshot());
        let weak_state = state.downgrade();
        let watcher = cx.spawn(async move |cx| {
            let mut handle = handle;
            // Closed only happens once the entry is gone; a live subscription
            // pins it (GC skips subscribed entries), so ending is correct.
            while handle.changed().await.is_ok() {
                let snapshot = handle.snapshot();
                let updated = weak_state.update(cx, |state, cx| {
                    *state = snapshot;
                    cx.notify();
                });
                if updated.is_err() {
                    break; // state entity released
                }
            }
        });
        Self {
            state,
            key,
            client: client.downgrade(),
            _watcher: watcher,
        }
    }

    /// The entity holding the latest state — observe it like any entity.
    pub fn state(&self) -> &Entity<QueryState<T, E>> {
        &self.state
    }

    /// Read the current state (lock-free; render-path safe).
    pub fn read<'a>(&self, cx: &'a App) -> &'a QueryState<T, E> {
        self.state.read(cx)
    }

    /// Request a revalidation (deduplicated against in-flight requests).
    pub fn revalidate(&self) {
        if let Some(client) = self.client.upgrade() {
            client.revalidate_key(self.key.clone());
        }
    }

    /// The observed key.
    pub fn key(&self) -> &QueryKey {
        &self.key
    }
}

//! Browser implementation of the [`swr_core::Runtime`] trait (spec chapter 8,
//! M6): tasks run on the browser event loop via `spawn_local`, timers via
//! `gloo-timers`, time via the Performance API (`web_time`), plus a shared,
//! reference-counted focus/online event source forwarding DOM events to
//! [`SwrClient::broadcast`].
//!
//! On non-wasm targets this crate compiles to an empty library.
#![cfg(target_arch = "wasm32")]
#![deny(missing_docs)]

use std::cell::RefCell;

use gloo_events::EventListener;
use swr_core::{Instant, Runtime, RuntimeFuture, SwrClient, SwrEvent};

/// [`Runtime`] backed by the browser event loop.
///
/// `sleep_until` is implemented over `setTimeout`, so deadlines further out
/// than `u32::MAX` milliseconds (~49.7 days) are not supported.
#[derive(Clone, Copy, Debug, Default)]
pub struct WebRuntime;

impl WebRuntime {
    /// Create the runtime.
    pub fn new() -> Self {
        Self
    }
}

impl Runtime for WebRuntime {
    fn now(&self) -> Instant {
        // web_time::Instant: Performance-API-backed on wasm (the std version
        // would panic here, spec chapter 8).
        Instant::now()
    }

    fn spawn(&self, fut: RuntimeFuture) {
        wasm_bindgen_futures::spawn_local(fut);
    }

    fn sleep_until(&self, at: Instant) -> RuntimeFuture {
        Box::pin(async move {
            // The delay is computed at poll time, not scheduling time.
            let delay = at.saturating_duration_since(Instant::now());
            gloo_timers::future::sleep(delay).await;
        })
    }
}

/// RAII registration forwarding browser events to a client (spec chapter 8):
/// window `focus` and `visibilitychange`-to-visible become
/// [`SwrEvent::Focus`], window `online` becomes [`SwrEvent::Online`].
///
/// One shared set of DOM listeners is registered globally and
/// reference-counted: the first attachment adds them, dropping the last
/// [`WebEventSource`] removes them. Every attached client receives every
/// event.
#[must_use = "dropping the WebEventSource detaches the client from browser events"]
pub struct WebEventSource {
    id: u64,
}

impl WebEventSource {
    /// Attach `client` to the shared browser event listeners.
    ///
    /// # Panics
    ///
    /// Panics outside a browser environment (no `window`/`document`), e.g.
    /// under plain Node.
    pub fn attach(client: &SwrClient) -> Self {
        REGISTRY.with(|registry| registry.borrow_mut().attach(client.clone()))
    }
}

impl Drop for WebEventSource {
    fn drop(&mut self) {
        REGISTRY.with(|registry| registry.borrow_mut().detach(self.id));
    }
}

thread_local! {
    static REGISTRY: RefCell<Registry> = RefCell::new(Registry::default());
}

#[derive(Default)]
struct Registry {
    next_id: u64,
    clients: Vec<(u64, SwrClient)>,
    /// The single set of DOM listeners; `EventListener` detaches on drop.
    listeners: Option<Vec<EventListener>>,
}

impl Registry {
    fn attach(&mut self, client: SwrClient) -> WebEventSource {
        let id = self.next_id;
        self.next_id += 1;
        self.clients.push((id, client));
        if self.listeners.is_none() {
            self.listeners = Some(register_dom_listeners());
        }
        WebEventSource { id }
    }

    fn detach(&mut self, id: u64) {
        self.clients.retain(|(client_id, _)| *client_id != id);
        if self.clients.is_empty() {
            self.listeners = None;
        }
    }
}

fn register_dom_listeners() -> Vec<EventListener> {
    let window = web_sys::window().expect("browser environment: window missing");
    let document = window
        .document()
        .expect("browser environment: document missing");
    vec![
        EventListener::new(&window, "focus", |_event| broadcast_all(SwrEvent::Focus)),
        EventListener::new(&window, "online", |_event| broadcast_all(SwrEvent::Online)),
        EventListener::new(&document, "visibilitychange", |_event| {
            // Only the transition back to visible counts as a focus event.
            if document_visible() {
                broadcast_all(SwrEvent::Focus);
            }
        }),
    ]
}

fn document_visible() -> bool {
    web_sys::window()
        .and_then(|window| window.document())
        .is_some_and(|document| document.visibility_state() == web_sys::VisibilityState::Visible)
}

fn broadcast_all(ev: SwrEvent) {
    // Snapshot the clients first: a broadcast may synchronously drop or
    // attach sources, which would otherwise alias the RefCell borrow.
    let clients: Vec<SwrClient> = REGISTRY.with(|registry| {
        registry
            .borrow()
            .clients
            .iter()
            .map(|(_, client)| client.clone())
            .collect()
    });
    for client in clients {
        client.broadcast(ev);
    }
}

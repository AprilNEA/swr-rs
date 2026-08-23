//! Browser-only smoke test for the shared DOM event source: synthetic
//! `online` events reach every attached client, reference counting keeps the
//! listeners while any source lives, and detaching stops forwarding.
#![cfg(target_arch = "wasm32")]

use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use swr_core::{QueryHandle, QueryOptions, ReadPolicy, SwrClient};
use swr_runtime_web::{WebEventSource, WebRuntime};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn always_stale() -> QueryOptions {
    QueryOptions {
        stale_time: Duration::ZERO,
        gc_time: Duration::from_millis(50),
        ..QueryOptions::default()
    }
}

fn dispatch_online() {
    let window = web_sys::window().expect("browser environment: window");
    let event = web_sys::Event::new("online").expect("synthetic event");
    window.dispatch_event(&event).expect("dispatch");
}

async fn wait_value(handle: &mut QueryHandle<u32, String>, want: u32) {
    loop {
        if handle.snapshot().data.as_deref() == Some(&want) {
            return;
        }
        handle.changed().await.expect("channel open");
    }
}

#[wasm_bindgen_test]
async fn online_events_reach_attached_clients() {
    let client = SwrClient::builder()
        .default_options(always_stale())
        .build(Arc::new(WebRuntime::new()));
    let calls = Rc::new(Cell::new(0u32));
    let fetcher = {
        let calls = Rc::clone(&calls);
        move |_key: &'static str| {
            let calls = Rc::clone(&calls);
            async move {
                calls.set(calls.get() + 1);
                Ok::<u32, String>(calls.get())
            }
        }
    };

    let mut handle = client.subscribe::<_, u32, String, _>("k", fetcher, always_stale());
    wait_value(&mut handle, 1).await;

    let source_a = WebEventSource::attach(&client);
    let source_b = WebEventSource::attach(&client);
    dispatch_online();
    wait_value(&mut handle, 2).await;

    // One source dropped, one left: events still forward (refcount > 0).
    drop(source_a);
    dispatch_online();
    wait_value(&mut handle, 3).await;

    // Last source dropped: the DOM listeners are removed.
    drop(source_b);
    dispatch_online();
    gloo_timers::future::sleep(Duration::from_millis(50)).await;
    assert_eq!(calls.get(), 3, "detached clients see no further events");

    // The read path still works untouched.
    let value = client
        .fetch(
            "k",
            {
                let calls = Rc::clone(&calls);
                move |_key: &'static str| {
                    let calls = Rc::clone(&calls);
                    async move {
                        calls.set(calls.get() + 1);
                        Ok::<u32, String>(calls.get())
                    }
                }
            },
            ReadPolicy::EnsureFresh,
        )
        .await
        .expect("manual refetch");
    assert_eq!(*value, 4);
}

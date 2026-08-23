//! Wasm smoke test — the core's fetch/subscribe/snapshot path
//! on the browser event loop. Environment-agnostic (runs under Node or a
//! browser); the DOM event source is covered in `browser.rs`.
#![cfg(target_arch = "wasm32")]

use std::sync::Arc;
use std::time::Duration;

use swr_core::{QueryOptions, ReadPolicy, SwrClient};
use swr_runtime_web::WebRuntime;
use wasm_bindgen_test::wasm_bindgen_test;

/// Short gc_time so no long-lived `setTimeout` outlives the test process.
fn test_client() -> SwrClient {
    SwrClient::builder()
        .default_options(QueryOptions {
            gc_time: Duration::from_millis(50),
            ..QueryOptions::default()
        })
        .build(Arc::new(WebRuntime::new()))
}

#[wasm_bindgen_test]
async fn fetch_subscribe_snapshot_smoke() {
    let client = test_client();
    let fetcher = |name: &'static str| async move { Ok::<String, String>(format!("hi {name}")) };

    let value = client
        .fetch("web", fetcher, ReadPolicy::StaleWhileRevalidate)
        .await
        .expect("first load");
    assert_eq!(value.as_str(), "hi web");

    let mut handle = client.subscribe::<_, String, String, _>(
        "web",
        fetcher,
        QueryOptions {
            gc_time: Duration::from_millis(50),
            ..QueryOptions::default()
        },
    );
    let state = handle.snapshot();
    assert_eq!(state.data.expect("cached").as_str(), "hi web");
    assert!(!state.is_loading);

    handle.revalidate();
    handle.changed().await.expect("revalidation notifies");
    loop {
        let state = handle.snapshot();
        if !state.is_validating {
            assert_eq!(state.data.expect("refreshed").as_str(), "hi web");
            assert!(state.error.is_none());
            break;
        }
        handle.changed().await.expect("channel open");
    }
}

#[wasm_bindgen_test]
async fn timers_fire_on_the_event_loop() {
    let client = test_client();
    let fetcher = |_name: &'static str| async move { Ok::<u32, String>(7) };

    let value = client
        .fetch("timer", fetcher, ReadPolicy::EnsureFresh)
        .await
        .expect("load");
    assert_eq!(*value, 7);

    // With no subscribers the 50ms GC timer collects the entry.
    gloo_timers::future::sleep(Duration::from_millis(120)).await;
    let miss = client.fetch("timer", fetcher, ReadPolicy::CacheOnly).await;
    assert!(
        matches!(miss, Err(swr_core::FetchError::Miss)),
        "entry garbage-collected via the setTimeout-backed sleep"
    );
}

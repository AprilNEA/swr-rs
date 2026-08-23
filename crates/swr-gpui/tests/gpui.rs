//! Adapter tests on GPUI's deterministic test executor: the Runtime wiring
//! (fetch, virtual-clock timers) and the Query → entity bridge.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use gpui::TestAppContext;
use swr_core::{FetchError, QueryOptions, ReadPolicy, SwrClient};
use swr_gpui::{GpuiRuntime, Query};

fn counting_fetcher(
    calls: &Arc<AtomicU32>,
) -> impl Fn(&'static str) -> std::future::Ready<Result<u32, String>> + Clone + use<> {
    let calls = Arc::clone(calls);
    move |_key| std::future::ready(Ok(calls.fetch_add(1, Ordering::SeqCst) + 1))
}

fn short_gc_client(cx: &TestAppContext) -> SwrClient {
    SwrClient::builder()
        .default_options(QueryOptions {
            gc_time: Duration::from_secs(5),
            ..QueryOptions::default()
        })
        .build(Arc::new(GpuiRuntime::from(cx.executor())))
}

#[gpui::test]
async fn fetch_runs_on_the_gpui_executor(cx: &mut TestAppContext) {
    let client = cx.update(|cx| swr_gpui::client(cx));
    let calls = Arc::new(AtomicU32::new(0));

    let value = client
        .fetch("k", counting_fetcher(&calls), ReadPolicy::EnsureFresh)
        .await
        .expect("first load");
    assert_eq!(*value, 1);

    // Fresh within stale_time: served from the cache.
    let cached = client
        .fetch(
            "k",
            counting_fetcher(&calls),
            ReadPolicy::StaleWhileRevalidate,
        )
        .await
        .expect("cache hit");
    assert_eq!(*cached, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1, "one upstream call");
}

#[gpui::test]
async fn gc_runs_on_the_virtual_clock(cx: &mut TestAppContext) {
    let client = short_gc_client(cx);
    let calls = Arc::new(AtomicU32::new(0));

    let value = client
        .fetch("k", counting_fetcher(&calls), ReadPolicy::EnsureFresh)
        .await
        .expect("load");
    assert_eq!(*value, 1);

    cx.executor().advance_clock(Duration::from_secs(6));
    cx.run_until_parked();

    let miss = client
        .fetch("k", counting_fetcher(&calls), ReadPolicy::CacheOnly)
        .await;
    assert!(
        matches!(miss, Err(FetchError::Miss)),
        "idle entry collected once the virtual clock passed gc_time"
    );
}

#[gpui::test]
async fn refresh_ticks_on_the_virtual_clock(cx: &mut TestAppContext) {
    let client = cx.update(|cx| swr_gpui::client(cx));
    let calls = Arc::new(AtomicU32::new(0));

    let handle = client.subscribe::<_, u32, String, _>(
        "k",
        counting_fetcher(&calls),
        QueryOptions {
            refresh_interval: Some(Duration::from_secs(5)),
            ..QueryOptions::default()
        },
    );
    cx.run_until_parked();
    assert_eq!(calls.load(Ordering::SeqCst), 1, "initial load");

    cx.executor().advance_clock(Duration::from_secs(12));
    cx.run_until_parked();
    assert_eq!(calls.load(Ordering::SeqCst), 3, "refreshes at t+5 and t+10");
    drop(handle);
}

#[gpui::test]
async fn query_bridges_changes_into_entity_state(cx: &mut TestAppContext) {
    let client = cx.update(|cx| swr_gpui::client(cx));
    let calls = Arc::new(AtomicU32::new(0));

    let query = cx.update(|cx| {
        let handle = client.subscribe::<_, u32, String, _>(
            "k",
            counting_fetcher(&calls),
            QueryOptions::default(),
        );
        Query::new(&client, handle, cx)
    });
    cx.run_until_parked();
    cx.update(|cx| {
        let state = query.read(cx);
        assert_eq!(state.data.as_deref(), Some(&1), "first load applied");
        assert!(!state.is_validating);
    });

    // Local writes flow through the watcher into the entity.
    client.set::<_, u32, String>("k", 42);
    cx.run_until_parked();
    cx.update(|cx| assert_eq!(query.read(cx).data.as_deref(), Some(&42)));

    // revalidate() works from the erased key.
    query.revalidate();
    cx.run_until_parked();
    cx.update(|cx| assert_eq!(query.read(cx).data.as_deref(), Some(&2)));
}

#[gpui::test]
async fn dropping_the_query_unsubscribes(cx: &mut TestAppContext) {
    let client = short_gc_client(cx);
    let calls = Arc::new(AtomicU32::new(0));

    let query = cx.update(|cx| {
        let handle = client.subscribe::<_, u32, String, _>(
            "k",
            counting_fetcher(&calls),
            QueryOptions {
                // gc_time aggregates as the max across subscribers and touches; the
                // default 300s would out-vote the client's 5s default.
                gc_time: Duration::from_secs(5),
                ..QueryOptions::default()
            },
        );
        Query::new(&client, handle, cx)
    });
    cx.run_until_parked();
    cx.update(|cx| assert_eq!(query.read(cx).data.as_deref(), Some(&1)));

    drop(query);
    cx.run_until_parked();
    cx.executor().advance_clock(Duration::from_secs(6));
    cx.run_until_parked();

    let miss = client
        .fetch("k", counting_fetcher(&calls), ReadPolicy::CacheOnly)
        .await;
    assert!(
        matches!(miss, Err(FetchError::Miss)),
        "watcher cancelled, handle dropped, entry GC'd"
    );
}

//! M3–M5 integration tests (spec 9.2): the async shell on a paused tokio
//! clock. IT1–IT4 plus subscribe/GC/refresh and mutation end-to-end paths.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use crate::{
    Instant, MutateOptions, QueryHandle, QueryKey, QueryOptions, ReadPolicy, Runtime,
    RuntimeFuture, SwrClient,
};

struct TokioTestRuntime;

impl Runtime for TokioTestRuntime {
    fn now(&self) -> Instant {
        tokio::time::Instant::now().into_std()
    }

    fn spawn(&self, fut: RuntimeFuture) {
        tokio::spawn(fut);
    }

    fn sleep_until(&self, at: Instant) -> RuntimeFuture {
        Box::pin(async move {
            tokio::time::sleep_until(tokio::time::Instant::from_std(at)).await;
        })
    }
}

fn client() -> SwrClient {
    SwrClient::new(Arc::new(TokioTestRuntime))
}

/// Counter-based fetcher: returns 1, 2, 3, ... per call.
fn counting_fetcher(
    calls: &Arc<AtomicU32>,
) -> impl Fn(&'static str) -> std::future::Ready<Result<u32, String>> + use<> {
    let calls = Arc::clone(calls);
    move |_key| std::future::ready(Ok(calls.fetch_add(1, Ordering::SeqCst) + 1))
}

async fn wait_for(handle: &mut QueryHandle<u32, String>, want: u32) {
    loop {
        if handle.snapshot().data.as_deref() == Some(&want) {
            return;
        }
        handle.changed().await.expect("channel open");
    }
}

/// IT1: an EnsureFresh waiter survives a mutation discarding its flight — the
/// E11 step-4 notify wakes it, it pokes a fresh flight, and returns
/// (WAIT-1/WAIT-2).
#[tokio::test(start_paused = true)]
async fn it1_wait_loop_survives_mutation_interrupt() {
    let client = client();
    let calls = Arc::new(AtomicU32::new(0));
    let fetcher = {
        let calls = Arc::clone(&calls);
        move |_key: &'static str| {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if n == 0 {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    Ok::<u32, String>(1)
                } else {
                    Ok(2)
                }
            }
        }
    };
    let fetch_task = tokio::spawn({
        let client = client.clone();
        async move { client.fetch("k", fetcher, ReadPolicy::EnsureFresh).await }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;

    let mutated = client
        .mutate::<_, u32, String, _>(
            "k",
            MutateOptions {
                optimistic: None,
                rollback_on_error: true,
                populate: false,
                revalidate: false,
            },
            async { Ok(None) },
        )
        .await;
    assert!(mutated.is_ok());

    let value = fetch_task
        .await
        .expect("fetch task not cancelled")
        .expect("second flight succeeds");
    assert_eq!(*value, 2, "waiter converges on the poked flight");
}

/// IT2: an entry GC'd mid-wait closes the channel; the waiter re-issues the
/// read against the rebuilt entry with a fresh target (5.6 ★).
#[tokio::test(start_paused = true)]
async fn it2_closed_channel_self_heals() {
    let client = client();
    let calls = Arc::new(AtomicU32::new(0));
    let fetcher = {
        let calls = Arc::clone(&calls);
        move |_key: &'static str| {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if n == 0 {
                    std::future::pending::<()>().await;
                    unreachable!("first flight never completes");
                }
                Ok::<u32, String>(42)
            }
        }
    };
    let fetch_task = tokio::spawn({
        let client = client.clone();
        async move { client.fetch("k", fetcher, ReadPolicy::EnsureFresh).await }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Simulate a completed GC while the waiter is parked.
    client.force_remove(&QueryKey::new::<u32, String>("k"));

    let value = fetch_task
        .await
        .expect("fetch task not cancelled")
        .expect("rebuilt entry serves the read");
    assert_eq!(*value, 42);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "one flight per entry incarnation"
    );
}

/// IT3: SWR end-to-end — stale read returns the old value immediately, the
/// background refresh lands, and a subscriber observes the new value.
#[tokio::test(start_paused = true)]
async fn it3_swr_end_to_end() {
    let client = client();
    let calls = Arc::new(AtomicU32::new(0));

    let first = client
        .fetch(
            "k",
            counting_fetcher(&calls),
            ReadPolicy::StaleWhileRevalidate,
        )
        .await
        .expect("first load");
    assert_eq!(*first, 1);

    tokio::time::sleep(Duration::from_secs(3)).await; // past the 2s stale_time

    let stale = client
        .fetch(
            "k",
            counting_fetcher(&calls),
            ReadPolicy::StaleWhileRevalidate,
        )
        .await
        .expect("stale hit returns immediately");
    assert_eq!(*stale, 1, "old value served while revalidating");

    let mut handle = client.subscribe::<_, u32, String, _>(
        "k",
        counting_fetcher(&calls),
        QueryOptions::default(),
    );
    wait_for(&mut handle, 2).await;
    let state = handle.snapshot();
    assert!(!state.is_validating, "refresh settled");
    assert!(state.error.is_none());
}

/// IT4: the fetch task is detached from its caller — a timed-out `fetch()`
/// still commits to the cache (D-3).
#[tokio::test(start_paused = true)]
async fn it4_detached_fetch_commits_after_caller_timeout() {
    let client = client();
    let fetcher = |_key: &'static str| async {
        tokio::time::sleep(Duration::from_secs(5)).await;
        Ok::<u32, String>(7)
    };

    let timed_out = tokio::time::timeout(
        Duration::from_secs(1),
        client.fetch("k", fetcher, ReadPolicy::EnsureFresh),
    )
    .await;
    assert!(
        timed_out.is_err(),
        "caller gave up (WAIT-3: timeouts belong to the caller)"
    );

    tokio::time::sleep(Duration::from_secs(10)).await;

    let cached = client
        .fetch("k", fetcher, ReadPolicy::CacheOnly)
        .await
        .expect("detached fetch committed");
    assert_eq!(*cached, 7);
}

/// M4: dropping the last handle starts the GC countdown; reads keep the entry
/// alive; expiry removes it (GC-1, E14).
#[tokio::test(start_paused = true)]
async fn gc_collects_after_unsubscribe() {
    let client = SwrClient::builder()
        .default_options(QueryOptions {
            gc_time: Duration::from_secs(5),
            ..QueryOptions::default()
        })
        .build(Arc::new(TokioTestRuntime));
    let calls = Arc::new(AtomicU32::new(0));

    let mut handle = client.subscribe::<_, u32, String, _>(
        "k",
        counting_fetcher(&calls),
        QueryOptions {
            gc_time: Duration::from_secs(5),
            ..QueryOptions::default()
        },
    );
    wait_for(&mut handle, 1).await;
    drop(handle);

    tokio::time::sleep(Duration::from_secs(1)).await;
    let cached = client
        .fetch("k", counting_fetcher(&calls), ReadPolicy::CacheOnly)
        .await
        .expect("still cached before gc_time");
    assert_eq!(*cached, 1);

    tokio::time::sleep(Duration::from_secs(6)).await;
    let miss = client
        .fetch("k", counting_fetcher(&calls), ReadPolicy::CacheOnly)
        .await;
    assert!(
        matches!(miss, Err(crate::FetchError::Miss)),
        "entry collected after gc_time"
    );
}

/// M4: refresh ticks at the min subscriber interval and stops once the last
/// subscriber leaves (OPT-3, RF-1, E15).
#[tokio::test(start_paused = true)]
async fn refresh_ticks_while_subscribed() {
    let client = client();
    let calls = Arc::new(AtomicU32::new(0));

    let mut handle = client.subscribe::<_, u32, String, _>(
        "k",
        counting_fetcher(&calls),
        QueryOptions {
            refresh_interval: Some(Duration::from_secs(5)),
            ..QueryOptions::default()
        },
    );
    wait_for(&mut handle, 1).await;

    tokio::time::sleep(Duration::from_secs(12)).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "initial load plus refreshes at t+5 and t+10"
    );

    drop(handle);
    tokio::time::sleep(Duration::from_secs(20)).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "refresh stops with no subscribers"
    );
}

/// M5: optimistic value is visible while the mutation runs and rolls back on
/// error (E10/E11, SEQ-4).
#[tokio::test(start_paused = true)]
async fn mutate_optimistic_rollback_end_to_end() {
    let client = client();
    let noop = |_key: &'static str| std::future::ready(Ok::<u32, String>(99));
    client.set::<_, u32, String>("k", 1);

    let (release, gate) = tokio::sync::oneshot::channel::<()>();
    let mutate_task = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .mutate::<_, u32, String, _>(
                    "k",
                    MutateOptions {
                        optimistic: Some(5),
                        rollback_on_error: true,
                        populate: true,
                        revalidate: false,
                    },
                    async move {
                        gate.await.expect("gate released");
                        Err("boom".to_string())
                    },
                )
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;

    let optimistic = client
        .fetch("k", noop, ReadPolicy::CacheOnly)
        .await
        .expect("optimistic value cached");
    assert_eq!(
        *optimistic, 5,
        "optimistic value visible during the mutation"
    );

    release.send(()).expect("mutation still waiting");
    let result = mutate_task.await.expect("mutate task not cancelled");
    assert_eq!(result.expect_err("mutation failed").as_str(), "boom");

    let rolled_back = client
        .fetch("k", noop, ReadPolicy::CacheOnly)
        .await
        .expect("rolled-back value cached");
    assert_eq!(*rolled_back, 1);
}

/// M5: a populate mutation writes its result; the caller gets the same Arc.
#[tokio::test(start_paused = true)]
async fn mutate_populate_writes_result() {
    let client = client();
    let noop = |_key: &'static str| std::future::ready(Ok::<u32, String>(99));

    let result = client
        .mutate::<_, u32, String, _>(
            "k",
            MutateOptions {
                revalidate: false,
                ..MutateOptions::default()
            },
            async { Ok(Some(10)) },
        )
        .await
        .expect("mutation succeeds")
        .expect("mutation produced a value");
    assert_eq!(*result, 10);

    let cached = client
        .fetch("k", noop, ReadPolicy::CacheOnly)
        .await
        .expect("populated");
    assert_eq!(*cached, 10);
}

/// M5: a dropped mutate future aborts the mutation instead of wedging the
/// entry (cancel safety; OPEN_QUESTIONS Q-2).
#[tokio::test(start_paused = true)]
async fn cancelled_mutation_releases_the_entry() {
    let client = client();
    let calls = Arc::new(AtomicU32::new(0));
    client.set::<_, u32, String>("k", 1);

    let mutate_task = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .mutate::<_, u32, String, _>(
                    "k",
                    MutateOptions {
                        optimistic: Some(5),
                        rollback_on_error: true,
                        populate: true,
                        revalidate: false,
                    },
                    std::future::pending::<Result<Option<u32>, String>>(),
                )
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    mutate_task.abort();
    let _ = mutate_task.await;

    let value = client
        .fetch("k", counting_fetcher(&calls), ReadPolicy::EnsureFresh)
        .await
        .expect("entry not wedged by the cancelled mutation");
    // The optimistic write rolled back and EnsureFresh revalidated normally.
    assert_eq!(*value, 1);
}

/// TE-1/TE-2 (M2): identical segments under different value types are distinct
/// entries; typed reads downcast safely on both.
#[tokio::test(start_paused = true)]
async fn same_segments_different_types_are_distinct_entries() {
    let client = client();
    client.set::<_, u32, String>("k", 7);
    client.set::<_, String, String>("k", "seven".to_string());

    let number = client
        .fetch(
            "k",
            |_key: &'static str| std::future::ready(Ok::<u32, String>(0)),
            ReadPolicy::CacheOnly,
        )
        .await
        .expect("u32 entry");
    assert_eq!(*number, 7);

    let text = client
        .fetch(
            "k",
            |_key: &'static str| std::future::ready(Ok::<String, String>(String::new())),
            ReadPolicy::CacheOnly,
        )
        .await
        .expect("string entry");
    assert_eq!(text.as_str(), "seven");
}

/// A CacheOnly read of an unknown key misses without creating an entry.
#[tokio::test(start_paused = true)]
async fn cache_only_miss() {
    let client = client();
    let miss = client
        .fetch(
            "unknown",
            |_key: &'static str| std::future::ready(Ok::<u32, String>(0)),
            ReadPolicy::CacheOnly,
        )
        .await;
    assert!(matches!(miss, Err(crate::FetchError::Miss)));
}

/// EnsureFresh waits out staleness: a stale entry is refetched before returning.
#[tokio::test(start_paused = true)]
async fn ensure_fresh_refetches_stale_data() {
    let client = client();
    let calls = Arc::new(AtomicU32::new(0));

    let first = client
        .fetch("k", counting_fetcher(&calls), ReadPolicy::EnsureFresh)
        .await
        .expect("first load");
    assert_eq!(*first, 1);

    tokio::time::sleep(Duration::from_secs(3)).await;

    let fresh = client
        .fetch("k", counting_fetcher(&calls), ReadPolicy::EnsureFresh)
        .await
        .expect("refetched");
    assert_eq!(*fresh, 2, "EnsureFresh never returns stale data");
}

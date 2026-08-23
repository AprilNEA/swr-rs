//! BFF-style example (spec M5): cache a slow upstream behind
//! stale-while-revalidate, then demonstrate subscriptions, optimistic
//! mutations, and prefix invalidation.
//!
//! Run with: `cargo run -p swr-runtime-tokio --example bff`

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use swr_core::{MutateOptions, QueryOptions, ReadPolicy, SwrClient};
use swr_runtime_tokio::TokioRuntime;

type UserKey = (&'static str, u64);

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let client = SwrClient::builder()
        .default_options(QueryOptions {
            stale_time: Duration::from_millis(200),
            ..QueryOptions::default()
        })
        .build(Arc::new(TokioRuntime::current()));

    // A slow upstream that counts how often it is actually called.
    let upstream_calls = Arc::new(AtomicU32::new(0));
    let fetcher = {
        let calls = Arc::clone(&upstream_calls);
        move |(_, id): UserKey| {
            let calls = Arc::clone(&calls);
            async move {
                let version = calls.fetch_add(1, Ordering::SeqCst) + 1;
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<String, String>(format!("user-{id} (upstream v{version})"))
            }
        }
    };
    let upstream = |c: &Arc<AtomicU32>| c.load(Ordering::SeqCst);

    // 1. First read hits the upstream.
    let user = client
        .fetch(
            ("user", 1u64),
            fetcher.clone(),
            ReadPolicy::StaleWhileRevalidate,
        )
        .await
        .unwrap();
    println!(
        "first read   -> {user} [{} upstream call(s)]",
        upstream(&upstream_calls)
    );

    // 2. An immediate second read is served from the cache.
    let user = client
        .fetch(
            ("user", 1u64),
            fetcher.clone(),
            ReadPolicy::StaleWhileRevalidate,
        )
        .await
        .unwrap();
    println!(
        "cached read  -> {user} [{} upstream call(s)]",
        upstream(&upstream_calls)
    );

    // 3. After stale_time, a read returns the old value immediately and
    //    refreshes in the background; a subscriber observes the new value.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let stale = client
        .fetch(
            ("user", 1u64),
            fetcher.clone(),
            ReadPolicy::StaleWhileRevalidate,
        )
        .await
        .unwrap();
    println!("stale read   -> {stale} (served instantly, refreshing behind the scenes)");

    let mut handle = client.subscribe::<_, String, String, _>(
        ("user", 1u64),
        fetcher.clone(),
        QueryOptions::default(),
    );
    while handle.snapshot().is_validating {
        handle.changed().await.unwrap();
    }
    println!("refreshed    -> {}", handle.snapshot().data.unwrap());

    // 4. Optimistic mutation: the optimistic value is visible instantly; the
    //    mutation result then populates the cache.
    let updated = client
        .mutate::<_, String, String, _>(
            ("user", 1u64),
            MutateOptions {
                optimistic: Some("user-1 (saving...)".to_string()),
                revalidate: false,
                ..MutateOptions::default()
            },
            async {
                tokio::time::sleep(Duration::from_millis(30)).await;
                Ok(Some("user-1 (renamed)".to_string()))
            },
        )
        .await
        .unwrap();
    println!("mutated      -> {}", updated.unwrap());

    // 5. Prefix invalidation marks every ("user", ...) entry stale; the active
    //    subscription refetches immediately.
    client.invalidate(("user",));
    while handle.snapshot().is_validating {
        handle.changed().await.unwrap();
    }
    println!(
        "invalidated  -> {} [{} upstream call(s) total]",
        handle.snapshot().data.unwrap(),
        upstream(&upstream_calls)
    );
}

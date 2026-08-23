//! Minimal quickstart for the `swr` facade crate.
//!
//! Run with: `cargo run -p swr --example quickstart`

use swr::ReadPolicy;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let client = swr::client();
    let fetch_user =
        |(_, id): (&str, u64)| async move { Ok::<String, String>(format!("user-{id}")) };

    let user = client
        .fetch(("user", 1u64), fetch_user, ReadPolicy::StaleWhileRevalidate)
        .await
        .unwrap();
    println!("fetched: {user}");

    // Local writes are authoritative (D-7) and served straight from the cache.
    client.set::<_, String, String>(("user", 1u64), "user-1 (renamed)".to_string());
    let user = client
        .fetch(("user", 1u64), fetch_user, ReadPolicy::CacheOnly)
        .await
        .unwrap();
    println!("cached:  {user}");
}

//! End-to-end tests against a minimal local HTTP server (raw TCP, canned
//! responses): the happy path through an swr client with caching, plus status
//! and decode error surfacing across the blocking bridge.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use swr::ReadPolicy;
use swr_core::Fetcher;
use swr_ureq::JsonFetcher;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Debug, PartialEq, serde::Deserialize)]
struct User {
    id: u64,
    name: String,
}

type Responder = Arc<dyn Fn(&str) -> (u16, String) + Send + Sync>;

/// Serve canned responses; `respond` sees the full request head and returns
/// `(status, json_body)`. Returns the base URL and a connection counter
/// (`connection: close` makes connections equal upstream requests).
async fn serve(respond: Responder) -> (String, Arc<AtomicU32>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let base = format!("http://{}", listener.local_addr().expect("addr"));
    let hits = Arc::new(AtomicU32::new(0));
    let counter = Arc::clone(&hits);
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            let respond = Arc::clone(&respond);
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut chunk = [0u8; 1024];
                while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                    match stream.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => head.extend_from_slice(&chunk[..n]),
                    }
                }
                let head = String::from_utf8_lossy(&head).into_owned();
                let (status, body) = respond(&head);
                let reason = if status == 200 { "OK" } else { "NO" };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    (base, hits)
}

#[tokio::test]
async fn get_json_end_to_end_with_caching() {
    let (base, hits) = serve(Arc::new(|head: &str| {
        assert!(
            head.starts_with("GET /users/1 "),
            "unexpected request: {head}"
        );
        (200, r#"{"id":1,"name":"ada"}"#.to_string())
    }))
    .await;

    let users: JsonFetcher<(&str, u64), User> =
        JsonFetcher::get(ureq::Agent::new_with_defaults(), move |(_, id)| {
            format!("{base}/users/{id}")
        });

    let client = swr::client();
    let user = client
        .fetch(
            ("user", 1u64),
            users.clone(),
            ReadPolicy::StaleWhileRevalidate,
        )
        .await
        .expect("fetch");
    assert_eq!(user.id, 1);
    assert_eq!(user.name, "ada");

    // A second read inside stale_time is served from the cache.
    let cached = client
        .fetch(("user", 1u64), users, ReadPolicy::StaleWhileRevalidate)
        .await
        .expect("cache hit");
    assert_eq!(*cached, *user);
    assert_eq!(hits.load(Ordering::SeqCst), 1, "one upstream request");
}

#[tokio::test]
async fn non_2xx_surfaces_as_a_status_error() {
    let (base, _) = serve(Arc::new(|_head: &str| {
        (404, r#"{"error":"nope"}"#.to_string())
    }))
    .await;

    let users: JsonFetcher<u64, User> =
        JsonFetcher::get(ureq::Agent::new_with_defaults(), move |id| {
            format!("{base}/users/{id}")
        });
    let err = users.fetch(1).await.expect_err("404 is an error");
    assert!(
        matches!(err, ureq::Error::StatusCode(404)),
        "expected StatusCode(404), got {err:?}"
    );
}

#[tokio::test]
async fn invalid_json_surfaces_as_a_decode_error() {
    let (base, _) = serve(Arc::new(|_head: &str| (200, "not json".to_string()))).await;

    let users: JsonFetcher<u64, User> =
        JsonFetcher::get(ureq::Agent::new_with_defaults(), move |id| {
            format!("{base}/users/{id}")
        });
    let err = users.fetch(1).await.expect_err("bad body is an error");
    assert!(
        matches!(err, ureq::Error::Json(_)),
        "expected Json decode error, got {err:?}"
    );
}

#[tokio::test]
async fn exchange_controls_method_and_headers() {
    let (base, _) = serve(Arc::new(|head: &str| {
        assert!(head.starts_with("GET /items?id=7 "), "unexpected: {head}");
        assert!(head.contains("x-swr: on"), "missing header: {head}");
        (200, r#"{"id":7,"name":"widget"}"#.to_string())
    }))
    .await;

    let items: JsonFetcher<u64, User> =
        JsonFetcher::new(ureq::Agent::new_with_defaults(), move |agent, id| {
            agent
                .get(format!("{base}/items?id={id}"))
                .header("x-swr", "on")
                .call()?
                .body_mut()
                .read_json()
        });
    let item = items.fetch(7).await.expect("fetch");
    assert_eq!(item.name, "widget");
}

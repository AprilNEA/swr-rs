//! [reqwest] fetchers for swr.
//!
//! A [`Fetcher`](swr_core::Fetcher) is just a closure, so reqwest works with
//! swr out of the box — this crate only removes the boilerplate: a cloneable
//! [`JsonFetcher`] maps a cache key to an HTTP request and decodes the JSON
//! response, with [`reqwest::Error`] as the query error type (non-2xx counts
//! as an error via [`error_for_status`](reqwest::Response::error_for_status)).
//!
//! ```no_run
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! use swr::ReadPolicy;
//! use swr_reqwest::JsonFetcher;
//!
//! #[derive(serde::Deserialize)]
//! struct User {
//!     name: String,
//! }
//!
//! let http = reqwest::Client::new();
//! let users: JsonFetcher<(&str, u64), User> =
//!     JsonFetcher::get(http, |(_, id)| format!("https://api.example.com/users/{id}"));
//!
//! let client = swr::client();
//! let user = client
//!     .fetch(("user", 1u64), users.clone(), ReadPolicy::StaleWhileRevalidate)
//!     .await
//!     .unwrap();
//! println!("{}", user.name);
//! # }
//! ```
#![deny(missing_docs)]

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use swr_core::Fetcher;

type BuildRequest<K> = dyn Fn(&reqwest::Client, K) -> reqwest::RequestBuilder + Send + Sync;

/// A cloneable fetcher: build an HTTP request from the cache key, send it via
/// a shared [`reqwest::Client`], and decode the JSON response into `T`.
///
/// Clones share the underlying client and request builder, so one instance
/// can serve every `fetch`/`subscribe` call for an endpoint.
pub struct JsonFetcher<K, T> {
    http: reqwest::Client,
    build: Arc<BuildRequest<K>>,
    _output: PhantomData<fn() -> T>,
}

impl<K, T> JsonFetcher<K, T> {
    /// `GET url(key)`, decoding the JSON response into `T`.
    pub fn get(http: reqwest::Client, url: impl Fn(K) -> String + Send + Sync + 'static) -> Self {
        Self::request(http, move |client, key| client.get(url(key)))
    }

    /// Full control: build any request from the key — method, query
    /// parameters, headers, body.
    ///
    /// The response is still decoded as JSON after
    /// [`error_for_status`](reqwest::Response::error_for_status).
    pub fn request(
        http: reqwest::Client,
        build: impl Fn(&reqwest::Client, K) -> reqwest::RequestBuilder + Send + Sync + 'static,
    ) -> Self {
        Self {
            http,
            build: Arc::new(build),
            _output: PhantomData,
        }
    }
}

// Manual impl: cloning must not require `K: Clone` or `T: Clone` (TE-2 spirit).
impl<K, T> Clone for JsonFetcher<K, T> {
    fn clone(&self) -> Self {
        Self {
            http: self.http.clone(),
            build: Arc::clone(&self.build),
            _output: PhantomData,
        }
    }
}

impl<K, T> Fetcher<K, T, reqwest::Error> for JsonFetcher<K, T>
where
    K: 'static,
    T: DeserializeOwned + Send + Sync + 'static,
{
    type Future = Pin<Box<dyn Future<Output = Result<T, reqwest::Error>> + Send + 'static>>;

    fn fetch(&self, key: K) -> Self::Future {
        let request = (self.build)(&self.http, key);
        Box::pin(async move {
            let response = request.send().await?.error_for_status()?;
            response.json::<T>().await
        })
    }
}

//! [ureq] fetchers for swr.
//!
//! ureq is a blocking HTTP client; this crate bridges it into swr's async
//! [`Fetcher`] model by running each exchange on a dedicated worker thread
//! and awaiting the result over a channel. The bridge only awaits that
//! channel, so it works under any [`Runtime`](swr_core::Runtime) — tokio is
//! not required.
//!
//! One OS thread runs per in-flight request. swr deduplicates concurrent
//! revalidation per key (one flight at a time), so the thread count tracks
//! the number of distinct keys loading at once, not the request rate. For
//! high-fanout servers prefer `swr-reqwest`.
//!
//! ```no_run
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! use swr::ReadPolicy;
//! use swr_ureq::JsonFetcher;
//!
//! #[derive(serde::Deserialize)]
//! struct User {
//!     name: String,
//! }
//!
//! let agent = ureq::Agent::new_with_defaults();
//! let users: JsonFetcher<(&str, u64), User> =
//!     JsonFetcher::get(agent, |(_, id)| format!("https://api.example.com/users/{id}"));
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
use std::pin::Pin;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use swr_core::Fetcher;

type Exchange<K, T> = dyn Fn(&ureq::Agent, K) -> Result<T, ureq::Error> + Send + Sync;

/// A cloneable fetcher running blocking [ureq] exchanges off the async
/// threads. Clones share the underlying [`ureq::Agent`] (and its connection
/// pool), so one instance can serve every `fetch`/`subscribe` call for an
/// endpoint.
///
/// With ureq's default configuration, non-2xx responses surface as
/// [`ureq::Error::StatusCode`] and JSON decode failures as
/// [`ureq::Error::Json`].
///
/// # Panics
///
/// A fetch panics if the worker thread cannot be spawned, or if the exchange
/// closure itself panics on the worker thread.
pub struct JsonFetcher<K, T> {
    agent: ureq::Agent,
    exchange: Arc<Exchange<K, T>>,
}

impl<K, T> JsonFetcher<K, T> {
    /// Full control: run any blocking ureq exchange for the key.
    pub fn new(
        agent: ureq::Agent,
        exchange: impl Fn(&ureq::Agent, K) -> Result<T, ureq::Error> + Send + Sync + 'static,
    ) -> Self {
        Self {
            agent,
            exchange: Arc::new(exchange),
        }
    }

    /// `GET url(key)`, decoding the JSON response into `T`.
    pub fn get(agent: ureq::Agent, url: impl Fn(K) -> String + Send + Sync + 'static) -> Self
    where
        T: DeserializeOwned,
    {
        Self::new(agent, move |agent, key| {
            agent.get(url(key)).call()?.body_mut().read_json::<T>()
        })
    }
}

// Manual impl: cloning must not require `K: Clone` or `T: Clone`.
impl<K, T> Clone for JsonFetcher<K, T> {
    fn clone(&self) -> Self {
        Self {
            agent: self.agent.clone(),
            exchange: Arc::clone(&self.exchange),
        }
    }
}

impl<K, T> Fetcher<K, T, ureq::Error> for JsonFetcher<K, T>
where
    K: Send + 'static,
    T: Send + 'static,
{
    type Future = Pin<Box<dyn Future<Output = Result<T, ureq::Error>> + Send + 'static>>;

    fn fetch(&self, key: K) -> Self::Future {
        let agent = self.agent.clone();
        let exchange = Arc::clone(&self.exchange);
        let (tx, rx) = tokio::sync::oneshot::channel();
        std::thread::Builder::new()
            .name("swr-ureq".to_owned())
            .spawn(move || {
                // A dropped waiter just closes the channel; the send result
                // is irrelevant (swr commits results through its own path).
                let _ = tx.send(exchange(&agent, key));
            })
            .expect("failed to spawn the swr-ureq worker thread");
        Box::pin(async move {
            rx.await
                .expect("swr-ureq worker thread panicked before replying")
        })
    }
}

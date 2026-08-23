//! Tokio implementation of the [`swr_core::Runtime`] trait (native targets).
#![deny(missing_docs)]

use swr_core::{Instant, Runtime, RuntimeFuture};

/// [`Runtime`] backed by a tokio runtime handle.
///
/// Holding a [`tokio::runtime::Handle`] (instead of calling
/// [`tokio::spawn`]) lets timers and unsubscribe events fire from any thread —
/// including `Drop` impls running outside the async context.
///
/// Time goes through [`tokio::time`], so `tokio::time::pause()` works in
/// tests.
#[derive(Clone, Debug)]
pub struct TokioRuntime {
    handle: tokio::runtime::Handle,
}

impl TokioRuntime {
    /// Capture the current tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics when called outside a tokio runtime context.
    pub fn current() -> Self {
        Self {
            handle: tokio::runtime::Handle::current(),
        }
    }
}

impl From<tokio::runtime::Handle> for TokioRuntime {
    fn from(handle: tokio::runtime::Handle) -> Self {
        Self { handle }
    }
}

impl Runtime for TokioRuntime {
    fn now(&self) -> Instant {
        tokio::time::Instant::now().into_std()
    }

    fn spawn(&self, fut: RuntimeFuture) {
        self.handle.spawn(fut);
    }

    fn sleep_until(&self, at: Instant) -> RuntimeFuture {
        // The Sleep is created inside the future so it binds to the runtime
        // it is polled on, not to the caller's context.
        Box::pin(async move {
            tokio::time::sleep_until(tokio::time::Instant::from_std(at)).await;
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use swr_core::{QueryOptions, ReadPolicy, SwrClient};

    use super::TokioRuntime;

    #[tokio::test(start_paused = true)]
    async fn fetch_subscribe_smoke() {
        let client = SwrClient::new(Arc::new(TokioRuntime::current()));
        let fetcher =
            |name: &'static str| async move { Ok::<String, String>(format!("hi {name}")) };

        let value = client
            .fetch("world", fetcher, ReadPolicy::StaleWhileRevalidate)
            .await
            .expect("first load");
        assert_eq!(value.as_str(), "hi world");

        let mut handle =
            client.subscribe::<_, String, String, _>("world", fetcher, QueryOptions::default());
        let state = handle.snapshot();
        assert_eq!(state.data.expect("cached").as_str(), "hi world");

        // Timers scheduled through the handle fire on the paused clock.
        tokio::time::sleep(Duration::from_millis(10)).await;
        handle.revalidate();
        handle.changed().await.expect("refresh notifies");
    }
}

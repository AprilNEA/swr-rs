//! Public error types.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

/// Error returned by [`SwrClient::fetch`](crate::SwrClient::fetch) (spec 7.1).
#[derive(Debug)]
pub enum FetchError<E> {
    /// The fetcher failed; carries the typed error as `Arc<E>` (TE-2).
    Fetch(Arc<E>),
    /// The entry has no fetcher to run. Non-`CacheOnly` reads require one.
    NoFetcher,
    /// A `CacheOnly` read found no cached data.
    Miss,
}

impl<E: fmt::Display> fmt::Display for FetchError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fetch(e) => write!(f, "fetcher failed: {e}"),
            Self::NoFetcher => f.write_str("no fetcher available for this key"),
            Self::Miss => f.write_str("cache-only read found no data"),
        }
    }
}

impl<E: Error + 'static> Error for FetchError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Fetch(e) => Some(&**e),
            _ => None,
        }
    }
}

/// The watched entry was garbage-collected and its channel closed; re-subscribe
/// to keep observing the key (spec 7.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Closed;

impl fmt::Display for Closed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("entry was garbage-collected; re-subscribe to continue")
    }
}

impl Error for Closed {}

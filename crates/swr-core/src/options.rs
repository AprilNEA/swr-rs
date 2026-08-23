//! Public option types: read policy, per-query options, mutation options, and
//! host environment events.

use std::time::Duration;

/// Read policy for [`SwrClient::fetch`](crate::SwrClient::fetch) (spec 7.1).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadPolicy {
    /// Return stale data immediately and refresh in the background; with no
    /// data, wait for the first result. The default.
    #[default]
    StaleWhileRevalidate,
    /// Wait for a complete result — or local write — no older than this call.
    EnsureFresh,
    /// Read the cache only; never start a request.
    CacheOnly,
}

/// Per-query options. Active subscribers' options are aggregated per entry
/// (OPT-1..OPT-4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryOptions {
    /// Freshness window (OPT-1). Absorbs SWR's `dedupingInterval` (D-4).
    /// Default: 2 seconds.
    pub stale_time: Duration,
    /// Delay before an idle entry is garbage-collected (OPT-2). Default: 300 seconds.
    pub gc_time: Duration,
    /// Background refresh interval while subscribed (OPT-3). Default: `None`.
    pub refresh_interval: Option<Duration>,
    /// Revalidate stale entries on [`SwrEvent::Focus`] broadcasts (OPT-4). Default: `true`.
    pub revalidate_on_focus: bool,
    /// Revalidate stale entries on [`SwrEvent::Online`] broadcasts (OPT-4). Default: `true`.
    pub revalidate_on_online: bool,
    /// Minimum spacing between focus-triggered revalidations (OPT-5; SWR's
    /// `focusThrottleInterval`). Online broadcasts are not throttled.
    /// Default: 5 seconds.
    pub focus_throttle: Duration,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            stale_time: Duration::from_secs(2),
            gc_time: Duration::from_secs(300),
            refresh_interval: None,
            revalidate_on_focus: true,
            revalidate_on_online: true,
            focus_throttle: Duration::from_secs(5),
        }
    }
}

impl QueryOptions {
    /// SWR's `useSWRImmutable`: never revalidate automatically. Data counts
    /// as fresh forever and focus/online broadcasts are ignored; manual
    /// `revalidate()` and `invalidate()` still work.
    pub fn immutable() -> Self {
        Self {
            stale_time: Duration::MAX,
            revalidate_on_focus: false,
            revalidate_on_online: false,
            ..Self::default()
        }
    }
}

/// Options for [`SwrClient::mutate`](crate::SwrClient::mutate) (spec 7.1).
#[derive(Debug)]
pub struct MutateOptions<T> {
    /// Optimistic value written before the mutation future runs (E10).
    pub optimistic: Option<T>,
    /// On error, roll the optimistic write back unless something else wrote in
    /// between (SEQ-4). Default: `true`.
    pub rollback_on_error: bool,
    /// Write an `Ok(Some(v))` mutation result into the cache. Default: `true`.
    pub populate: bool,
    /// Revalidate once the last concurrent mutation finishes (E11 step 3).
    /// Default: `true`.
    pub revalidate: bool,
}

impl<T> Default for MutateOptions<T> {
    fn default() -> Self {
        Self {
            optimistic: None,
            rollback_on_error: true,
            populate: true,
            revalidate: true,
        }
    }
}

/// Environment events fed by the host via
/// [`SwrClient::broadcast`](crate::SwrClient::broadcast) (E13).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SwrEvent {
    /// The window or application regained focus.
    Focus,
    /// Network connectivity was restored.
    Online,
}

/// The non-generic part of [`MutateOptions`], carried through mutation events.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MutateFlags {
    pub rollback_on_error: bool,
    pub populate: bool,
    pub revalidate: bool,
}

impl<T> From<&MutateOptions<T>> for MutateFlags {
    fn from(opts: &MutateOptions<T>) -> Self {
        Self {
            rollback_on_error: opts.rollback_on_error,
            populate: opts.populate,
            revalidate: opts.revalidate,
        }
    }
}

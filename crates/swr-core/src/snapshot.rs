//! Externally visible entry state (spec 4.4): the erased [`Snapshot`] carried
//! by the watch channel, and its typed view [`QueryState`].

use std::fmt;
use std::sync::Arc;

use crate::Instant;
use crate::erased::{ErasedValue, downcast_value};
use crate::marker::{MaybeSend, MaybeSync};

/// Type-erased, watch-channel-visible state of one entry (spec 4.4).
///
/// Reading a snapshot never touches the state machine lock (SNAP-1).
#[derive(Clone)]
pub struct Snapshot {
    /// Current data, if any.
    pub data: Option<ErasedValue>,
    /// Latest fetch error, if any. May coexist with `data` (SNAP-2, D-10).
    pub error: Option<ErasedValue>,
    /// Seq that produced `data`.
    pub data_seq: u64,
    /// Seq of the latest fetch `CommitErr`. Mutation errors never set this (WAIT-4).
    pub error_seq: u64,
    /// Seq of the in-flight request, if any.
    pub inflight: Option<u64>,
    /// Whether an async mutation is in progress.
    pub is_mutating: bool,
    /// Instant of the last authoritative write to `data` (commit or populate;
    /// optimistic writes do not move it, D-7).
    pub updated_at: Option<Instant>,
    /// Monotonic notification version. Guards the watch channel against a
    /// stale `Notify` overtaking a newer one across batches (see EFF-2 note in
    /// the client shell).
    pub(crate) version: u64,
}

impl Snapshot {
    pub(crate) fn empty() -> Self {
        Self {
            data: None,
            error: None,
            data_seq: 0,
            error_seq: 0,
            inflight: None,
            is_mutating: false,
            updated_at: None,
            version: 0,
        }
    }

    /// A request is in flight (first load or background refresh).
    pub fn is_validating(&self) -> bool {
        self.inflight.is_some()
    }

    /// First load: fetching with no data yet.
    pub fn is_loading(&self) -> bool {
        self.is_validating() && self.data.is_none()
    }
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot")
            .field("has_data", &self.data.is_some())
            .field("has_error", &self.error.is_some())
            .field("data_seq", &self.data_seq)
            .field("error_seq", &self.error_seq)
            .field("inflight", &self.inflight)
            .field("is_mutating", &self.is_mutating)
            .field("updated_at", &self.updated_at)
            .field("version", &self.version)
            .finish()
    }
}

/// Typed view of a [`Snapshot`] (spec 4.4).
pub struct QueryState<T, E> {
    /// Current data.
    pub data: Option<Arc<T>>,
    /// Latest fetch error. May coexist with `data` (SNAP-2).
    pub error: Option<Arc<E>>,
    /// First load: no data yet and a request in flight.
    pub is_loading: bool,
    /// Any request in flight (first load or background refresh).
    pub is_validating: bool,
    /// Instant of the last authoritative write to `data`.
    pub updated_at: Option<Instant>,
}

impl<T, E> QueryState<T, E>
where
    T: MaybeSend + MaybeSync + 'static,
    E: MaybeSend + MaybeSync + 'static,
{
    pub(crate) fn from_snapshot(snapshot: Snapshot) -> Self {
        Self {
            is_loading: snapshot.is_loading(),
            is_validating: snapshot.is_validating(),
            updated_at: snapshot.updated_at,
            data: snapshot.data.map(downcast_value::<T>),
            error: snapshot.error.map(downcast_value::<E>),
        }
    }
}

// Manual impl: `Arc` clones must not require `T: Clone` (TE-2).
impl<T, E> Clone for QueryState<T, E> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            error: self.error.clone(),
            is_loading: self.is_loading,
            is_validating: self.is_validating,
            updated_at: self.updated_at,
        }
    }
}

// Manual impl: presence-only output without requiring `T: Debug`.
impl<T, E> fmt::Debug for QueryState<T, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryState")
            .field("has_data", &self.data.is_some())
            .field("has_error", &self.error.is_some())
            .field("is_loading", &self.is_loading)
            .field("is_validating", &self.is_validating)
            .field("updated_at", &self.updated_at)
            .finish()
    }
}

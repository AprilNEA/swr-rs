//! UI-agnostic stale-while-revalidate cache core.
//!
//! All SWR semantics — deduplication, race resolution, staleness, optimistic
//! updates, GC — live in one pure synchronous state machine (D-1); a thin
//! async shell executes its effects. See `handoff.md` for the normative spec.
#![deny(missing_docs)]
// TODO(M3): remove once the async shell consumes the state machine.
#![allow(dead_code, reason = "M1: the async shell (M3) is not wired up yet")]

mod erased;
mod key;
mod machine;
mod marker;
mod options;
mod snapshot;

/// The one `Instant` used across the library (spec 3.2). On native targets it
/// is a zero-cost re-export of [`std::time::Instant`]; on `wasm32` it is
/// backed by the Performance API.
pub use web_time::Instant;

pub use erased::{BoxedFuture, ErasedValue};
pub use key::{IntoKeyPrefix, IntoQueryKey, IntoSegment, IntoSegments, QueryKey, Segment};
pub use marker::{MaybeSend, MaybeSync};
pub use options::{MutateOptions, QueryOptions, ReadPolicy, SwrEvent};
pub use snapshot::{QueryState, Snapshot};

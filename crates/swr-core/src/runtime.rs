//! The pluggable runtime abstraction (spec chapter 8).

use crate::Instant;
use crate::erased::BoxedFuture;
use crate::marker::{MaybeSend, MaybeSync};

/// Boxed unit future crossing the runtime boundary.
pub type RuntimeFuture = BoxedFuture<()>;

/// Clock, spawn, and timer abstraction. `swr-runtime-tokio` implements it for
/// native targets, `swr-runtime-web` for browsers.
///
/// RT-1: every instant the library reads comes from [`Runtime::now`] — never
/// `Instant::now()` directly — so tests can inject a mock clock.
pub trait Runtime: MaybeSend + MaybeSync + 'static {
    /// The current instant.
    fn now(&self) -> Instant;

    /// Spawn a detached task. Fetches and timers must keep running after the
    /// caller disappears (D-3).
    fn spawn(&self, fut: RuntimeFuture);

    /// A future resolving at `at`. Implementations may create it lazily; it is
    /// only awaited inside a task spawned via [`Runtime::spawn`].
    fn sleep_until(&self, at: Instant) -> RuntimeFuture;
}

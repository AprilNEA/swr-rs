//! `MaybeSend` / `MaybeSync`: alias `Send` / `Sync` on native targets, relaxed on
//! `wasm32` (spec 3.2). All types crossing a spawn boundary are bounded by these.

#[cfg(not(target_arch = "wasm32"))]
mod imp {
    /// Alias for [`Send`] on native targets; unconstrained on `wasm32`.
    pub trait MaybeSend: Send {}
    impl<T: Send + ?Sized> MaybeSend for T {}

    /// Alias for [`Sync`] on native targets; unconstrained on `wasm32`.
    pub trait MaybeSync: Sync {}
    impl<T: Sync + ?Sized> MaybeSync for T {}
}

#[cfg(target_arch = "wasm32")]
mod imp {
    /// Alias for [`Send`] on native targets; unconstrained on `wasm32`.
    pub trait MaybeSend {}
    impl<T: ?Sized> MaybeSend for T {}

    /// Alias for [`Sync`] on native targets; unconstrained on `wasm32`.
    pub trait MaybeSync {}
    impl<T: ?Sized> MaybeSync for T {}
}

pub use imp::{MaybeSend, MaybeSync};

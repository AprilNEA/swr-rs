//! Structured cache keys (spec 4.1).
//!
//! An entry's identity is the pair `(type_id, segments)` (K-1). Prefix
//! invalidation matches only the segments and ignores the type id (K-2), so
//! `invalidate(["user"])` hits entries of every value type under that prefix.

use std::any::TypeId;
use std::fmt;
use std::sync::Arc;

/// One structured segment of a [`QueryKey`].
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Segment {
    /// A string segment.
    Str(Arc<str>),
    /// An unsigned integer segment.
    U64(u64),
    /// A signed integer segment.
    I64(i64),
    /// A boolean segment.
    Bool(bool),
    /// An opaque byte segment.
    Bytes(Arc<[u8]>),
}

/// Structured cache key: value-type pair id plus ordered segments (spec 4.1).
///
/// K-1: identity is full equality of `(type_id, segments)`. The same segments
/// under different value types are different — and mutually safe — entries.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct QueryKey {
    /// `TypeId` of the value pair `(T, E)`; guarantees downcasts never fail (TE-1).
    type_id: TypeId,
    /// Structured segments; prefix matching operates on these (K-2).
    segments: Arc<[Segment]>,
}

impl QueryKey {
    /// Builds a key binding `segments` to the value types `(T, E)`.
    pub fn new<T: 'static, E: 'static>(segments: impl IntoSegments) -> Self {
        Self {
            type_id: TypeId::of::<(T, E)>(),
            segments: segments.into_segments().into(),
        }
    }

    /// The ordered segments of this key.
    pub fn segments(&self) -> &[Segment] {
        &self.segments
    }

    /// K-2: prefix match on segments only; the type id is ignored.
    pub(crate) fn matches_prefix(&self, prefix: &[Segment]) -> bool {
        self.segments.len() >= prefix.len() && self.segments[..prefix.len()] == *prefix
    }
}

impl fmt::Debug for QueryKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.segments.iter()).finish()
    }
}

/// Converts a value into one key [`Segment`].
pub trait IntoSegment {
    /// Performs the conversion.
    fn into_segment(self) -> Segment;
}

impl IntoSegment for Segment {
    fn into_segment(self) -> Segment {
        self
    }
}

impl IntoSegment for &str {
    fn into_segment(self) -> Segment {
        Segment::Str(Arc::from(self))
    }
}

impl IntoSegment for String {
    fn into_segment(self) -> Segment {
        Segment::Str(Arc::from(self))
    }
}

impl IntoSegment for Arc<str> {
    fn into_segment(self) -> Segment {
        Segment::Str(self)
    }
}

impl IntoSegment for u64 {
    fn into_segment(self) -> Segment {
        Segment::U64(self)
    }
}

impl IntoSegment for u32 {
    fn into_segment(self) -> Segment {
        Segment::U64(u64::from(self))
    }
}

impl IntoSegment for i64 {
    fn into_segment(self) -> Segment {
        Segment::I64(self)
    }
}

impl IntoSegment for i32 {
    fn into_segment(self) -> Segment {
        Segment::I64(i64::from(self))
    }
}

impl IntoSegment for bool {
    fn into_segment(self) -> Segment {
        Segment::Bool(self)
    }
}

impl IntoSegment for Vec<u8> {
    fn into_segment(self) -> Segment {
        Segment::Bytes(self.into())
    }
}

impl IntoSegment for &[u8] {
    fn into_segment(self) -> Segment {
        Segment::Bytes(Arc::from(self))
    }
}

/// Converts a value into a full ordered segment list — a key body or a prefix.
///
/// Implemented for `&str` / `String` (single segment), `Vec<Segment>`, and
/// tuples of up to eight [`IntoSegment`] elements.
pub trait IntoSegments {
    /// Performs the conversion.
    fn into_segments(self) -> Vec<Segment>;
}

impl IntoSegments for &str {
    fn into_segments(self) -> Vec<Segment> {
        vec![self.into_segment()]
    }
}

impl IntoSegments for String {
    fn into_segments(self) -> Vec<Segment> {
        vec![self.into_segment()]
    }
}

impl IntoSegments for Vec<Segment> {
    fn into_segments(self) -> Vec<Segment> {
        self
    }
}

macro_rules! impl_tuple_segments {
    ($($name:ident),+) => {
        impl<$($name: IntoSegment),+> IntoSegments for ($($name,)+) {
            fn into_segments(self) -> Vec<Segment> {
                #[allow(non_snake_case, reason = "tuple destructuring reuses the type parameter names")]
                let ($($name,)+) = self;
                vec![$($name.into_segment()),+]
            }
        }
    };
}

impl_tuple_segments!(A);
impl_tuple_segments!(A, B);
impl_tuple_segments!(A, B, C);
impl_tuple_segments!(A, B, C, D);
impl_tuple_segments!(A, B, C, D, E);
impl_tuple_segments!(A, B, C, D, E, F);
impl_tuple_segments!(A, B, C, D, E, F, G);
impl_tuple_segments!(A, B, C, D, E, F, G, H);

/// K-3: user-side key construction, binding segments to the value types `(T, E)`.
pub trait IntoQueryKey<T: 'static, E: 'static> {
    /// Performs the conversion.
    fn into_query_key(self) -> QueryKey;
}

impl<T: 'static, E: 'static, K: IntoSegments> IntoQueryKey<T, E> for K {
    fn into_query_key(self) -> QueryKey {
        QueryKey::new::<T, E>(self)
    }
}

/// A key prefix for [`SwrClient::invalidate`](crate::SwrClient::invalidate) (K-2).
pub trait IntoKeyPrefix {
    /// Performs the conversion.
    fn into_prefix(self) -> Vec<Segment>;
}

impl<K: IntoSegments> IntoKeyPrefix for K {
    fn into_prefix(self) -> Vec<Segment> {
        self.into_segments()
    }
}

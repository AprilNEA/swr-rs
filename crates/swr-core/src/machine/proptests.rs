//! M1 property tests (spec 9.1): random event sequences preserve the seq
//! invariants. Debug assertions inside `handle` (INV-A and friends) run as
//! part of every case.
#![allow(
    clippy::disallowed_methods,
    reason = "tests construct base instants directly; RT-1 applies to library code"
)]

use std::sync::Arc;
use std::time::Duration;

use proptest::prelude::*;

use super::*;
use crate::erased::BoxedFuture;

fn test_fetcher() -> ErasedFetcher {
    Arc::new(|_key: QueryKey| {
        Box::pin(async { Ok(Arc::new(0u32) as ErasedValue) })
            as BoxedFuture<Result<ErasedValue, ErasedValue>>
    })
}

fn key() -> QueryKey {
    QueryKey::new::<u32, String>("k")
}

#[derive(Debug, Clone)]
enum Op {
    Read(u8),
    Subscribe,
    Unsubscribe(u8),
    Revalidate,
    CommitPending {
        pick: u8,
        ok: bool,
    },
    CommitJunk {
        seq: u64,
        ok: bool,
    },
    MutateSet(u32),
    MutateBegin {
        optimistic: Option<u32>,
    },
    MutateCommit {
        pick: u8,
        ok: bool,
        some: bool,
        rollback: bool,
        populate: bool,
        revalidate: bool,
    },
    MutateAbort {
        pick: u8,
        rollback: bool,
    },
    Invalidate,
    Broadcast(bool),
    TimerGc(u64),
    TimerRefresh(u64),
    Advance(u16),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0u8..3).prop_map(Op::Read),
        Just(Op::Subscribe),
        any::<u8>().prop_map(Op::Unsubscribe),
        Just(Op::Revalidate),
        (any::<u8>(), any::<bool>()).prop_map(|(pick, ok)| Op::CommitPending { pick, ok }),
        (0u64..6, any::<bool>()).prop_map(|(seq, ok)| Op::CommitJunk { seq, ok }),
        any::<u32>().prop_map(Op::MutateSet),
        proptest::option::of(any::<u32>()).prop_map(|optimistic| Op::MutateBegin { optimistic }),
        (
            any::<u8>(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>()
        )
            .prop_map(|(pick, ok, some, rollback, populate, revalidate)| {
                Op::MutateCommit {
                    pick,
                    ok,
                    some,
                    rollback,
                    populate,
                    revalidate,
                }
            }),
        (any::<u8>(), any::<bool>())
            .prop_map(|(pick, rollback)| Op::MutateAbort { pick, rollback }),
        Just(Op::Invalidate),
        any::<bool>().prop_map(Op::Broadcast),
        (0u64..6).prop_map(Op::TimerGc),
        (0u64..6).prop_map(Op::TimerRefresh),
        (0u16..5000).prop_map(Op::Advance),
    ]
}

fn check_invariants(inner: &Inner) {
    for (k, e) in &inner.entries {
        assert!(e.data_seq <= e.seq, "data_seq <= seq for {k:?}");
        assert!(e.error_seq <= e.seq, "error_seq <= seq for {k:?}");
        if let Some(s) = e.inflight {
            assert_eq!(s, e.seq, "inflight seq equals entry seq for {k:?}");
            assert_eq!(
                e.mutation_active, 0,
                "no flight during a mutation for {k:?}"
            );
        }
        if let Some(snap) = &e.optimistic {
            assert!(
                snap.written_seq <= e.seq,
                "optimistic seq bounded for {k:?}"
            );
        }
    }
}

fn pick<T>(items: &mut Vec<T>, pick: u8) -> Option<T> {
    if items.is_empty() {
        None
    } else {
        Some(items.remove(usize::from(pick) % items.len()))
    }
}

proptest! {
    #[test]
    fn random_event_sequences_preserve_invariants(
        ops in proptest::collection::vec(op_strategy(), 1..100)
    ) {
        let mut inner = Inner::new(QueryOptions::default());
        let mut now = Instant::now();
        let k = key();
        let mut pending_fetches: Vec<(u64, u64)> = Vec::new();
        let mut subs: Vec<u64> = Vec::new();
        let mut tokens: Vec<MutationToken> = Vec::new();
        let flags = |rollback_on_error, populate, revalidate| MutateFlags {
            rollback_on_error,
            populate,
            revalidate,
        };

        for op in ops {
            let ev = match op {
                Op::Read(p) => Event::Read {
                    key: k.clone(),
                    policy: match p {
                        0 => ReadPolicy::StaleWhileRevalidate,
                        1 => ReadPolicy::EnsureFresh,
                        _ => ReadPolicy::CacheOnly,
                    },
                    fetcher: Some(test_fetcher()),
                    compare: None,
                    opts: QueryOptions::default(),
                },
                Op::Subscribe => Event::Subscribe {
                    key: k.clone(),
                    fetcher: Some(test_fetcher()),
                    compare: None,
                    opts: QueryOptions::default(),
                },
                Op::Unsubscribe(p) => match pick(&mut subs, p) {
                    Some(sub_id) => Event::Unsubscribe { key: k.clone(), sub_id },
                    None => continue,
                },
                Op::Revalidate => Event::RevalidateRequested { key: k.clone() },
                Op::CommitPending { pick: p, ok } => match pick(&mut pending_fetches, p) {
                    Some((incarnation, seq)) if ok => Event::CommitOk {
                        key: k.clone(),
                        incarnation,
                        seq,
                        value: Arc::new(0u32),
                    },
                    Some((incarnation, seq)) => Event::CommitErr {
                        key: k.clone(),
                        incarnation,
                        seq,
                        error: Arc::new("e".to_string()),
                    },
                    None => continue,
                },
                Op::CommitJunk { seq, ok } => {
                    // Junk commits must not alias a live flight: SEQ-2/SEQ-5
                    // are about stale identities, not forged current ones.
                    if pending_fetches.iter().any(|(_, s)| *s == seq) {
                        continue;
                    }
                    if ok {
                        Event::CommitOk {
                            key: k.clone(),
                            incarnation: u64::from(seq as u32 % 3),
                            seq,
                            value: Arc::new(0u32),
                        }
                    } else {
                        Event::CommitErr {
                            key: k.clone(),
                            incarnation: u64::from(seq as u32 % 3),
                            seq,
                            error: Arc::new("e".to_string()),
                        }
                    }
                }
                Op::MutateSet(v) => Event::MutateSet { key: k.clone(), value: Arc::new(v) },
                Op::MutateBegin { optimistic } => Event::MutateBegin {
                    key: k.clone(),
                    optimistic: optimistic.map(|v| Arc::new(v) as ErasedValue),
                },
                Op::MutateCommit { pick: p, ok, some, rollback, populate, revalidate } => {
                    match pick(&mut tokens, p) {
                        Some(token) => Event::MutateCommit {
                            token,
                            result: if ok {
                                Ok(some.then(|| Arc::new(1u32) as ErasedValue))
                            } else {
                                Err(Arc::new("e".to_string()) as ErasedValue)
                            },
                            flags: flags(rollback, populate, revalidate),
                        },
                        None => continue,
                    }
                }
                Op::MutateAbort { pick: p, rollback } => match pick(&mut tokens, p) {
                    Some(token) => Event::MutateAbort {
                        token,
                        flags: flags(rollback, true, true),
                    },
                    None => continue,
                },
                Op::Invalidate => Event::Invalidate {
                    prefix: vec![Segment::Str("k".into())],
                },
                Op::Broadcast(focus) => Event::Broadcast {
                    ev: if focus { SwrEvent::Focus } else { SwrEvent::Online },
                },
                Op::TimerGc(generation) => Event::TimerFired {
                    key: k.clone(),
                    kind: TimerKind::Gc,
                    generation,
                },
                Op::TimerRefresh(generation) => Event::TimerFired {
                    key: k.clone(),
                    kind: TimerKind::Refresh,
                    generation,
                },
                Op::Advance(ms) => {
                    now += Duration::from_millis(u64::from(ms));
                    continue;
                }
            };
            let out = inner.handle(ev, now);
            match out.outcome {
                Outcome::Subscribed { sub_id, .. } => subs.push(sub_id),
                Outcome::Mutation(token) => tokens.push(token),
                _ => {}
            }
            for effect in &out.effects {
                if let Effect::StartFetch {
                    incarnation, seq, ..
                } = effect
                {
                    pending_fetches.push((*incarnation, *seq));
                }
            }
            check_invariants(&inner);
        }
    }
}

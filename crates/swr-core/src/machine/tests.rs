//! M1 state-machine unit tests T1–T14 (spec 9.1). Pure synchronous: events
//! in, state and effects asserted, no async runtime.
#![allow(
    clippy::disallowed_methods,
    reason = "tests construct base instants directly; RT-1 applies to library code"
)]

use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::erased::BoxedFuture;

fn test_fetcher() -> ErasedFetcher {
    Arc::new(|_key: QueryKey| {
        Box::pin(async { Ok(Arc::new(0u32) as ErasedValue) })
            as BoxedFuture<Result<ErasedValue, ErasedValue>>
    })
}

fn val(n: u32) -> ErasedValue {
    Arc::new(n)
}

fn err(msg: &str) -> ErasedValue {
    Arc::new(msg.to_string())
}

fn as_u32(v: &ErasedValue) -> u32 {
    *v.downcast_ref::<u32>().expect("test value is u32")
}

fn key(name: &str) -> QueryKey {
    QueryKey::new::<u32, String>(name)
}

fn flags(rollback_on_error: bool, populate: bool, revalidate: bool) -> MutateFlags {
    MutateFlags {
        rollback_on_error,
        populate,
        revalidate,
    }
}

/// Effects helpers.
fn start_fetch_seqs(effects: &[Effect]) -> Vec<u64> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::StartFetch { seq, .. } => Some(*seq),
            _ => None,
        })
        .collect()
}

fn notify_count(effects: &[Effect]) -> usize {
    effects
        .iter()
        .filter(|e| matches!(e, Effect::Notify { .. }))
        .count()
}

fn timers(effects: &[Effect], want: TimerKind) -> Vec<(Instant, u64)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::ScheduleTimer {
                kind,
                at,
                generation,
                ..
            } if *kind == want => Some((*at, *generation)),
            _ => None,
        })
        .collect()
}

/// Test harness around `Inner` with a manual clock.
struct Machine {
    inner: Inner,
    now: Instant,
}

impl Machine {
    fn new() -> Self {
        Self {
            inner: Inner::new(QueryOptions::default()),
            now: Instant::now(),
        }
    }

    fn advance(&mut self, d: Duration) {
        self.now += d;
    }

    fn handle(&mut self, ev: Event) -> HandleOutput {
        self.inner.handle(ev, self.now)
    }

    fn entry(&self, key: &QueryKey) -> &EntryCore {
        self.inner.entries.get(key).expect("entry exists")
    }

    fn data_u32(&self, key: &QueryKey) -> Option<u32> {
        self.entry(key).data.as_ref().map(as_u32)
    }

    fn read(&mut self, key: &QueryKey, policy: ReadPolicy) -> HandleOutput {
        self.handle(Event::Read {
            key: key.clone(),
            policy,
            fetcher: Some(test_fetcher()),
            opts: QueryOptions::default(),
        })
    }

    fn subscribe(&mut self, key: &QueryKey, opts: QueryOptions) -> (u64, HandleOutput) {
        let out = self.handle(Event::Subscribe {
            key: key.clone(),
            fetcher: test_fetcher(),
            opts,
        });
        let Outcome::Subscribed { sub_id, .. } = out.outcome else {
            panic!("Subscribe yields Subscribed");
        };
        (
            sub_id,
            HandleOutput {
                outcome: Outcome::None,
                effects: out.effects,
            },
        )
    }

    fn unsubscribe(&mut self, key: &QueryKey, sub_id: u64) -> HandleOutput {
        self.handle(Event::Unsubscribe {
            key: key.clone(),
            sub_id,
        })
    }

    fn commit_ok(&mut self, key: &QueryKey, seq: u64, v: u32) -> HandleOutput {
        self.handle(Event::CommitOk {
            key: key.clone(),
            seq,
            value: val(v),
        })
    }

    fn commit_err(&mut self, key: &QueryKey, seq: u64, msg: &str) -> HandleOutput {
        self.handle(Event::CommitErr {
            key: key.clone(),
            seq,
            error: err(msg),
        })
    }

    fn mutate_set(&mut self, key: &QueryKey, v: u32) -> HandleOutput {
        self.handle(Event::MutateSet {
            key: key.clone(),
            value: val(v),
        })
    }

    fn mutate_begin(
        &mut self,
        key: &QueryKey,
        optimistic: Option<u32>,
    ) -> (MutationToken, HandleOutput) {
        let out = self.handle(Event::MutateBegin {
            key: key.clone(),
            optimistic: optimistic.map(val),
        });
        let Outcome::Mutation(token) = out.outcome else {
            panic!("MutateBegin yields Mutation");
        };
        (
            token,
            HandleOutput {
                outcome: Outcome::None,
                effects: out.effects,
            },
        )
    }
}

fn wait_target(out: &HandleOutput) -> u64 {
    match &out.outcome {
        Outcome::Read(ReadOutcome::Wait { target, .. }) => *target,
        other => panic!("expected Wait outcome, got {:?}", outcome_kind(other)),
    }
}

fn ready_snapshot(out: &HandleOutput) -> &Snapshot {
    match &out.outcome {
        Outcome::Read(ReadOutcome::Ready(snap)) => snap,
        other => panic!("expected Ready outcome, got {:?}", outcome_kind(other)),
    }
}

fn outcome_kind(outcome: &Outcome) -> &'static str {
    match outcome {
        Outcome::None => "None",
        Outcome::Read(ReadOutcome::Ready(_)) => "Read(Ready)",
        Outcome::Read(ReadOutcome::Wait { .. }) => "Read(Wait)",
        Outcome::Read(ReadOutcome::NoFetcher) => "Read(NoFetcher)",
        Outcome::Subscribed { .. } => "Subscribed",
        Outcome::Mutation(_) => "Mutation",
    }
}

/// T1: a local write interrupts the flight; the late commit is dropped
/// (SEQ-2 / SEQ-3).
#[test]
fn t1_mutate_interrupts_inflight() {
    let mut m = Machine::new();
    let k = key("a");
    let out = m.read(&k, ReadPolicy::StaleWhileRevalidate);
    assert_eq!(wait_target(&out), 1);
    assert_eq!(start_fetch_seqs(&out.effects), [1]);

    m.mutate_set(&k, 7);
    let out = m.commit_ok(&k, 1, 9);
    assert!(out.effects.is_empty(), "dropped commit has no effects");

    let e = m.entry(&k);
    assert_eq!(m.data_u32(&k), Some(7));
    assert_eq!(e.data_seq, 2);
    assert!(e.inflight.is_none());
}

/// T2: invalidate discards the flight and refetches; the old response is
/// dropped, the new one lands (SEQ-2, D-5).
#[test]
fn t2_out_of_order_after_invalidate() {
    let mut m = Machine::new();
    let k = key("a");
    let (_, out) = m.subscribe(&k, QueryOptions::default());
    assert_eq!(start_fetch_seqs(&out.effects), [1]);

    let out = m.handle(Event::Invalidate {
        prefix: vec![Segment::Str("a".into())],
    });
    // discard_flight consumed seq 2; the refetch is seq 3.
    assert_eq!(start_fetch_seqs(&out.effects), [3]);
    assert_eq!(notify_count(&out.effects), 1, "EFF-3 merges the notifies");

    let out = m.commit_ok(&k, 1, 10);
    assert!(out.effects.is_empty(), "stale response dropped");
    assert_eq!(m.data_u32(&k), None);

    m.commit_ok(&k, 3, 20);
    let e = m.entry(&k);
    assert_eq!(m.data_u32(&k), Some(20));
    assert!(!e.invalidated);
    assert!(e.inflight.is_none());
}

/// T3: a local write between the optimistic write and the failed commit wins;
/// no rollback (SEQ-4).
#[test]
fn t3_rollback_collision() {
    let mut m = Machine::new();
    let k = key("a");
    let (token, _) = m.mutate_begin(&k, Some(1));
    assert_eq!(token.written_seq, Some(2));

    m.mutate_set(&k, 5);
    let out = m.handle(Event::MutateCommit {
        token,
        result: Err(err("boom")),
        flags: flags(true, true, false),
    });
    assert_eq!(notify_count(&out.effects), 1);

    let e = m.entry(&k);
    assert_eq!(m.data_u32(&k), Some(5), "later write wins; no rollback");
    assert_eq!(e.data_seq, 3);
    assert!(e.optimistic.is_none());
    assert_eq!(e.mutation_active, 0);
}

/// T4: failed mutation rolls the optimistic write back to the snapshot
/// (data, error, data_seq, updated_at).
#[test]
fn t4_normal_rollback() {
    let mut m = Machine::new();
    let k = key("a");
    m.mutate_set(&k, 1);
    let t0 = m.entry(&k).updated_at;
    m.advance(Duration::from_secs(1));

    let (token, _) = m.mutate_begin(&k, Some(9));
    assert_eq!(m.data_u32(&k), Some(9), "optimistic value visible");
    assert_eq!(
        m.entry(&k).updated_at,
        t0,
        "optimistic write keeps updated_at (D-7)"
    );

    let out = m.handle(Event::MutateCommit {
        token,
        result: Err(err("boom")),
        flags: flags(true, true, false),
    });
    assert_eq!(notify_count(&out.effects), 1);

    let e = m.entry(&k);
    assert_eq!(m.data_u32(&k), Some(1), "rolled back");
    assert_eq!(e.data_seq, 1);
    assert!(e.error.is_none(), "prev_error restored");
    assert_eq!(e.updated_at, t0);
    assert_eq!(e.mutation_active, 0);
}

/// T5: an active mutation vetoes fetch commits; populate writes the result and
/// the closing revalidation starts a fresh fetch (D-6, E11 step 3).
#[test]
fn t5_mutation_blocks_commit_then_populates() {
    let mut m = Machine::new();
    let k = key("a");
    let out = m.read(&k, ReadPolicy::StaleWhileRevalidate);
    assert_eq!(start_fetch_seqs(&out.effects), [1]);

    let (token, _) = m.mutate_begin(&k, None);
    let out = m.commit_ok(&k, 1, 10);
    assert!(out.effects.is_empty(), "commit during mutation dropped");
    assert_eq!(m.data_u32(&k), None);

    let out = m.handle(Event::MutateCommit {
        token,
        result: Ok(Some(val(42))),
        flags: flags(true, true, true),
    });
    assert_eq!(m.data_u32(&k), Some(42));
    let fetches = start_fetch_seqs(&out.effects);
    assert_eq!(fetches.len(), 1, "closing revalidation starts one fetch");
    assert_eq!(m.entry(&k).inflight, Some(fetches[0]));
}

/// T6: CommitErr keeps old data and updated_at; the error slots in beside the
/// data (D-10), with a single merged notify (EFF-3).
#[test]
fn t6_commit_err_keeps_data() {
    let mut m = Machine::new();
    let k = key("a");
    m.read(&k, ReadPolicy::StaleWhileRevalidate);
    m.commit_ok(&k, 1, 10);
    let t0 = m.entry(&k).updated_at;

    m.advance(Duration::from_secs(3));
    let out = m.read(&k, ReadPolicy::StaleWhileRevalidate);
    assert_eq!(start_fetch_seqs(&out.effects), [2]);

    let out = m.commit_err(&k, 2, "boom");
    assert_eq!(notify_count(&out.effects), 1);
    assert!(start_fetch_seqs(&out.effects).is_empty());

    let e = m.entry(&k);
    assert_eq!(m.data_u32(&k), Some(10), "data untouched");
    assert_eq!(e.updated_at, t0, "updated_at untouched");
    assert!(e.error.is_some());
    assert_eq!(e.error_seq, 2);
    assert!(e.inflight.is_none());
}

/// T7: concurrent reads deduplicate onto one flight and share the target seq.
#[test]
fn t7_dedup_two_reads() {
    let mut m = Machine::new();
    let k = key("a");
    let out1 = m.read(&k, ReadPolicy::EnsureFresh);
    assert_eq!(start_fetch_seqs(&out1.effects), [1]);

    let out2 = m.read(&k, ReadPolicy::EnsureFresh);
    assert!(
        start_fetch_seqs(&out2.effects).is_empty(),
        "no second fetch"
    );
    assert_eq!(wait_target(&out1), wait_target(&out2));
}

/// T8: a new subscription bumps gc_gen, so the pending GC timer is ignored
/// when it fires (TMR-1).
#[test]
fn t8_gc_generation() {
    let mut m = Machine::new();
    let k = key("a");
    let out = m.mutate_set(&k, 1);
    assert_eq!(
        timers(&out.effects, TimerKind::Gc).len(),
        1,
        "idle entry schedules GC"
    );

    let (sub, _) = m.subscribe(&k, QueryOptions::default());
    let out = m.unsubscribe(&k, sub);
    let gc = timers(&out.effects, TimerKind::Gc);
    assert_eq!(gc.len(), 1);
    let (at, generation) = gc[0];
    assert_eq!(at, m.now + Duration::from_secs(300), "default gc_time");

    m.subscribe(&k, QueryOptions::default());
    let out = m.handle(Event::TimerFired {
        key: k.clone(),
        kind: TimerKind::Gc,
        generation,
    });
    assert!(out.effects.is_empty());
    assert!(
        m.inner.entries.contains_key(&k),
        "stale timer ignored; entry alive"
    );
}

/// T9: an in-flight request defers GC scheduling until its commit lands (GC-1).
#[test]
fn t9_gc_deferred_by_inflight() {
    let mut m = Machine::new();
    let k = key("a");
    let (sub, out) = m.subscribe(&k, QueryOptions::default());
    assert_eq!(start_fetch_seqs(&out.effects), [1]);

    let out = m.unsubscribe(&k, sub);
    assert!(
        timers(&out.effects, TimerKind::Gc).is_empty(),
        "inflight defers GC"
    );

    let out = m.commit_ok(&k, 1, 10);
    assert_eq!(
        timers(&out.effects, TimerKind::Gc).len(),
        1,
        "GC scheduled after commit"
    );
}

/// T10: refresh interval aggregates to the min; the generation fences stale
/// timers after all subscribers leave (OPT-3, RF-1, TMR-1).
#[test]
fn t10_refresh_lifecycle() {
    let mut m = Machine::new();
    let k = key("a");
    let five = QueryOptions {
        refresh_interval: Some(Duration::from_secs(5)),
        ..QueryOptions::default()
    };
    let ten = QueryOptions {
        refresh_interval: Some(Duration::from_secs(10)),
        ..QueryOptions::default()
    };

    let (sub_a, out) = m.subscribe(&k, five);
    let rf = timers(&out.effects, TimerKind::Refresh);
    assert_eq!(rf.len(), 1);
    assert_eq!(rf[0].0, m.now + Duration::from_secs(5));

    let (sub_b, out) = m.subscribe(&k, ten);
    let rf = timers(&out.effects, TimerKind::Refresh);
    assert_eq!(rf.len(), 1);
    assert_eq!(rf[0].0, m.now + Duration::from_secs(5), "min interval wins");

    let out = m.unsubscribe(&k, sub_a);
    let rf = timers(&out.effects, TimerKind::Refresh);
    assert_eq!(rf.len(), 1);
    assert_eq!(
        rf[0].0,
        m.now + Duration::from_secs(10),
        "remaining subscriber's interval"
    );
    let live_gen = rf[0].1;

    let out = m.unsubscribe(&k, sub_b);
    assert!(
        timers(&out.effects, TimerKind::Refresh).is_empty(),
        "no subscribers, no reschedule"
    );

    let out = m.handle(Event::TimerFired {
        key: k.clone(),
        kind: TimerKind::Refresh,
        generation: live_gen,
    });
    assert!(
        out.effects.is_empty(),
        "stale refresh timer fenced by generation"
    );
}

/// T11: prefix invalidation crosses value types (K-2); active entries refetch,
/// idle ones stay marked.
#[test]
fn t11_prefix_invalidation_across_types() {
    let mut m = Machine::new();
    let ka = QueryKey::new::<u32, String>(("user", 1u64));
    let kb = QueryKey::new::<String, String>(("user", 2u64));

    let (_, out) = m.subscribe(&ka, QueryOptions::default());
    assert_eq!(start_fetch_seqs(&out.effects), [1]);
    m.handle(Event::MutateSet {
        key: kb.clone(),
        value: Arc::new("hello".to_string()),
    });

    let out = m.handle(Event::Invalidate {
        prefix: vec![Segment::Str("user".into())],
    });
    assert_eq!(
        start_fetch_seqs(&out.effects).len(),
        1,
        "only the active entry refetches"
    );

    let ea = m.entry(&ka);
    assert!(ea.inflight.is_some(), "active entry refetching");
    assert!(!ea.invalidated, "refetch converges the invalidation");
    let eb = m.entry(&kb);
    assert!(eb.invalidated, "idle entry stays marked");
    assert!(eb.inflight.is_none());
}

/// T12: broadcasts only disturb active, stale, idle entries (E13).
#[test]
fn t12_broadcast_targets() {
    let mut m = Machine::new();
    let ka = key("fresh");
    let kb = key("unsubscribed");
    let kc = key("mutating");
    let kd = key("stale");

    let long_stale = QueryOptions {
        stale_time: Duration::from_secs(100),
        ..QueryOptions::default()
    };
    m.subscribe(&ka, long_stale);
    m.commit_ok(&ka, 1, 1);

    m.read(&kb, ReadPolicy::StaleWhileRevalidate);
    m.commit_ok(&kb, 1, 2);

    m.subscribe(&kc, QueryOptions::default());
    m.commit_ok(&kc, 1, 3);
    m.mutate_begin(&kc, None);

    m.subscribe(&kd, QueryOptions::default());
    m.commit_ok(&kd, 1, 4);

    m.advance(Duration::from_secs(3));
    let out = m.handle(Event::Broadcast {
        ev: SwrEvent::Focus,
    });
    assert_eq!(
        start_fetch_seqs(&out.effects).len(),
        1,
        "only the stale active entry"
    );
    assert!(m.entry(&ka).inflight.is_none(), "fresh entry untouched");
    assert!(
        m.entry(&kb).inflight.is_none(),
        "unsubscribed entry untouched"
    );
    assert!(m.entry(&kc).inflight.is_none(), "mutating entry untouched");
    assert!(
        m.entry(&kd).inflight.is_some(),
        "stale active entry refetches"
    );
}

/// T13: StartFetch precedes Notify (EFF-2) and same-key notifies merge (EFF-3).
#[test]
fn t13_effect_order_and_merge() {
    let mut m = Machine::new();
    let k = key("a");
    m.mutate_set(&k, 1);
    m.read(&k, ReadPolicy::StaleWhileRevalidate); // store a fetcher

    let (token, _) = m.mutate_begin(&k, Some(2));
    // Err + rollback + revalidate: error write, rollback, is_mutating flip and
    // fetch start all in one batch.
    let out = m.handle(Event::MutateCommit {
        token,
        result: Err(err("boom")),
        flags: flags(true, true, true),
    });
    assert_eq!(notify_count(&out.effects), 1, "EFF-3: one merged notify");
    let fetch_idx = out
        .effects
        .iter()
        .position(|e| matches!(e, Effect::StartFetch { .. }))
        .expect("revalidation fetch");
    let notify_idx = out
        .effects
        .iter()
        .position(|e| matches!(e, Effect::Notify { .. }))
        .expect("notify");
    assert!(fetch_idx < notify_idx, "EFF-2: StartFetch before Notify");
}

/// T14: a stale SWR read returns the old value immediately plus exactly one
/// background fetch (E2-2).
#[test]
fn t14_stale_read_returns_old_value() {
    let mut m = Machine::new();
    let k = key("a");
    m.read(&k, ReadPolicy::StaleWhileRevalidate);
    m.commit_ok(&k, 1, 10);

    m.advance(Duration::from_secs(3));
    let out = m.read(&k, ReadPolicy::StaleWhileRevalidate);
    let snap = ready_snapshot(&out);
    assert_eq!(as_u32(snap.data.as_ref().expect("stale data returned")), 10);
    assert_eq!(
        snap.inflight,
        Some(2),
        "snapshot reflects the started refresh"
    );
    assert_eq!(start_fetch_seqs(&out.effects), [2]);
    assert_eq!(notify_count(&out.effects), 1);

    // A second stale read while the refresh flies does not duplicate it (E2-3).
    let out = m.read(&k, ReadPolicy::StaleWhileRevalidate);
    assert!(start_fetch_seqs(&out.effects).is_empty());
    assert_eq!(
        as_u32(ready_snapshot(&out).data.as_ref().expect("data")),
        10
    );
}

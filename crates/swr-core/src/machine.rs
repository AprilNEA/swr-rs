//! The sans-io state machine `Inner` (spec chapter 5, rules in chapter 6).
//!
//! `Inner` is pure and synchronous: events in, state changes plus [`Effect`]s
//! out. It never awaits, spawns, runs callbacks, or sends on the watch channel
//! — the async shell in [`crate::client`] executes the effects outside the
//! lock (LOCK-1..LOCK-3). Guards are annotated `// E7-3`-style, linking back to
//! the transition tables in `handoff.md`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::Instant;
use crate::erased::{ErasedCompare, ErasedFetcher, ErasedValue};
use crate::key::{QueryKey, Segment};
use crate::options::{MutateFlags, QueryOptions, ReadPolicy, SwrEvent};
use crate::snapshot::Snapshot;

#[cfg(test)]
mod proptests;
#[cfg(test)]
mod tests;

/// Timer classes scheduled by the machine (5.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TimerKind {
    /// Entry removal countdown (GC-1 / E14).
    Gc,
    /// Background refresh tick (RF-1 / E15).
    Refresh,
}

/// Ticket returned by `MutateBegin`, consumed by `MutateCommit`/`MutateAbort`.
#[derive(Clone, Debug)]
pub(crate) struct MutationToken {
    pub key: QueryKey,
    /// Seq consumed by this mutation's optimistic write, if it made one.
    /// Rollback compares `data_seq` against it (SEQ-4).
    pub written_seq: Option<u64>,
}

/// Input events (5.2).
pub(crate) enum Event {
    /// The read path of `fetch()`. A provided fetcher replaces the stored one
    /// (last-wins, API-2).
    Read {
        key: QueryKey,
        policy: ReadPolicy,
        fetcher: Option<ErasedFetcher>,
        /// Structural-sharing comparator (D-30); a provided one replaces the
        /// stored one, `None` leaves it untouched.
        compare: Option<ErasedCompare>,
        opts: QueryOptions,
    },
    /// A `QueryHandle` is being created.
    Subscribe {
        key: QueryKey,
        fetcher: ErasedFetcher,
        /// Structural-sharing comparator (D-30); a provided one replaces the
        /// stored one, `None` leaves it untouched.
        compare: Option<ErasedCompare>,
        opts: QueryOptions,
    },
    /// A `QueryHandle` was dropped. `sub_id` identifies which subscriber's
    /// options leave the aggregate (OPT-*).
    Unsubscribe { key: QueryKey, sub_id: u64 },
    /// Manual revalidation (`handle.revalidate()` / `client.revalidate(key)`).
    RevalidateRequested { key: QueryKey },
    /// A fetch task finished successfully.
    CommitOk {
        key: QueryKey,
        /// Entry incarnation the flight belongs to (SEQ-5, D-31).
        incarnation: u64,
        seq: u64,
        value: ErasedValue,
    },
    /// A fetch task failed.
    CommitErr {
        key: QueryKey,
        /// Entry incarnation the flight belongs to (SEQ-5, D-31).
        incarnation: u64,
        seq: u64,
        error: ErasedValue,
    },
    /// Synchronous local write (`client.set`).
    MutateSet { key: QueryKey, value: ErasedValue },
    /// An async mutation begins; yields a [`MutationToken`].
    MutateBegin {
        key: QueryKey,
        optimistic: Option<ErasedValue>,
    },
    /// An async mutation finished.
    MutateCommit {
        token: MutationToken,
        result: Result<Option<ErasedValue>, ErasedValue>,
        flags: MutateFlags,
    },
    /// The `mutate()` future was dropped before completion. Releases
    /// `mutation_active` and rolls back like the error path, without writing
    /// an error (cancel safety; see OPEN_QUESTIONS Q-2).
    MutateAbort {
        token: MutationToken,
        flags: MutateFlags,
    },
    /// Prefix invalidation (K-2).
    Invalidate { prefix: Vec<Segment> },
    /// Environment event broadcast (focus / online).
    Broadcast { ev: SwrEvent },
    /// A scheduled timer fired. Stale generations are ignored (TMR-1).
    TimerFired {
        key: QueryKey,
        kind: TimerKind,
        generation: u64,
    },
}

/// Output side effects (5.3). Executed strictly in order (EFF-1), outside the
/// lock (EFF-4).
pub(crate) enum Effect {
    /// Spawn a detached task running the stored fetcher; its result comes back
    /// as `CommitOk`/`CommitErr` (D-3).
    StartFetch {
        key: QueryKey,
        /// Entry incarnation, echoed back by the commit events (SEQ-5).
        incarnation: u64,
        seq: u64,
        fetcher: ErasedFetcher,
    },
    /// Send a fresh snapshot on the entry's watch channel, outside the lock
    /// (LOCK-3). At most one per key per batch (EFF-3), after any
    /// `StartFetch` (EFF-2).
    Notify {
        tx: Arc<watch::Sender<Snapshot>>,
        snapshot: Snapshot,
    },
    /// Spawn: sleep until `at`, then feed `TimerFired { key, kind, generation }` back.
    ScheduleTimer {
        key: QueryKey,
        kind: TimerKind,
        at: Instant,
        generation: u64,
    },
}

impl std::fmt::Debug for Effect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StartFetch { key, seq, .. } => f
                .debug_struct("StartFetch")
                .field("key", key)
                .field("seq", seq)
                .finish_non_exhaustive(),
            Self::Notify { snapshot, .. } => f
                .debug_struct("Notify")
                .field("snapshot", snapshot)
                .finish_non_exhaustive(),
            Self::ScheduleTimer {
                key,
                kind,
                at,
                generation,
            } => f
                .debug_struct("ScheduleTimer")
                .field("key", key)
                .field("kind", kind)
                .field("at", at)
                .field("generation", generation)
                .finish(),
        }
    }
}

/// What a `Read` returns to the caller (transition tables E1–E3).
pub(crate) enum ReadOutcome {
    /// A snapshot the caller can use immediately.
    Ready(Snapshot),
    /// Enter the wait loop (5.6) until `data_seq >= target` or
    /// `error_seq >= target`.
    Wait {
        target: u64,
        rx: watch::Receiver<Snapshot>,
    },
    /// E2-7 / E3-5: nothing cached and no fetcher to run.
    NoFetcher,
}

/// Per-event return value for the caller (the tables' "返回" column).
pub(crate) enum Outcome {
    /// Nothing to return.
    None,
    /// Result of a `Read`.
    Read(ReadOutcome),
    /// Result of a `Subscribe`.
    Subscribed {
        sub_id: u64,
        rx: watch::Receiver<Snapshot>,
    },
    /// Result of a `MutateBegin`.
    Mutation(MutationToken),
}

/// Result of one [`Inner::handle`] call.
pub(crate) struct HandleOutput {
    pub outcome: Outcome,
    pub effects: Vec<Effect>,
}

/// Rollback snapshot taken by an optimistic write (4.2).
struct OptimisticSnapshot {
    prev_data: Option<ErasedValue>,
    prev_error: Option<ErasedValue>,
    prev_data_seq: u64,
    prev_updated_at: Option<Instant>,
    /// Seq the optimistic value was written at; rollback requires
    /// `data_seq == written_seq` (SEQ-4).
    written_seq: u64,
}

/// Option aggregation across active subscribers plus the latest read (OPT-*).
#[derive(Default)]
struct EntryOptions {
    subs: HashMap<u64, QueryOptions>,
    /// `gc_time` from the most recent `Read`/`Subscribe` touch. OPT-2 speaks
    /// of "the latest Read"; subscriptions are included so a sole subscriber's
    /// `gc_time` still applies after it unsubscribes.
    last_touch_gc_time: Option<Duration>,
}

impl EntryOptions {
    /// OPT-2: max of active subscribers and the latest touch.
    fn gc_time(&self, default_gc: Duration) -> Duration {
        self.subs
            .values()
            .map(|o| o.gc_time)
            .chain(self.last_touch_gc_time)
            .max()
            .unwrap_or(default_gc)
    }

    /// OPT-3: min non-`None` interval among active subscribers.
    fn refresh_interval(&self) -> Option<Duration> {
        self.subs.values().filter_map(|o| o.refresh_interval).min()
    }

    /// OPT-1 (broadcast/refresh side): min `stale_time` among active subscribers.
    fn min_stale_time(&self, default_stale: Duration) -> Duration {
        self.subs
            .values()
            .map(|o| o.stale_time)
            .min()
            .unwrap_or(default_stale)
    }

    /// OPT-4: enabled if any active subscriber enables it.
    fn any_on_focus(&self) -> bool {
        self.subs.values().any(|o| o.revalidate_on_focus)
    }

    /// OPT-4: enabled if any active subscriber enables it.
    fn any_on_online(&self) -> bool {
        self.subs.values().any(|o| o.revalidate_on_online)
    }

    /// OPT-5: min focus throttle among active subscribers (the most eager
    /// subscriber wins, like OPT-1/OPT-3).
    fn min_focus_throttle(&self, default_throttle: Duration) -> Duration {
        self.subs
            .values()
            .map(|o| o.focus_throttle)
            .min()
            .unwrap_or(default_throttle)
    }
}

/// One cache entry (spec 4.2).
struct EntryCore {
    /// Distinguishes this entry from earlier incarnations under the same key
    /// (SEQ-5, D-31): per-entry seqs restart at 0 after GC removal, so a
    /// discarded old-incarnation flight could otherwise alias a new flight's
    /// seq and commit into the wrong incarnation.
    incarnation: u64,

    // ---- values ----
    data: Option<ErasedValue>,
    error: Option<ErasedValue>,
    data_seq: u64,
    error_seq: u64,
    updated_at: Option<Instant>,

    // ---- race control ----
    seq: u64,
    inflight: Option<u64>,
    mutation_active: usize,
    invalidated: bool,
    optimistic: Option<OptimisticSnapshot>,

    // ---- lifecycle ----
    subscribers: usize,
    gc_gen: u64,
    refresh_gen: u64,
    /// Focus-triggered revalidation is suppressed until this instant (OPT-5,
    /// SWR's `focusThrottleInterval`). Re-armed on each accepted focus event.
    focus_blocked_until: Option<Instant>,

    // ---- behavior ----
    fetcher: Option<ErasedFetcher>,
    /// Structural-sharing comparator (D-30, CMP-1): decides only whether the
    /// stored `Arc` is kept on an equal commit; never affects seq or notify.
    compare: Option<ErasedCompare>,
    opts: EntryOptions,

    // ---- notification ----
    /// Monotonic snapshot version; lets the shell drop watch sends that would
    /// run behind a newer snapshot published by a racing commit.
    snap_version: u64,
    tx: Arc<watch::Sender<Snapshot>>,
}

impl EntryCore {
    fn new(incarnation: u64) -> Self {
        let (tx, _rx) = watch::channel(Snapshot::empty());
        Self {
            incarnation,
            data: None,
            error: None,
            data_seq: 0,
            error_seq: 0,
            updated_at: None,
            seq: 0,
            inflight: None,
            mutation_active: 0,
            invalidated: false,
            optimistic: None,
            subscribers: 0,
            gc_gen: 0,
            refresh_gen: 0,
            focus_blocked_until: None,
            fetcher: None,
            compare: None,
            opts: EntryOptions::default(),
            snap_version: 0,
            tx: Arc::new(tx),
        }
    }

    /// `start_fetch!` precondition (5.4).
    fn can_start_fetch(&self) -> bool {
        self.fetcher.is_some() && self.mutation_active == 0 && self.inflight.is_none()
    }

    /// `is_stale` (5.4). An unrepresentable deadline (`updated_at +
    /// stale_time` overflows, e.g. `QueryOptions::immutable()`) means the
    /// entry never goes stale by time.
    fn is_stale(&self, stale_time: Duration, now: Instant) -> bool {
        self.invalidated
            || self.updated_at.is_none_or(|t| {
                t.checked_add(stale_time)
                    .is_some_and(|deadline| now >= deadline)
            })
    }

    /// `active` (5.4).
    fn is_active(&self) -> bool {
        self.subscribers > 0
    }

    /// `discard_flight!` (5.4): a future commit of the old flight now fails SEQ-2.
    fn discard_flight(&mut self) {
        self.seq += 1;
        self.inflight = None;
    }

    /// `local_write!` (5.4).
    fn local_write(&mut self, value: ErasedValue, now: Instant) {
        self.seq += 1;
        self.data = Some(value);
        self.data_seq = self.seq;
        self.updated_at = Some(now);
        self.error = None;
        self.invalidated = false;
        self.inflight = None;
    }

    /// Entry is idle: eligible for the GC countdown (GC-1).
    fn is_quiescent(&self) -> bool {
        self.subscribers == 0 && self.mutation_active == 0 && self.inflight.is_none()
    }

    fn build_snapshot(&self, version: u64) -> Snapshot {
        Snapshot {
            data: self.data.clone(),
            error: self.error.clone(),
            data_seq: self.data_seq,
            error_seq: self.error_seq,
            inflight: self.inflight,
            is_mutating: self.mutation_active > 0,
            updated_at: self.updated_at,
            version,
        }
    }

    /// Snapshot of the current state without consuming a notify version.
    fn snapshot_now(&self) -> Snapshot {
        self.build_snapshot(self.snap_version)
    }
}

/// `start_fetch!` (5.4): consume a seq, mark it in flight, emit `StartFetch`.
///
/// Also clears `invalidated`: the started fetch is the one converging the
/// invalidation, and its own commit must satisfy INV-A (OPEN_QUESTIONS Q-1;
/// same move E11 step 3 makes explicitly).
fn start_fetch(e: &mut EntryCore, key: &QueryKey, ctx: &mut Ctx) -> u64 {
    debug_assert!(e.can_start_fetch(), "start_fetch! precondition violated");
    e.seq += 1;
    e.inflight = Some(e.seq);
    e.invalidated = false;
    let fetcher = e
        .fetcher
        .clone()
        .expect("start_fetch! precondition: fetcher present");
    ctx.effects.push(Effect::StartFetch {
        key: key.clone(),
        incarnation: e.incarnation,
        seq: e.seq,
        fetcher,
    });
    e.seq
}

/// Per-`handle()` working set: effects plus ordered, deduplicated key marks.
#[derive(Default)]
struct Ctx {
    effects: Vec<Effect>,
    /// Keys needing a `Notify`; flushed as one merged send per key (EFF-3),
    /// after all `StartFetch` effects (EFF-2).
    notify: Vec<QueryKey>,
    /// Keys the event touched; GC-1 runs over these.
    touched: Vec<QueryKey>,
    /// Keys whose refresh scheduling basis changed; RF-1 runs over these.
    refresh_changed: Vec<QueryKey>,
}

impl Ctx {
    fn mark_notify(&mut self, key: &QueryKey) {
        if !self.notify.contains(key) {
            self.notify.push(key.clone());
        }
    }

    fn mark_touched(&mut self, key: &QueryKey) {
        if !self.touched.contains(key) {
            self.touched.push(key.clone());
        }
    }

    fn mark_refresh_changed(&mut self, key: &QueryKey) {
        if !self.refresh_changed.contains(key) {
            self.refresh_changed.push(key.clone());
        }
    }
}

/// The state machine (5.1). One instance behind the client's single lock.
pub(crate) struct Inner {
    entries: HashMap<QueryKey, EntryCore>,
    defaults: QueryOptions,
    next_sub_id: u64,
    /// Incarnation counter for newly created entries (SEQ-5).
    next_incarnation: u64,
}

impl Inner {
    pub(crate) fn new(defaults: QueryOptions) -> Self {
        Self {
            entries: HashMap::new(),
            defaults,
            next_sub_id: 0,
            next_incarnation: 0,
        }
    }

    /// Sole entry point (5.1): feed one event, get the outcome plus effects.
    /// Pure and synchronous; the caller executes effects in order (EFF-1)
    /// outside the lock (EFF-4).
    pub(crate) fn handle(&mut self, ev: Event, now: Instant) -> HandleOutput {
        let mut ctx = Ctx::default();
        let outcome = match ev {
            Event::Read {
                key,
                policy,
                fetcher,
                compare,
                opts,
            } => self.on_read(key, policy, fetcher, compare, opts, now, &mut ctx),
            Event::Subscribe {
                key,
                fetcher,
                compare,
                opts,
            } => self.on_subscribe(key, fetcher, compare, opts, now, &mut ctx),
            Event::Unsubscribe { key, sub_id } => {
                self.on_unsubscribe(&key, sub_id, &mut ctx);
                Outcome::None
            }
            Event::RevalidateRequested { key } => {
                self.on_revalidate_requested(&key, &mut ctx);
                Outcome::None
            }
            Event::CommitOk {
                key,
                incarnation,
                seq,
                value,
            } => {
                self.on_commit_ok(&key, incarnation, seq, value, now, &mut ctx);
                Outcome::None
            }
            Event::CommitErr {
                key,
                incarnation,
                seq,
                error,
            } => {
                self.on_commit_err(&key, incarnation, seq, error, &mut ctx);
                Outcome::None
            }
            Event::MutateSet { key, value } => {
                self.on_mutate_set(key, value, now, &mut ctx);
                Outcome::None
            }
            Event::MutateBegin { key, optimistic } => {
                self.on_mutate_begin(key, optimistic, &mut ctx)
            }
            Event::MutateCommit {
                token,
                result,
                flags,
            } => {
                self.on_mutate_commit(token, result, flags, now, &mut ctx);
                Outcome::None
            }
            Event::MutateAbort { token, flags } => {
                self.on_mutate_abort(token, flags, &mut ctx);
                Outcome::None
            }
            Event::Invalidate { prefix } => {
                self.on_invalidate(&prefix, &mut ctx);
                Outcome::None
            }
            Event::Broadcast { ev } => {
                self.on_broadcast(ev, now, &mut ctx);
                Outcome::None
            }
            Event::TimerFired {
                key,
                kind,
                generation,
            } => {
                match kind {
                    TimerKind::Gc => self.on_timer_gc(&key, generation),
                    TimerKind::Refresh => self.on_timer_refresh(&key, generation, &mut ctx),
                }
                Outcome::None
            }
        };
        self.post_rules(now, &mut ctx);
        self.flush_notifies(&mut ctx);
        HandleOutput {
            outcome,
            effects: ctx.effects,
        }
    }

    /// Fetch-or-create with a fresh incarnation on insert (SEQ-5).
    fn entry_or_create(&mut self, key: &QueryKey) -> &mut EntryCore {
        if !self.entries.contains_key(key) {
            let incarnation = self.next_incarnation;
            self.next_incarnation += 1;
            self.entries
                .insert(key.clone(), EntryCore::new(incarnation));
        }
        self.entries.get_mut(key).expect("just ensured above")
    }

    /// E1 / E2 / E3.
    #[allow(
        clippy::too_many_arguments,
        reason = "mirrors the Event::Read payload; splitting it would obscure the E1-E3 tables"
    )]
    fn on_read(
        &mut self,
        key: QueryKey,
        policy: ReadPolicy,
        fetcher: Option<ErasedFetcher>,
        compare: Option<ErasedCompare>,
        opts: QueryOptions,
        now: Instant,
        ctx: &mut Ctx,
    ) -> Outcome {
        if policy == ReadPolicy::CacheOnly {
            // E1-1: a missing entry is *not* created.
            let Some(e) = self.entries.get_mut(&key) else {
                return Outcome::Read(ReadOutcome::Ready(Snapshot::empty()));
            };
            // E1-2: last-wins fetcher replacement; the read's gc_time joins OPT-2.
            if let Some(f) = fetcher {
                e.fetcher = Some(f);
            }
            if let Some(c) = compare {
                e.compare = Some(c);
            }
            e.opts.last_touch_gc_time = Some(opts.gc_time);
            ctx.mark_touched(&key);
            return Outcome::Read(ReadOutcome::Ready(e.snapshot_now()));
        }

        // E2/E3 preamble: create if missing; fetcher last-wins; record read opts.
        let e = self.entry_or_create(&key);
        if let Some(f) = fetcher {
            e.fetcher = Some(f);
        }
        if let Some(c) = compare {
            e.compare = Some(c);
        }
        e.opts.last_touch_gc_time = Some(opts.gc_time);
        ctx.mark_touched(&key);

        let has_data = e.data.is_some();
        let stale = e.is_stale(opts.stale_time, now);
        let read = match policy {
            ReadPolicy::StaleWhileRevalidate => {
                if has_data && !stale {
                    // E2-1: fresh hit.
                    ReadOutcome::Ready(e.snapshot_now())
                } else if has_data {
                    // E2-2: stale hit — refresh in the background if possible.
                    if e.can_start_fetch() {
                        start_fetch(e, &key, ctx);
                        ctx.mark_notify(&key);
                    }
                    // E2-2 / E2-3: either way, return the stale snapshot now.
                    ReadOutcome::Ready(e.snapshot_now())
                } else if let Some(s) = e.inflight {
                    // E2-4: no data, join the in-flight request.
                    ReadOutcome::Wait {
                        target: s,
                        rx: e.tx.subscribe(),
                    }
                } else if e.can_start_fetch() {
                    // E2-5: no data, start the first load.
                    let s = start_fetch(e, &key, ctx);
                    ctx.mark_notify(&key);
                    ReadOutcome::Wait {
                        target: s,
                        rx: e.tx.subscribe(),
                    }
                } else if e.mutation_active > 0 {
                    // E2-6: wait for the mutation to settle (or a later fetch).
                    ReadOutcome::Wait {
                        target: e.seq,
                        rx: e.tx.subscribe(),
                    }
                } else {
                    // E2-7.
                    ReadOutcome::NoFetcher
                }
            }
            ReadPolicy::EnsureFresh => {
                if has_data && !stale {
                    // E3-1: fresh hit.
                    ReadOutcome::Ready(e.snapshot_now())
                } else if let Some(s) = e.inflight {
                    // E3-2.
                    ReadOutcome::Wait {
                        target: s,
                        rx: e.tx.subscribe(),
                    }
                } else if e.can_start_fetch() {
                    // E3-3.
                    let s = start_fetch(e, &key, ctx);
                    ctx.mark_notify(&key);
                    ReadOutcome::Wait {
                        target: s,
                        rx: e.tx.subscribe(),
                    }
                } else if e.mutation_active > 0 {
                    // E3-4.
                    ReadOutcome::Wait {
                        target: e.seq,
                        rx: e.tx.subscribe(),
                    }
                } else {
                    // E3-5.
                    ReadOutcome::NoFetcher
                }
            }
            ReadPolicy::CacheOnly => unreachable!("handled above"),
        };
        Outcome::Read(read)
    }

    /// E4.
    fn on_subscribe(
        &mut self,
        key: QueryKey,
        fetcher: ErasedFetcher,
        compare: Option<ErasedCompare>,
        opts: QueryOptions,
        now: Instant,
        ctx: &mut Ctx,
    ) -> Outcome {
        let sub_id = self.next_sub_id;
        self.next_sub_id += 1;
        let e = self.entry_or_create(&key);
        ctx.mark_touched(&key);
        e.fetcher = Some(fetcher);
        if let Some(c) = compare {
            e.compare = Some(c);
        }
        e.opts.last_touch_gc_time = Some(opts.gc_time);
        let stale = e.is_stale(opts.stale_time, now);
        e.opts.subs.insert(sub_id, opts);
        // E4-1: count the subscriber; bumping gc_gen logically cancels any
        // pending GC timer.
        e.subscribers += 1;
        e.gc_gen += 1;
        ctx.mark_refresh_changed(&key);
        // E4-2: missing or stale data starts a fetch when possible.
        if (e.data.is_none() || stale) && e.can_start_fetch() {
            start_fetch(e, &key, ctx);
            ctx.mark_notify(&key);
        }
        // No extra Notify for the subscription itself: the watch channel's
        // current value already is the snapshot (E4 note).
        Outcome::Subscribed {
            sub_id,
            rx: e.tx.subscribe(),
        }
    }

    /// E5.
    fn on_unsubscribe(&mut self, key: &QueryKey, sub_id: u64, ctx: &mut Ctx) {
        // E5-1.
        let Some(e) = self.entries.get_mut(key) else {
            return;
        };
        debug_assert!(e.subscribers > 0, "unsubscribe without matching subscribe");
        e.subscribers = e.subscribers.saturating_sub(1);
        e.opts.subs.remove(&sub_id);
        ctx.mark_touched(key);
        ctx.mark_refresh_changed(key);
        // GC-1 / RF-1 post rules do the rest.
    }

    /// E6.
    fn on_revalidate_requested(&mut self, key: &QueryKey, ctx: &mut Ctx) {
        // E6-1: no entry or no fetcher — nothing to do.
        let Some(e) = self.entries.get_mut(key) else {
            return;
        };
        if e.fetcher.is_none() {
            return;
        }
        ctx.mark_touched(key);
        // E6-2: deduplicate against an in-flight request or active mutation.
        if e.inflight.is_some() || e.mutation_active > 0 {
            return;
        }
        // E6-3.
        start_fetch(e, key, ctx);
        ctx.mark_notify(key);
    }

    /// E7.
    fn on_commit_ok(
        &mut self,
        key: &QueryKey,
        incarnation: u64,
        seq: u64,
        value: ErasedValue,
        now: Instant,
        ctx: &mut Ctx,
    ) {
        // E7-1: entry already GC'd — a legal race, drop silently (GC-2).
        let Some(e) = self.entries.get_mut(key) else {
            return;
        };
        // E7-0 / SEQ-5: a flight from an earlier incarnation of this key must
        // not alias the rebuilt entry's seq space (D-31).
        if e.incarnation != incarnation {
            return;
        }
        // E7-2: mutations veto every fetch commit (SEQ-2, D-6).
        if e.mutation_active > 0 {
            return;
        }
        // E7-3: out-of-order or interrupted response (SEQ-2).
        if e.inflight != Some(seq) {
            return;
        }
        // E7-4: the commit lands.
        debug_assert_eq!(e.seq, seq, "inflight seq must equal the entry seq");
        debug_assert!(
            !e.invalidated,
            "INV-A: invalidated must be false on the commit-apply path"
        );
        // D-30 / CMP-1: structural sharing. When a comparator says the new
        // value equals the current one, keep the old `Arc` (so subscribers can
        // detect no-change via `Arc::ptr_eq`) and drop the new allocation.
        // Everything else — data_seq, updated_at, the notify — advances
        // exactly as without a comparator; skipping the notify would strand
        // EnsureFresh waiters (WAIT-1, D-11).
        let unchanged = e
            .compare
            .as_ref()
            .zip(e.data.as_ref())
            .is_some_and(|(compare, old)| compare(old, &value));
        if !unchanged {
            e.data = Some(value);
        }
        e.data_seq = seq;
        e.updated_at = Some(now);
        e.error = None;
        e.inflight = None;
        e.optimistic = None;
        ctx.mark_touched(key);
        ctx.mark_notify(key);
        ctx.mark_refresh_changed(key);
    }

    /// E8.
    fn on_commit_err(
        &mut self,
        key: &QueryKey,
        incarnation: u64,
        seq: u64,
        error: ErasedValue,
        ctx: &mut Ctx,
    ) {
        // E8-1..3: same guards as E7, including the incarnation fence (E8-0).
        let Some(e) = self.entries.get_mut(key) else {
            return;
        };
        if e.incarnation != incarnation {
            return;
        }
        if e.mutation_active > 0 {
            return;
        }
        if e.inflight != Some(seq) {
            return;
        }
        // E8-4: record the error; data and updated_at stay untouched (D-10).
        e.error = Some(error);
        e.error_seq = seq;
        e.inflight = None;
        ctx.mark_touched(key);
        ctx.mark_notify(key);
        ctx.mark_refresh_changed(key);
    }

    /// E9.
    fn on_mutate_set(&mut self, key: QueryKey, value: ErasedValue, now: Instant, ctx: &mut Ctx) {
        let e = self.entry_or_create(&key);
        ctx.mark_touched(&key);
        // E9-1.
        e.local_write(value, now);
        e.optimistic = None;
        ctx.mark_notify(&key);
    }

    /// E10.
    fn on_mutate_begin(
        &mut self,
        key: QueryKey,
        optimistic: Option<ErasedValue>,
        ctx: &mut Ctx,
    ) -> Outcome {
        let e = self.entry_or_create(&key);
        ctx.mark_touched(&key);
        // E10-1: the is_mutating flip must notify — wait loops depend on it (5.6).
        e.mutation_active += 1;
        e.discard_flight();
        ctx.mark_notify(&key);
        let mut written_seq = None;
        if let Some(value) = optimistic {
            // E10-2: snapshot the previous state, then write the optimistic
            // value. `updated_at` stays put: optimistic values never count as
            // fresh (D-7).
            e.seq += 1;
            e.optimistic = Some(OptimisticSnapshot {
                prev_data: e.data.take(),
                prev_error: e.error.clone(),
                prev_data_seq: e.data_seq,
                prev_updated_at: e.updated_at,
                written_seq: e.seq,
            });
            e.data = Some(value);
            e.data_seq = e.seq;
            written_seq = Some(e.seq);
        }
        Outcome::Mutation(MutationToken { key, written_seq })
    }

    /// E11. Steps run in order; they are not mutually exclusive guards.
    fn on_mutate_commit(
        &mut self,
        token: MutationToken,
        result: Result<Option<ErasedValue>, ErasedValue>,
        flags: MutateFlags,
        now: Instant,
        ctx: &mut Ctx,
    ) {
        let key = token.key.clone();
        let Some(e) = self.entries.get_mut(&key) else {
            debug_assert!(
                false,
                "MutateCommit on removed entry: mutation_active blocks GC (E14)"
            );
            return;
        };
        ctx.mark_touched(&key);
        // E11-1.
        debug_assert!(e.mutation_active > 0, "MutateCommit without MutateBegin");
        e.mutation_active = e.mutation_active.saturating_sub(1);
        // E11-2.
        match result {
            Ok(Some(value)) if flags.populate => {
                e.local_write(value, now);
                e.optimistic = None;
                ctx.mark_notify(&key);
            }
            Ok(_) => {
                // The optimistic value stays as the current value; step 3's
                // revalidation converges the truth. Only this mutation's own
                // snapshot is dropped (single-slot policy, spec §13).
                clear_own_optimistic(e, &token);
            }
            Err(error) => {
                // Mutation errors never write error_seq (WAIT-4): they belong
                // to the mutate() caller, not to waiting readers.
                e.error = Some(error);
                ctx.mark_notify(&key);
                rollback_if_unclobbered(e, &token, flags.rollback_on_error);
            }
        }
        // E11-3.
        finish_mutation(e, &key, flags.revalidate, ctx);
        // E11-4: always notify — the is_mutating flip back to false is the
        // wake-up the wait loop (5.6) relies on. EFF-3 merges duplicates.
        ctx.mark_notify(&key);
    }

    /// `MutateAbort`: the error path of E11 without an error to record.
    fn on_mutate_abort(&mut self, token: MutationToken, flags: MutateFlags, ctx: &mut Ctx) {
        let key = token.key.clone();
        let Some(e) = self.entries.get_mut(&key) else {
            debug_assert!(
                false,
                "MutateAbort on removed entry: mutation_active blocks GC (E14)"
            );
            return;
        };
        ctx.mark_touched(&key);
        debug_assert!(e.mutation_active > 0, "MutateAbort without MutateBegin");
        e.mutation_active = e.mutation_active.saturating_sub(1);
        rollback_if_unclobbered(e, &token, flags.rollback_on_error);
        finish_mutation(e, &key, flags.revalidate, ctx);
        ctx.mark_notify(&key);
    }

    /// E12.
    fn on_invalidate(&mut self, prefix: &[Segment], ctx: &mut Ctx) {
        // K-3: v1 prefix matching is an O(n) scan over the table.
        let keys: Vec<QueryKey> = self
            .entries
            .keys()
            .filter(|k| k.matches_prefix(prefix))
            .cloned()
            .collect();
        for key in keys {
            let e = self
                .entries
                .get_mut(&key)
                .expect("key collected from the table above");
            ctx.mark_touched(&key);
            // E12-1: during a mutation, only mark; E11 step 3 converges later.
            if e.mutation_active > 0 {
                e.invalidated = true;
                continue;
            }
            // E12-2: mark dirty; discard any in-flight request (D-5).
            e.invalidated = true;
            if e.inflight.is_some() {
                e.discard_flight();
            }
            ctx.mark_notify(&key);
            // E12-3: active entries with a fetcher refetch immediately.
            if e.is_active() && e.fetcher.is_some() {
                start_fetch(e, &key, ctx);
                ctx.mark_notify(&key);
            }
            // Entries without subscribers stay marked; the next Read/Subscribe
            // sees them stale and fetches then.
        }
    }

    /// E13.
    fn on_broadcast(&mut self, ev: SwrEvent, now: Instant, ctx: &mut Ctx) {
        let default_stale = self.defaults.stale_time;
        let default_throttle = self.defaults.focus_throttle;
        let keys: Vec<QueryKey> = self.entries.keys().cloned().collect();
        for key in keys {
            let e = self
                .entries
                .get_mut(&key)
                .expect("key collected from the table above");
            // E13: every condition must hold.
            if !e.is_active() {
                continue;
            }
            let enabled = match ev {
                SwrEvent::Focus => e.opts.any_on_focus(),
                SwrEvent::Online => e.opts.any_on_online(),
            };
            if !enabled {
                continue;
            }
            // OPT-5 (D-27): focus events are throttled per entry; online
            // events are not (mirrors SWR's focusThrottleInterval).
            if ev == SwrEvent::Focus && e.focus_blocked_until.is_some_and(|until| now < until) {
                continue;
            }
            if !e.is_stale(e.opts.min_stale_time(default_stale), now) {
                continue;
            }
            if e.inflight.is_some() || e.mutation_active > 0 || e.fetcher.is_none() {
                continue;
            }
            if ev == SwrEvent::Focus {
                // Re-arm the throttle window on each accepted focus event.
                e.focus_blocked_until =
                    now.checked_add(e.opts.min_focus_throttle(default_throttle));
            }
            ctx.mark_touched(&key);
            start_fetch(e, &key, ctx);
            ctx.mark_notify(&key);
        }
    }

    /// E14.
    fn on_timer_gc(&mut self, key: &QueryKey, generation: u64) {
        // E14-1: missing entry or stale generation (TMR-1) — ignore.
        let Some(e) = self.entries.get(key) else {
            return;
        };
        if generation != e.gc_gen {
            return;
        }
        // E14-2: remove; dropping the watch sender closes every receiver.
        if e.is_quiescent() {
            self.entries.remove(key);
        }
        // E14-3: otherwise ignore.
    }

    /// E15.
    fn on_timer_refresh(&mut self, key: &QueryKey, generation: u64, ctx: &mut Ctx) {
        // E15-1: missing entry or stale generation (TMR-1) — ignore.
        let Some(e) = self.entries.get_mut(key) else {
            return;
        };
        if generation != e.refresh_gen {
            return;
        }
        // E15-2: no subscribers — natural stop.
        if e.subscribers == 0 {
            return;
        }
        // E15-3: busy — only re-arm the next tick via RF-1.
        if e.inflight.is_some() || e.mutation_active > 0 {
            ctx.mark_refresh_changed(key);
            return;
        }
        // E15-4: refresh now, and re-arm via RF-1.
        if e.fetcher.is_some() {
            ctx.mark_touched(key);
            start_fetch(e, key, ctx);
            ctx.mark_notify(key);
        }
        ctx.mark_refresh_changed(key);
    }

    /// GC-1 and RF-1 (5.4): run after every event over the touched entries.
    fn post_rules(&mut self, now: Instant, ctx: &mut Ctx) {
        let default_gc = self.defaults.gc_time;
        // GC-1: quiescent entries (re)start the GC countdown. An in-flight
        // request defers scheduling until its commit lands (D-8).
        let touched = std::mem::take(&mut ctx.touched);
        for key in &touched {
            let Some(e) = self.entries.get_mut(key) else {
                continue;
            };
            if e.is_quiescent() {
                e.gc_gen += 1;
                // An unrepresentable deadline (huge gc_time) means: never
                // collect — skip scheduling instead of overflowing.
                if let Some(at) = now.checked_add(e.opts.gc_time(default_gc)) {
                    ctx.effects.push(Effect::ScheduleTimer {
                        key: key.clone(),
                        kind: TimerKind::Gc,
                        at,
                        generation: e.gc_gen,
                    });
                }
            }
        }
        // RF-1: reschedule when the refresh basis changed.
        let refresh = std::mem::take(&mut ctx.refresh_changed);
        for key in &refresh {
            let Some(e) = self.entries.get_mut(key) else {
                continue;
            };
            if e.subscribers > 0 {
                if let Some(at) = e
                    .opts
                    .refresh_interval()
                    .and_then(|interval| now.checked_add(interval))
                {
                    e.refresh_gen += 1;
                    ctx.effects.push(Effect::ScheduleTimer {
                        key: key.clone(),
                        kind: TimerKind::Refresh,
                        at,
                        generation: e.refresh_gen,
                    });
                }
            } else {
                // Natural expiry: no reschedule, just invalidate old timers.
                e.refresh_gen += 1;
            }
        }
    }

    /// EFF-2 / EFF-3: append one merged `Notify` per marked key, after every
    /// `StartFetch` in the batch.
    fn flush_notifies(&mut self, ctx: &mut Ctx) {
        for key in std::mem::take(&mut ctx.notify) {
            let Some(e) = self.entries.get_mut(&key) else {
                continue;
            };
            e.snap_version += 1;
            let snapshot = e.build_snapshot(e.snap_version);
            ctx.effects.push(Effect::Notify {
                tx: Arc::clone(&e.tx),
                snapshot,
            });
        }
    }

    /// Test hook: force-remove an entry, simulating a completed GC (IT2).
    #[cfg(test)]
    pub(crate) fn remove_entry_for_test(&mut self, key: &QueryKey) {
        self.entries.remove(key);
    }
}

/// Drop this mutation's own optimistic snapshot, leaving another mutation's
/// snapshot in place (single-slot policy, spec §13).
fn clear_own_optimistic(e: &mut EntryCore, token: &MutationToken) {
    let Some(ws) = token.written_seq else {
        return;
    };
    if e.optimistic.as_ref().is_some_and(|s| s.written_seq == ws) {
        e.optimistic = None;
    }
}

/// E11-2 error branch / SEQ-4: roll back only when this mutation's optimistic
/// write is still the latest write (`data_seq == written_seq`); otherwise the
/// later write wins and the snapshot is dropped.
fn rollback_if_unclobbered(e: &mut EntryCore, token: &MutationToken, rollback_on_error: bool) {
    let Some(ws) = token.written_seq else {
        return;
    };
    if !e.optimistic.as_ref().is_some_and(|s| s.written_seq == ws) {
        // The slot belongs to a later mutation (or was cleared by a local
        // write, E9); data_seq has moved past ws either way — skip.
        return;
    }
    let snap = e.optimistic.take().expect("checked above");
    if rollback_on_error && e.data_seq == ws {
        e.data = snap.prev_data;
        e.error = snap.prev_error;
        e.data_seq = snap.prev_data_seq;
        e.updated_at = snap.prev_updated_at;
    }
    // SEQ-4 otherwise: a later write wins; the snapshot is dropped either way.
}

/// E11 step 3: once the last concurrent mutation is out, revalidate (or
/// converge a deferred invalidation).
fn finish_mutation(e: &mut EntryCore, key: &QueryKey, revalidate: bool, ctx: &mut Ctx) {
    if e.mutation_active == 0 && (revalidate || e.invalidated) && e.can_start_fetch() {
        // `start_fetch` clears `invalidated`; this refresh converges it.
        start_fetch(e, key, ctx);
        ctx.mark_notify(key);
    }
}

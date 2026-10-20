//! A generic ordered, replayable, broadcast delivery log.
//!
//! [`DeliveryLog<T>`] hands every item a strictly increasing `u64` seq, fans it
//! out to live subscribers, and retains a bounded window of past items so a
//! reconnecting subscriber can catch up. When the window no longer covers what
//! a subscriber asks for, the answer is an explicit [`Replay::Gap`] rather than
//! a silently partial replay.
//!
//! Every log also has an [`epoch`](DeliveryLog::epoch): a random id minted
//! when it is created. A seq means something only within one log, so a reader
//! that keeps its place across the log being replaced (the producer restarted,
//! or the log was dropped and created again) should hold a [`Cursor`] and ask
//! [`DeliveryLog::replay_from`], which answers another log's cursor with
//! [`Replay::Gap`] instead of treating its seq as one of this log's.
//!
//! This crate is protocol-agnostic: it knows nothing about what `T` is.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use tokio::sync::broadcast;

/// Default number of delivered items retained for replay, for callers that do
/// not want to size the log themselves. See [`DeliveryLog::with_defaults`].
pub const DEFAULT_RETENTION: usize = 1024;
/// Default depth of the live broadcast fan-out channel — NOT the replay log's
/// size. See [`DeliveryLog::with_defaults`].
pub const DEFAULT_LIVE_CAPACITY: usize = 256;

/// Where a reader stands in one particular log: that log's
/// [`epoch`](DeliveryLog::epoch) and the last seq the reader has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor<'a> {
    pub epoch: &'a str,
    pub seq: u64,
}

pub struct DeliveryLog<T: Clone + Send + 'static> {
    inner: Mutex<DeliveryLogInner<T>>,
    retention: usize,
    epoch: String,
}

struct DeliveryLogInner<T> {
    entries: BTreeMap<u64, Retained<T>>,
    keys: HashMap<String, u64>,
    next_seq: u64,
    live: broadcast::Sender<(u64, T)>,
}

struct Retained<T> {
    key: Option<String>,
    item: T,
}

fn collect<'a, T: Clone + 'a>(
    entries: impl Iterator<Item = (&'a u64, &'a Retained<T>)>,
) -> Vec<(u64, T)> {
    entries.map(|(seq, r)| (*seq, r.item.clone())).collect()
}

/// The outcome of a replay request. `Gap` means the caller's `last_seen_seq`
/// predates what the log still holds — it must rebuild from a baseline, not
/// stitch `retained` onto its old state as if nothing were missing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Replay<T> {
    /// Every item after the requested seq is present, in order, with no hole.
    /// Always the answer for `None`, and for an empty log.
    Contiguous(Vec<(u64, T)>),
    /// Entries after the requested seq were evicted, or (from
    /// [`DeliveryLog::replay_from`]) the cursor belongs to another log or
    /// points past this one. `first_retained_seq` is the oldest seq still
    /// held (the next seq to be written, when the log holds nothing), so the
    /// caller can size the hole.
    Gap {
        first_retained_seq: u64,
        retained: Vec<(u64, T)>,
    },
}

impl<T> Replay<T> {
    /// Everything the log can still hand back, gap or not.
    pub fn retained(self) -> Vec<(u64, T)> {
        match self {
            Replay::Contiguous(v) => v,
            Replay::Gap { retained, .. } => retained,
        }
    }

    pub fn is_gap(&self) -> bool {
        matches!(self, Replay::Gap { .. })
    }
}

impl<T: Clone + Send + 'static> DeliveryLog<T> {
    /// `retention`: how many past entries stay available for replay before the
    /// oldest are evicted. `live_capacity`: depth of the live broadcast
    /// fan-out channel — NOT the same thing as retention.
    ///
    /// Both must be non-zero: a zero retention would evict every item as it
    /// arrived, and `tokio::sync::broadcast` rejects a zero capacity.
    pub fn new(retention: usize, live_capacity: usize) -> Self {
        assert!(retention > 0, "DeliveryLog retention must be non-zero");
        assert!(
            live_capacity > 0,
            "DeliveryLog live_capacity must be non-zero"
        );
        let (live, _) = broadcast::channel(live_capacity);
        Self {
            inner: Mutex::new(DeliveryLogInner {
                entries: BTreeMap::new(),
                keys: HashMap::new(),
                next_seq: 0,
                live,
            }),
            retention,
            epoch: uuid::Uuid::new_v4().simple().to_string(),
        }
    }

    /// A log sized with [`DEFAULT_RETENTION`] and [`DEFAULT_LIVE_CAPACITY`],
    /// for callers with no particular volume or retention needs.
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_RETENTION, DEFAULT_LIVE_CAPACITY)
    }

    /// How many past entries this log retains for replay.
    pub fn retention(&self) -> usize {
        self.retention
    }

    pub fn push(&self, item: T) -> u64 {
        self.push_inner(None, item)
    }

    /// Like [`push`](Self::push), but the earlier retained entry with the same
    /// `key`, if there is one, is dropped from the retained buffer, so a hot
    /// item is retained once however often it is pushed. Live subscribers still
    /// receive every push, and seqs keep increasing; only a read of the retained
    /// entries skips the replaced seqs. [`forget_key`](Self::forget_key) ends
    /// the replacement chain.
    pub fn push_keyed(&self, key: impl Into<String>, item: T) -> u64 {
        self.push_inner(Some(key.into()), item)
    }

    /// A later [`push_keyed`](Self::push_keyed) of `key` replaces nothing pushed
    /// before this call, so entries on either side of a removal both stay.
    pub fn forget_key(&self, key: &str) {
        let mut inner = self.inner.lock().expect("DeliveryLog mutex poisoned");
        inner.keys.remove(key);
    }

    fn push_inner(&self, key: Option<String>, item: T) -> u64 {
        let mut inner = self.inner.lock().expect("DeliveryLog mutex poisoned");
        let seq = inner.next_seq;
        inner.next_seq += 1;
        if let Some(key) = &key {
            if let Some(earlier) = inner.keys.insert(key.clone(), seq) {
                inner.entries.remove(&earlier);
            }
        }
        inner.entries.insert(
            seq,
            Retained {
                key,
                item: item.clone(),
            },
        );
        while inner.entries.len() > self.retention {
            let Some((evicted_seq, evicted)) = inner.entries.pop_first() else {
                break;
            };
            if let Some(key) = evicted.key {
                if inner.keys.get(&key) == Some(&evicted_seq) {
                    inner.keys.remove(&key);
                }
            }
        }
        let _ = inner.live.send((seq, item));
        seq
    }

    fn replay_locked(
        entries: &BTreeMap<u64, Retained<T>>,
        last_seen_seq: Option<u64>,
    ) -> Replay<T> {
        let first_retained_seq = entries.keys().next().copied();
        match (last_seen_seq, first_retained_seq) {
            (Some(seq), Some(first)) if seq + 1 < first => Replay::Gap {
                first_retained_seq: first,
                retained: collect(entries.iter()),
            },
            (None, _) => Replay::Contiguous(collect(entries.iter())),
            (Some(seq), _) => Replay::Contiguous(collect(entries.range(seq + 1..))),
        }
    }

    /// Recover after a `broadcast::error::RecvError::Lagged`, or check
    /// whether a `last_seen_seq` can still be satisfied, without opening a
    /// new live subscription.
    pub fn replay_since(&self, last_seen_seq: Option<u64>) -> Replay<T> {
        let inner = self.inner.lock().expect("DeliveryLog mutex poisoned");
        Self::replay_locked(&inner.entries, last_seen_seq)
    }

    /// This log's epoch: 32 lowercase hex characters, random, fixed for the
    /// log's lifetime. Two logs never share one.
    pub fn epoch(&self) -> &str {
        &self.epoch
    }

    /// Like [`replay_since`](Self::replay_since), but for a reader that names
    /// the log its seq came from. A cursor from another epoch, or one at or
    /// past [`next_seq`](Self::next_seq), was never part of this log's
    /// history: the answer is [`Replay::Gap`] with everything retained, so the
    /// reader rebuilds from a baseline.
    pub fn replay_from(&self, cursor: Option<Cursor<'_>>) -> Replay<T> {
        let inner = self.inner.lock().expect("DeliveryLog mutex poisoned");
        match cursor {
            Some(cursor) if cursor.epoch != self.epoch || cursor.seq >= inner.next_seq => {
                Replay::Gap {
                    first_retained_seq: inner
                        .entries
                        .keys()
                        .next()
                        .copied()
                        .unwrap_or(inner.next_seq),
                    retained: collect(inner.entries.iter()),
                }
            }
            cursor => Self::replay_locked(&inner.entries, cursor.map(|c| c.seq)),
        }
    }

    pub fn next_seq(&self) -> u64 {
        self.inner
            .lock()
            .expect("DeliveryLog mutex poisoned")
            .next_seq
    }

    /// Catch up and subscribe under one lock, so no item can slip between the
    /// replay snapshot and the start of the live subscription.
    pub fn subscribe_with_replay(
        &self,
        last_seen_seq: Option<u64>,
    ) -> (Replay<T>, broadcast::Receiver<(u64, T)>) {
        let inner = self.inner.lock().expect("DeliveryLog mutex poisoned");
        let replay = Self::replay_locked(&inner.entries, last_seen_seq);
        let rx = inner.live.subscribe();
        (replay, rx)
    }
}

impl<T: Clone + Send + 'static> Default for DeliveryLog<T> {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RETENTION: usize = DEFAULT_RETENTION;

    #[test]
    fn push_assigns_strictly_increasing_seq_starting_at_zero() {
        let log = DeliveryLog::<String>::with_defaults();
        assert_eq!(log.push("a".to_string()), 0);
        assert_eq!(log.push("b".to_string()), 1);
        assert_eq!(log.push("c".to_string()), 2);
    }

    #[test]
    fn subscribe_with_replay_none_returns_everything_so_far() {
        let log = DeliveryLog::<String>::with_defaults();
        log.push("a".to_string());
        log.push("b".to_string());
        let (replay, _rx) = log.subscribe_with_replay(None);
        assert_eq!(
            replay,
            Replay::Contiguous(vec![(0, "a".to_string()), (1, "b".to_string())])
        );
    }

    #[test]
    fn subscribe_with_replay_some_seq_returns_only_entries_after_it() {
        let log = DeliveryLog::<String>::with_defaults();
        log.push("a".to_string());
        log.push("b".to_string());
        log.push("c".to_string());
        let (replay, _rx) = log.subscribe_with_replay(Some(0));
        assert_eq!(
            replay,
            Replay::Contiguous(vec![(1, "b".to_string()), (2, "c".to_string())])
        );
    }

    #[test]
    fn subscribe_with_replay_current_seq_returns_empty() {
        let log = DeliveryLog::<String>::with_defaults();
        log.push("a".to_string());
        let (replay, _rx) = log.subscribe_with_replay(Some(0));
        assert!(replay.retained().is_empty());
    }

    #[tokio::test]
    async fn a_live_subscriber_receives_a_push_made_after_it_subscribed() {
        let log = DeliveryLog::<String>::with_defaults();
        let (_replay, mut rx) = log.subscribe_with_replay(None);
        log.push("a".to_string());
        let received = rx.recv().await.unwrap();
        assert_eq!(received, (0, "a".to_string()));
    }

    #[tokio::test]
    async fn two_simultaneous_subscribers_receive_the_same_sequence_in_the_same_order() {
        let log = DeliveryLog::<String>::with_defaults();
        let (_r1, mut rx1) = log.subscribe_with_replay(None);
        let (_r2, mut rx2) = log.subscribe_with_replay(None);
        log.push("a".to_string());
        log.push("b".to_string());
        assert_eq!(rx1.recv().await.unwrap(), (0, "a".to_string()));
        assert_eq!(rx1.recv().await.unwrap(), (1, "b".to_string()));
        assert_eq!(rx2.recv().await.unwrap(), (0, "a".to_string()));
        assert_eq!(rx2.recv().await.unwrap(), (1, "b".to_string()));
    }

    #[test]
    fn pushing_past_retention_evicts_the_oldest_entry() {
        let log = DeliveryLog::<u64>::with_defaults();
        for i in 0..=RETENTION as u64 {
            log.push(i);
        }
        let (replay, _rx) = log.subscribe_with_replay(None);
        let retained = replay.retained();
        assert_eq!(retained.len(), RETENTION);
        assert_eq!(
            retained.first().unwrap().0,
            1,
            "seq 0 must have been evicted to stay within retention"
        );
        assert_eq!(retained.last().unwrap().0, RETENTION as u64);
    }

    #[test]
    fn replay_for_an_evicted_seq_returns_a_gap_not_a_silent_partial_replay() {
        let log = DeliveryLog::<u64>::with_defaults();
        // Evict two entries (seq 0 and seq 1), not one: a client who has
        // already seen seq 0 doesn't need it replayed, so evicting only
        // seq 0 is never itself a gap (see the "current seq" test above).
        // The gap is the item AFTER last_seen_seq being missing.
        for i in 0..RETENTION as u64 + 2 {
            log.push(i);
        }
        // Resuming from seq 0 needs seq 1 onward, but seq 1 was evicted too.
        let (replay, _rx) = log.subscribe_with_replay(Some(0));
        match replay {
            Replay::Gap {
                first_retained_seq,
                retained,
            } => {
                assert_eq!(first_retained_seq, 2);
                assert_eq!(retained.len(), RETENTION);
            }
            Replay::Contiguous(_) => {
                panic!("expected a Gap: seq 1 was evicted and resuming from seq 0 needs it")
            }
        }
    }

    #[test]
    fn a_brand_new_subscriber_never_sees_a_gap_even_on_an_over_full_log() {
        let log = DeliveryLog::<u64>::with_defaults();
        for i in 0..=RETENTION as u64 {
            log.push(i);
        }
        let (replay, _rx) = log.subscribe_with_replay(None);
        assert!(
            !replay.is_gap(),
            "None must always mean 'everything currently retained', never an error"
        );
    }

    #[test]
    fn last_seen_seq_at_or_after_the_oldest_retained_entry_is_not_a_gap() {
        let log = DeliveryLog::<u64>::with_defaults();
        for i in 0..=RETENTION as u64 {
            log.push(i);
        }
        // seq 1 is the oldest still retained; asking to resume from it is satisfiable.
        let (replay, _rx) = log.subscribe_with_replay(Some(1));
        assert!(!replay.is_gap());
    }

    #[test]
    fn retention_is_per_instance_not_a_global_constant() {
        let log = DeliveryLog::<u64>::new(3, 4);
        assert_eq!(log.retention(), 3);
        for i in 0..5 {
            log.push(i);
        }
        let retained = log.replay_since(None).retained();
        assert_eq!(retained.len(), 3);
        assert_eq!(retained.first().unwrap().0, 2);
        assert_eq!(retained.last().unwrap().0, 4);
        // seq assignment is unaffected by eviction.
        assert_eq!(log.next_seq(), 5);
        // Resuming from an evicted seq is still an explicit gap.
        assert!(log.replay_since(Some(0)).is_gap());
    }

    #[test]
    fn with_defaults_matches_the_named_default_constants() {
        let log = DeliveryLog::<u64>::with_defaults();
        assert_eq!(log.retention(), DEFAULT_RETENTION);
    }

    #[tokio::test]
    async fn live_capacity_is_independent_of_retention() {
        // A log that retains a lot but fans out shallowly: a slow subscriber
        // lags on the live channel while replay can still serve it fully.
        let log = DeliveryLog::<u64>::new(64, 2);
        let (_replay, mut rx) = log.subscribe_with_replay(None);
        for i in 0..8 {
            log.push(i);
        }
        assert!(matches!(
            rx.recv().await,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
        ));
        let retained = log.replay_since(None).retained();
        assert_eq!(
            retained.len(),
            8,
            "retention is unaffected by the live channel lagging"
        );
    }

    #[test]
    #[should_panic(expected = "retention must be non-zero")]
    fn zero_retention_is_rejected_rather_than_silently_dropping_everything() {
        let _ = DeliveryLog::<u64>::new(0, 4);
    }

    #[test]
    #[should_panic(expected = "live_capacity must be non-zero")]
    fn zero_live_capacity_is_rejected() {
        let _ = DeliveryLog::<u64>::new(4, 0);
    }

    #[test]
    fn every_log_has_its_own_32_hex_epoch() {
        let a: DeliveryLog<String> = DeliveryLog::with_defaults();
        let b: DeliveryLog<String> = DeliveryLog::with_defaults();
        assert_eq!(a.epoch().len(), 32);
        assert!(a
            .epoch()
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        assert_ne!(a.epoch(), b.epoch());
        assert_eq!(a.epoch(), a.epoch(), "an epoch never changes for a log");
    }

    #[test]
    fn replay_from_the_same_epoch_matches_replay_since() {
        let log = DeliveryLog::new(3, 4);
        for s in ["a", "b", "c", "d", "e"] {
            log.push(s.to_string());
        }
        let epoch = log.epoch().to_string();
        for seq in [None, Some(0), Some(1), Some(2), Some(3)] {
            let cursor = seq.map(|seq| Cursor { epoch: &epoch, seq });
            assert_eq!(
                log.replay_from(cursor),
                log.replay_since(seq),
                "seq {seq:?}"
            );
        }
    }

    #[test]
    fn a_cursor_from_another_epoch_is_a_gap_with_everything_retained() {
        let log = DeliveryLog::new(4, 4);
        log.push("a".to_string());
        log.push("b".to_string());
        let replay = log.replay_from(Some(Cursor {
            epoch: "00000000000000000000000000000000",
            seq: 0,
        }));
        assert_eq!(
            replay,
            Replay::Gap {
                first_retained_seq: 0,
                retained: vec![(0, "a".to_string()), (1, "b".to_string())],
            }
        );
    }

    #[test]
    fn a_cursor_from_another_epoch_is_a_gap_even_on_an_empty_log() {
        let log: DeliveryLog<String> = DeliveryLog::with_defaults();
        let replay = log.replay_from(Some(Cursor {
            epoch: "00000000000000000000000000000000",
            seq: 7,
        }));
        assert_eq!(
            replay,
            Replay::Gap {
                first_retained_seq: 0,
                retained: vec![],
            }
        );
    }

    #[test]
    fn a_cursor_ahead_of_the_log_is_a_gap() {
        let log = DeliveryLog::new(4, 4);
        log.push("a".to_string());
        let epoch = log.epoch().to_string();
        // seq 0 exists, so a cursor at 0 is current; 1 and beyond were never written.
        assert_eq!(
            log.replay_from(Some(Cursor {
                epoch: &epoch,
                seq: 0
            })),
            Replay::Contiguous(vec![])
        );
        assert!(log
            .replay_from(Some(Cursor {
                epoch: &epoch,
                seq: 1
            }))
            .is_gap());
        assert!(log
            .replay_from(Some(Cursor {
                epoch: &epoch,
                seq: 99
            }))
            .is_gap());
    }

    fn retained_of(log: &DeliveryLog<String>) -> Vec<(u64, String)> {
        log.subscribe_with_replay(None).0.retained()
    }

    #[test]
    fn a_keyed_push_replaces_the_earlier_retained_entry_of_its_key() {
        let log = DeliveryLog::<String>::with_defaults();
        log.push_keyed("a", "a1".to_string());
        log.push("other".to_string());
        log.push_keyed("a", "a2".to_string());
        assert_eq!(
            retained_of(&log),
            vec![(1, "other".to_string()), (2, "a2".to_string())]
        );
    }

    #[tokio::test]
    async fn the_broadcast_still_carries_every_push() {
        let log = DeliveryLog::<String>::with_defaults();
        let (_replay, mut rx) = log.subscribe_with_replay(None);
        log.push_keyed("a", "a1".to_string());
        log.push_keyed("a", "a2".to_string());
        assert_eq!(rx.recv().await.unwrap(), (0, "a1".to_string()));
        assert_eq!(rx.recv().await.unwrap(), (1, "a2".to_string()));
    }

    #[test]
    fn seqs_keep_increasing_and_the_retained_tail_skips() {
        let log = DeliveryLog::<String>::with_defaults();
        log.push("x".to_string());
        for i in 0..5 {
            assert_eq!(log.push_keyed("hot", format!("h{i}")), 1 + i);
        }
        assert_eq!(log.next_seq(), 6);
        assert_eq!(
            retained_of(&log),
            vec![(0, "x".to_string()), (5, "h4".to_string())]
        );
    }

    #[test]
    fn forget_key_stops_a_later_push_replacing_across_the_removal() {
        let log = DeliveryLog::<String>::with_defaults();
        log.push_keyed("n", "put1".to_string());
        log.forget_key("n");
        log.push("removed".to_string());
        log.push_keyed("n", "put2".to_string());
        assert_eq!(
            retained_of(&log),
            vec![
                (0, "put1".to_string()),
                (1, "removed".to_string()),
                (2, "put2".to_string())
            ]
        );
    }

    #[test]
    fn retention_counts_retained_entries_only() {
        let log = DeliveryLog::<u64>::new(3, 16);
        for i in 0..100 {
            log.push_keyed("hot", i);
        }
        log.push(1000);
        log.push(1001);
        let retained = log.subscribe_with_replay(None).0.retained();
        assert_eq!(
            retained,
            vec![(99, 99), (100, 1000), (101, 1001)],
            "the hot item stays while the log holds fewer entries than its retention"
        );
        log.push(1002);
        let retained = log.subscribe_with_replay(None).0.retained();
        assert_eq!(retained.first().unwrap().0, 100);
        log.push_keyed("hot", 7);
        assert_eq!(log.subscribe_with_replay(None).0.retained().len(), 3);
    }

    #[test]
    fn an_unkeyed_push_is_unchanged() {
        let log = DeliveryLog::<String>::with_defaults();
        log.push("a".to_string());
        log.push("a".to_string());
        assert_eq!(retained_of(&log).len(), 2);
    }

    #[test]
    fn an_evicted_keyed_entry_frees_its_key() {
        let log = DeliveryLog::<u64>::new(2, 16);
        log.push_keyed("k", 1);
        log.push(2);
        log.push(3);
        log.push_keyed("k", 4);
        assert_eq!(
            log.subscribe_with_replay(None).0.retained(),
            vec![(2, 3), (3, 4)]
        );
    }
}

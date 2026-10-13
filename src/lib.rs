//! A generic ordered, replayable, broadcast delivery log.
//!
//! [`DeliveryLog<T>`] hands every item a strictly increasing `u64` seq, fans it
//! out to live subscribers, and retains a bounded window of past items so a
//! reconnecting subscriber can catch up. When the window no longer covers what
//! a subscriber asks for, the answer is an explicit [`Replay::Gap`] rather than
//! a silently partial replay.
//!
//! This crate is protocol-agnostic: it knows nothing about what `T` is.

use std::collections::VecDeque;
use std::sync::Mutex;
use tokio::sync::broadcast;

/// Default number of delivered items retained for replay, for callers that do
/// not want to size the log themselves. See [`DeliveryLog::with_defaults`].
pub const DEFAULT_RETENTION: usize = 1024;
/// Default depth of the live broadcast fan-out channel — NOT the replay log's
/// size. See [`DeliveryLog::with_defaults`].
pub const DEFAULT_LIVE_CAPACITY: usize = 256;

pub struct DeliveryLog<T: Clone + Send + 'static> {
    inner: Mutex<DeliveryLogInner<T>>,
    retention: usize,
}

struct DeliveryLogInner<T> {
    entries: VecDeque<(u64, T)>,
    next_seq: u64,
    live: broadcast::Sender<(u64, T)>,
}

/// The outcome of a replay request. `Gap` means the caller's `last_seen_seq`
/// predates what the log still holds — it must rebuild from a baseline, not
/// stitch `retained` onto its old state as if nothing were missing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Replay<T> {
    /// Every item after the requested seq is present, in order, with no hole.
    /// Always the answer for `None`, and for an empty log.
    Contiguous(Vec<(u64, T)>),
    /// Entries after the requested seq were evicted. `first_retained_seq` is
    /// the oldest seq still held, so the caller can size the hole.
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
                entries: VecDeque::new(),
                next_seq: 0,
                live,
            }),
            retention,
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
        let mut inner = self.inner.lock().expect("DeliveryLog mutex poisoned");
        let seq = inner.next_seq;
        inner.next_seq += 1;
        inner.entries.push_back((seq, item.clone()));
        if inner.entries.len() > self.retention {
            inner.entries.pop_front();
        }
        let _ = inner.live.send((seq, item));
        seq
    }

    fn replay_locked(entries: &VecDeque<(u64, T)>, last_seen_seq: Option<u64>) -> Replay<T> {
        let first_retained_seq = entries.front().map(|(s, _)| *s);
        match (last_seen_seq, first_retained_seq) {
            (Some(seq), Some(first)) if seq + 1 < first => Replay::Gap {
                first_retained_seq: first,
                retained: entries.iter().cloned().collect(),
            },
            (None, _) => Replay::Contiguous(entries.iter().cloned().collect()),
            (Some(seq), _) => {
                Replay::Contiguous(entries.iter().filter(|(s, _)| *s > seq).cloned().collect())
            }
        }
    }

    /// Recover after a `broadcast::error::RecvError::Lagged`, or check
    /// whether a `last_seen_seq` can still be satisfied, without opening a
    /// new live subscription.
    pub fn replay_since(&self, last_seen_seq: Option<u64>) -> Replay<T> {
        let inner = self.inner.lock().expect("DeliveryLog mutex poisoned");
        Self::replay_locked(&inner.entries, last_seen_seq)
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
}

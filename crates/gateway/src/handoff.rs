//! The seam between the venue and whoever distributes its events.
//!
//! Fan-out costs the trading thread nothing here. It copies a pass's events
//! once, into a buffer somebody else drains, and returns to matching. What it
//! replaces is a walk over every subscriber of every touched channel, done on
//! the thread that sequences orders -- which is why a thousand subscribers
//! moved the venue's own round trip from 17.5 microseconds to 14.3
//! milliseconds. The work did not disappear; it stopped being in the way.
//!
//! **Bounded, and it degrades in the right direction.** Buffers are allocated
//! once and recycled. If the far side falls behind, the venue finds none free
//! and drops the batch rather than growing a queue or waiting -- the rule every
//! exchange feed is built on. A dropped batch is a sequence gap, which is
//! exactly what subscribers already detect by arithmetic and repair by asking
//! for a restatement. Order entry is untouched by any of it.
//!
//! Two moves under a mutex, per pass, is the whole synchronisation cost. A
//! lock-free ring would buy less than it costs to justify: the lock is taken
//! once per group rather than once per event, and never while anything is
//! encoded or written.

use bx_protocol::Event;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Batches in flight before the venue starts dropping.
///
/// Each holds one pass, so this is how far the far side may fall behind. Sized
/// to absorb a scheduling hiccup rather than a slow consumer: a consumer that
/// needs more than this is not keeping up, and letting it accumulate would put
/// the venue's memory in a subscriber's hands.
const IN_FLIGHT: usize = 64;

/// Events a batch is sized for before it has to grow.
const PER_BATCH: usize = 4 * 1024;

/// How often the seam is allowed to ring the bell.
///
/// Waking is a syscall on the thread that sequences orders, and the two things
/// that suppress it multiply: a wake happens only when the far side is asleep
/// *and* this long has passed since the last one. Either alone leaves most of
/// them standing. Warm, 1,024 subscribers, median of three runs:
///
/// | wake suppressed by | round trip, min | p50 | orders/sec |
/// |---|---|---|---|
/// | nothing -- no wake at all | 8.1 us | 12.1 us | 3,959,737 |
/// | the flag and this cap | 8.6 us | 16.1 us | 3,888,577 |
/// | this cap alone | 16.2 us | 20.1 us | 3,492,010 |
/// | the flag alone | 16.9 us | 20.9 us | 4,042,887 |
/// | neither -- ring every pass | 18.3 us | 31.8 us | 3,541,885 |
///
/// Both earn their place, and together they cost the venue half a microsecond
/// of round trip against never waking the feed at all. What that buys is market
/// data leaving on the trade rather than 61.7 milliseconds later.
///
/// A quiet venue still rings on the first trade, the last wake being long past.
/// The one case left over is a group this cap suppresses with nothing behind it
/// -- the last of a burst, just before the venue goes quiet -- and the feed's
/// idle timer bounds that.
const WAKE_EVERY: Duration = Duration::from_micros(50);

#[derive(Debug, Default)]
struct Shared {
    /// Batches waiting to be taken, oldest first.
    filled: VecDeque<Vec<Event>>,
    /// Buffers the far side has finished with.
    spare: Vec<Vec<Event>>,
    /// Batches the venue could not hand over. A subscriber sees these as a
    /// sequence gap; an operator sees the count and knows the feed, not the
    /// venue, is behind.
    dropped: u64,
    /// Events inside those batches, which is the figure that says how much was
    /// missed rather than how often.
    events_dropped: u64,
}

/// The venue's end of the seam.
#[derive(Clone, Debug)]
pub struct Handoff {
    shared: Arc<Mutex<Shared>>,
    /// How the far side is told there is something to take.
    ///
    /// Without it the consumer can only find out by looking, and a consumer
    /// that waits on sockets looks when a socket does something -- so a batch
    /// handed over in a quiet moment sat until that wait timed out: 61.7
    /// milliseconds of market-data latency, measured, on a venue that answers
    /// orders in tens of microseconds. The same argument as telling a maker its
    /// order filled rather than making it ask.
    ///
    /// Shared across clones, and set by whichever clone the consumer holds.
    /// The venue's copy is made before the consumer exists, so a waker stored
    /// in one clone and not the others would be a waker nobody ever rings.
    wake: Arc<std::sync::OnceLock<Waker>>,
    /// Whether the far side is already awake and on its way to the queue.
    ///
    /// A consumer already draining does not need telling twice. On its own this
    /// stops fewer wakes than it sounds like it should, because the far side
    /// drains fast and is asleep again before the next group -- see
    /// [`WAKE_EVERY`] for what each suppressor is worth on its own.
    awake: Arc<AtomicBool>,
    /// When the seam started, so a wake can be timed without a shared clock.
    started: Instant,
    /// Nanoseconds since `started` at the last wake, for [`WAKE_EVERY`].
    last_wake: Arc<AtomicU64>,
}

/// What the far side registers to be woken through.
pub use mio::Waker;

impl Default for Handoff {
    fn default() -> Self {
        Self::new()
    }
}

impl Handoff {
    #[must_use]
    pub fn new() -> Self {
        let mut spare = Vec::with_capacity(IN_FLIGHT);
        for _ in 0..IN_FLIGHT {
            spare.push(Vec::with_capacity(PER_BATCH));
        }
        Self {
            shared: Arc::new(Mutex::new(Shared {
                filled: VecDeque::with_capacity(IN_FLIGHT),
                spare,
                dropped: 0,
                events_dropped: 0,
            })),
            wake: Arc::new(std::sync::OnceLock::new()),
            awake: Arc::new(AtomicBool::new(false)),
            started: Instant::now(),
            last_wake: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Gives the seam a way to wake whoever drains it.
    ///
    /// Called by the consumer once, with a waker registered on its own poll.
    pub fn wakes(&self, waker: Waker) {
        let _ = self.wake.set(waker);
    }

    /// Marks the far side as about to sleep, so the next offer wakes it.
    ///
    /// Called by the consumer immediately before it waits. Anything offered
    /// between this and the wait still wakes it, because the flag is cleared
    /// first and the offer that follows sees a sleeper.
    pub fn about_to_wait(&self) {
        self.awake.store(false, Ordering::Release);
    }

    /// Marks the far side as running, so offers stop paying for a syscall.
    pub fn awake(&self) {
        self.awake.store(true, Ordering::Release);
    }

    /// Hands over one pass's events. Returns whether they were taken.
    ///
    /// Never blocks and never allocates once running: the copy is into a buffer
    /// that has already been used and returned. A full seam means the far side
    /// is behind, and the batch is dropped rather than queued -- see the module
    /// note on why that is the only acceptable direction to fail in.
    pub fn offer(&self, events: &[Event]) -> bool {
        if events.is_empty() {
            return true;
        }
        let Ok(mut shared) = self.shared.lock() else {
            // The far side panicked holding the lock. The venue keeps trading:
            // a market-data thread is not permitted to take the book with it.
            return false;
        };
        let Some(mut batch) = shared.spare.pop() else {
            shared.dropped += 1;
            shared.events_dropped += events.len() as u64;
            return false;
        };
        batch.clear();
        batch.extend_from_slice(events);
        shared.filled.push_back(batch);
        drop(shared);
        // Outside the lock, and only when the far side is not already on its
        // way: a consumer mid-drain will see this batch without being told.
        if !self.awake.load(Ordering::Acquire)
            && let Some(waker) = self.wake.get()
        {
            let now = self.started.elapsed().as_nanos() as u64;
            let last = self.last_wake.load(Ordering::Relaxed);
            // Reading the clock is tens of nanoseconds against a syscall's
            // thousands, and the exchange only settles the newer time when it
            // is the thread that will actually ring.
            if now.saturating_sub(last) >= WAKE_EVERY.as_nanos() as u64
                && self
                    .last_wake
                    .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            {
                let _ = waker.wake();
            }
        }
        true
    }

    /// Whether anything is waiting to be taken.
    ///
    /// Checked by the consumer after [`Self::about_to_wait`] and before it
    /// waits, which is what closes the window between the two: a batch offered
    /// while the flag still said awake was never rung for, and the drain that
    /// would have found it has already been and gone. Either this sees it, or
    /// the offer happens after the flag is clear and rings.
    ///
    /// The lock is what makes that airtight rather than nearly so. Two threads
    /// each writing one flag and reading the other is the store-buffer shape,
    /// which acquire and release alone do not settle. Taking the mutex here puts
    /// this check and the offer's push into one order: either the push came
    /// first and is seen, or this came first, and then clearing the flag
    /// happened before the offer's read of it and the offer rings.
    pub fn pending(&self) -> bool {
        self.shared
            .lock()
            .is_ok_and(|shared| !shared.filled.is_empty())
    }

    /// Takes everything waiting, oldest first. Empty when there is nothing.
    pub fn take(&self, out: &mut Vec<Vec<Event>>) {
        let Ok(mut shared) = self.shared.lock() else {
            return;
        };
        out.extend(shared.filled.drain(..));
    }

    /// Returns a drained buffer to be filled again.
    pub fn recycle(&self, batch: Vec<Event>) {
        if let Ok(mut shared) = self.shared.lock()
            && shared.spare.len() < IN_FLIGHT
        {
            shared.spare.push(batch);
        }
    }

    /// Batches dropped, and the events inside them.
    #[must_use]
    pub fn dropped(&self) -> (u64, u64) {
        self.shared
            .lock()
            .map_or((0, 0), |shared| (shared.dropped, shared.events_dropped))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn events(count: usize) -> Vec<Event> {
        (0..count)
            .map(|i| Event {
                sequence: i as u64,
                ..Event::default()
            })
            .collect()
    }

    fn drain(handoff: &Handoff) -> Vec<Vec<Event>> {
        let mut out = Vec::new();
        handoff.take(&mut out);
        out
    }

    #[test]
    fn a_batch_arrives_whole_and_in_order() {
        let handoff = Handoff::new();
        assert!(handoff.offer(&events(3)));
        assert!(handoff.offer(&events(2)));
        let taken = drain(&handoff);
        assert_eq!(taken.len(), 2, "batches were merged or lost");
        assert_eq!(taken[0].len(), 3, "the oldest batch did not come first");
        assert_eq!(taken[1].len(), 2);
        assert_eq!(
            taken[0][2].sequence, 2,
            "events were reordered inside a batch"
        );
    }

    #[test]
    fn a_batch_offered_while_the_consumer_looked_awake_is_still_found() {
        let handoff = Handoff::new();
        // The consumer's last drain has been and gone, and it has not yet said
        // it is about to wait -- so this offer sees an awake consumer and does
        // not ring. Without the check after the flag, this batch waits out the
        // idle timer on a thread that had already stopped looking.
        handoff.awake();
        assert!(handoff.offer(&events(1)));
        handoff.about_to_wait();
        assert!(
            handoff.pending(),
            "a batch offered in the window between the drain and the wait was \
             neither rung for nor seen, so it sits until the timer expires"
        );
    }

    #[test]
    fn a_drained_seam_lets_the_consumer_sleep() {
        let handoff = Handoff::new();
        handoff.offer(&events(1));
        drop(drain(&handoff));
        handoff.about_to_wait();
        assert!(
            !handoff.pending(),
            "an empty seam reported work, which turns every wait into a spin"
        );
    }

    #[test]
    fn nothing_is_offered_for_a_pass_that_published_nothing() {
        let handoff = Handoff::new();
        assert!(handoff.offer(&[]), "an empty pass is not a failure");
        assert!(drain(&handoff).is_empty(), "an empty pass took a buffer");
    }

    #[test]
    fn a_far_side_that_stops_reading_is_dropped_rather_than_queued() {
        // The property the whole seam exists for: a market-data consumer that
        // stops keeping up must cost the venue a counter, not memory and not
        // latency.
        let handoff = Handoff::new();
        for _ in 0..IN_FLIGHT {
            assert!(handoff.offer(&events(1)));
        }
        assert!(
            !handoff.offer(&events(5)),
            "the seam accepted a batch it had no buffer for"
        );
        assert_eq!(
            handoff.dropped(),
            (1, 5),
            "a drop was not counted, so an operator could not see the feed fall behind"
        );
        // And it recovers the moment the far side catches up.
        for batch in drain(&handoff) {
            handoff.recycle(batch);
        }
        assert!(
            handoff.offer(&events(1)),
            "the seam stayed shut after draining"
        );
    }

    #[test]
    fn recycling_does_not_grow_the_pool_past_its_bound() {
        // Returning more than were issued -- which a confused caller could --
        // must not turn the bound into a suggestion.
        let handoff = Handoff::new();
        for _ in 0..IN_FLIGHT * 2 {
            handoff.recycle(Vec::with_capacity(PER_BATCH));
        }
        for _ in 0..IN_FLIGHT {
            assert!(handoff.offer(&events(1)));
        }
        assert!(
            !handoff.offer(&events(1)),
            "the pool grew past IN_FLIGHT, so the memory bound is not a bound"
        );
    }

    #[test]
    fn the_venue_keeps_trading_when_the_far_side_panics_holding_the_lock() {
        // A market-data thread must never be able to take the book with it.
        let handoff = Handoff::new();
        let poisoned = handoff.clone();
        let _ = std::thread::spawn(move || {
            let _held = poisoned.shared.lock().unwrap();
            panic!("the publisher died mid-batch");
        })
        .join();
        assert!(
            !handoff.offer(&events(1)),
            "a poisoned seam claimed success"
        );
        assert!(
            drain(&handoff).is_empty(),
            "a poisoned seam handed back batches it could not have read"
        );
        // The point: offering returned, rather than panicking or blocking.
    }
}

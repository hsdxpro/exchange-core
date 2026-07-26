//! Bounding how fast one account may send.
//!
//! The venue already sheds a client it cannot write *to*: the outbox budget
//! catches a session that stopped reading. Nothing caught the opposite — a
//! session that writes too much — and the two are different failures. A flooding
//! client is not slow; it is fast, and it crowds out every other client by
//! filling the group that the pass commits.
//!
//! A token bucket, because the thing worth allowing is a burst on top of a
//! sustained rate: a market maker requoting a whole book after a print sends
//! hundreds of orders in one breath and then goes quiet, and a limiter that
//! smoothed that to a flat per-second rate would break the legitimate case while
//! barely inconveniencing a flood.
//!
//! ## Why the clock is read once a pass
//!
//! The obvious implementation reads the clock per command. That is 20-25 ns
//! against an order path measured at 157 ns — a sixth of the venue's cost, spent
//! on a number that barely changes between two commands out of the same buffer.
//!
//! So the pass reads the clock once and every bucket refills against that one
//! reading. A client sending a thousand orders in a single write pays for one
//! `Instant::now()`, not a thousand, and what each command costs is a compare
//! and a decrement.
//!
//! Refilling is exact in integer arithmetic rather than approximate in floating
//! point: the remainder is carried in nanoseconds, so a bucket polled far faster
//! than its own token interval still earns tokens at the right rate instead of
//! truncating each refill to zero and never filling at all.
//!
//! This sits in the gateway, ahead of the sequencer, so no clock reading here
//! can reach the deterministic path. A rate-limited command is discarded before
//! it is sequenced, which is what keeps replay reproducible: the journal holds
//! what was accepted, and re-running it never has to ask what time it was.

use std::time::{Duration, Instant};

const NANOS_PER_SEC: u64 = 1_000_000_000;

/// How fast an account may send, and how much it may bank by staying quiet.
#[derive(Clone, Copy, Debug)]
pub struct RateLimit {
    /// Nanoseconds of elapsed time that earn one command.
    nanos_per_token: u64,
    /// Ceiling on banked commands: the size of burst allowed after a quiet spell.
    burst: u32,
}

impl RateLimit {
    /// # Panics
    /// If either argument is zero. A rate of zero would refuse every command and
    /// a burst of zero would refuse every first command, and both are far more
    /// likely to be a mistyped configuration than an intent.
    #[must_use]
    pub fn new(per_second: u32, burst: u32) -> Self {
        assert!(per_second > 0, "a rate limit of zero refuses everything");
        assert!(burst > 0, "a burst of zero refuses every first command");
        Self {
            // At most one token per nanosecond, which is far past any real rate
            // and keeps the divisor non-zero.
            nanos_per_token: (NANOS_PER_SEC / u64::from(per_second)).max(1),
            burst,
        }
    }

    /// Commands a second this allows in the steady state.
    #[must_use]
    pub const fn per_second(&self) -> u64 {
        NANOS_PER_SEC / self.nanos_per_token
    }

    #[must_use]
    pub const fn burst(&self) -> u32 {
        self.burst
    }
}

/// One account's allowance, as it stands.
///
/// Starts full, so a client that connects and immediately sends its opening
/// quotes is not punished for having just arrived.
#[derive(Clone, Copy, Debug)]
pub struct Bucket {
    tokens: u32,
    /// Elapsed time not yet worth a whole token. Carrying it is what makes the
    /// refill exact under frequent polling.
    credit_nanos: u64,
    last: Instant,
}

impl Bucket {
    #[must_use]
    pub fn new(limit: RateLimit, now: Instant) -> Self {
        Self {
            tokens: limit.burst,
            credit_nanos: 0,
            last: now,
        }
    }

    /// Adds whatever the time since the last refill has earned.
    ///
    /// Called once per pass for a session that spoke, not once per command.
    pub fn refill(&mut self, limit: RateLimit, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last);
        self.last = now;
        if self.tokens >= limit.burst {
            // Already full, so the elapsed time earns nothing and carrying it
            // would let an idle session bank an unbounded burst.
            self.credit_nanos = 0;
            return;
        }
        self.credit_nanos = self
            .credit_nanos
            .saturating_add(nanos(elapsed))
            .min(limit.nanos_per_token.saturating_mul(u64::from(limit.burst)));
        let earned = self.credit_nanos / limit.nanos_per_token;
        self.credit_nanos %= limit.nanos_per_token;
        self.tokens = u32::try_from(u64::from(self.tokens).saturating_add(earned))
            .unwrap_or(u32::MAX)
            .min(limit.burst);
    }

    /// Spends one token, if there is one. The whole per-command cost.
    pub fn take(&mut self) -> bool {
        if self.tokens == 0 {
            return false;
        }
        self.tokens -= 1;
        true
    }

    #[must_use]
    pub const fn available(&self) -> u32 {
        self.tokens
    }
}

/// Saturating, so a process running for centuries cannot wrap the arithmetic.
fn nanos(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bucket_starts_full_so_a_new_client_can_open_its_quotes() {
        let limit = RateLimit::new(1_000, 50);
        let mut bucket = Bucket::new(limit, Instant::now());
        for _ in 0..50 {
            assert!(bucket.take(), "a fresh bucket refused a command");
        }
        assert!(!bucket.take(), "the burst was not bounded");
    }

    #[test]
    fn tokens_come_back_at_the_configured_rate() {
        let limit = RateLimit::new(1_000, 10);
        let start = Instant::now();
        let mut bucket = Bucket::new(limit, start);
        while bucket.take() {}

        // A hundredth of a second at a thousand a second is ten commands, which
        // is also the ceiling.
        bucket.refill(limit, start + Duration::from_millis(10));
        assert_eq!(bucket.available(), 10);
    }

    #[test]
    fn refilling_faster_than_one_token_still_earns_at_the_right_rate() {
        // The bug this pins: dividing elapsed time by the token interval and
        // discarding the remainder means a bucket polled every microsecond at a
        // thousand a second earns zero every time and never refills at all.
        let limit = RateLimit::new(1_000, 100);
        let start = Instant::now();
        let mut bucket = Bucket::new(limit, start);
        while bucket.take() {}

        // A token is worth a millisecond at this rate, so each of these steps is
        // a thousandth of one. Truncating instead of carrying the remainder
        // would earn nothing here, forever.
        for step in 1..=100_000_u64 {
            bucket.refill(limit, start + Duration::from_micros(step));
        }
        // A tenth of a second at a thousand a second is a hundred commands,
        // which is also the ceiling.
        assert_eq!(bucket.available(), 100);
    }

    #[test]
    fn an_idle_bucket_does_not_bank_an_unbounded_burst() {
        let limit = RateLimit::new(1_000, 10);
        let start = Instant::now();
        let mut bucket = Bucket::new(limit, start);
        bucket.refill(limit, start + Duration::from_secs(3_600));
        assert_eq!(bucket.available(), 10, "an hour of silence became a flood");
    }

    #[test]
    fn a_sustained_sender_is_held_to_the_rate() {
        let limit = RateLimit::new(1_000, 10);
        let start = Instant::now();
        let mut bucket = Bucket::new(limit, start);
        while bucket.take() {}

        // One second, asking as fast as possible the whole way.
        let mut sent = 0;
        for micro in 1..=1_000_000_u64 {
            bucket.refill(limit, start + Duration::from_micros(micro));
            if bucket.take() {
                sent += 1;
            }
        }
        // A thousand a second, give or take the token that is mid-refill.
        assert!(
            (999..=1_001).contains(&sent),
            "a second of flooding got through {sent} commands at a limit of 1,000/sec"
        );
    }

    #[test]
    fn the_configured_rate_is_reported_back() {
        let limit = RateLimit::new(50_000, 1_000);
        assert_eq!(limit.per_second(), 50_000);
        assert_eq!(limit.burst(), 1_000);
    }
}

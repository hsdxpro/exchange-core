//! What the venue is doing, without changing what it does.
//!
//! A venue that cannot say what its p99 is cannot be operated: "it feels slow"
//! is not a bug report, and an average hides exactly the tail that matters.
//!
//! Three rules shape this, and they are the reason it is not simply a timer
//! around everything.
//!
//! **Nothing here allocates.** Histograms are fixed arrays, counters are `u64`
//! fields, and reading them out builds the only `String` involved. A metric that
//! allocates under load stops being a measurement and becomes a cause.
//!
//! **No clock reading reaches the deterministic path.** Timing happens in the
//! gateway around the commit, never inside the pipeline, so replay never has to
//! ask what time it was. This is the same rule that puts authentication and rate
//! limiting ahead of the sequencer.
//!
//! **Timings are sampled; counts are not.** `Instant::now()` is 20–25 ns, and a
//! pass that commits a group of one costs about 190 ns, so timing every pass
//! would spend a fifth of the venue's budget measuring itself. Every 64th pass
//! is timed instead, which puts the cost under a nanosecond amortised and still
//! collects thousands of samples a second under any load worth measuring.
//! Counters are increments on paths that are already branching, so those are
//! exact.
//!
//! The histogram is log-linear: a power-of-two bucket split into thirty-two
//! linear steps, so the worst relative error is one part in thirty-two, about
//! 3%. Recording is a leading-zero count, a shift and an increment — no branches
//! on the value, no division.

use std::time::Duration;

/// Power-of-two buckets, each split into this many linear steps.
const STEPS: usize = 32;
const STEP_BITS: u32 = 5;
const BUCKETS: usize = 64 * STEPS;

/// Passes between timed samples. A power of two so the test is a mask.
const SAMPLE_EVERY: u64 = 64;

/// A fixed-bucket distribution.
///
/// 16 KiB, no allocation, and recording is a handful of instructions. Percentiles
/// are computed when read, which happens off the hot path by definition.
#[derive(Clone, Debug)]
pub struct Histogram {
    buckets: [u64; BUCKETS],
    count: u64,
    max: u64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: [0; BUCKETS],
            count: 0,
            max: 0,
        }
    }
}

impl Histogram {
    /// Which bucket a value falls in: its magnitude, then four linear steps
    /// within that magnitude.
    const fn bucket(value: u64) -> usize {
        // Values below the first full magnitude get their own exact buckets,
        // which matters because a great many measurements here are small.
        if value < STEPS as u64 {
            return value as usize;
        }
        let magnitude = 63 - value.leading_zeros();
        let step = (value >> (magnitude - STEP_BITS)) & (STEPS as u64 - 1);
        (magnitude as usize) * STEPS + step as usize
    }

    /// Lowest value that lands in `index`. Used to report, not to record.
    const fn floor(index: usize) -> u64 {
        if index < STEPS {
            return index as u64;
        }
        let magnitude = (index / STEPS) as u32;
        let step = (index % STEPS) as u64;
        (1_u64 << magnitude) + (step << (magnitude - STEP_BITS))
    }

    pub const fn record(&mut self, value: u64) {
        let index = Self::bucket(value);
        self.buckets[index] += 1;
        self.count += 1;
        if value > self.max {
            self.max = value;
        }
    }

    #[must_use]
    pub const fn count(&self) -> u64 {
        self.count
    }

    #[must_use]
    pub const fn max(&self) -> u64 {
        self.max
    }

    /// The value at `fraction` of the distribution, 0.0 to 1.0.
    ///
    /// Reports the bucket's lower bound, so a quoted figure is one the venue
    /// genuinely reached rather than one rounded up into.
    #[must_use]
    pub fn percentile(&self, fraction: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let target = (self.count as f64 * fraction).ceil() as u64;
        let mut seen = 0;
        for (index, held) in self.buckets.iter().enumerate() {
            seen += held;
            if seen >= target {
                return Self::floor(index);
            }
        }
        self.max
    }
}

/// Everything the venue counts about itself.
///
/// Read by whatever is watching — a log line, a test, an operator's terminal.
/// Nothing here is pushed anywhere: a venue that blocks on a metrics endpoint
/// has made observability into an outage.
#[derive(Clone, Debug, Default)]
pub struct Metrics {
    passes: u64,
    /// Commands accepted into a group, over the venue's life.
    commands: u64,
    /// Passes that had something to commit.
    groups: u64,
    /// Commands in one group. Exact, not sampled: it is an integer the pass
    /// already has in hand.
    group_size: Histogram,
    /// Nanoseconds to apply and commit one group. Sampled.
    commit_ns: Histogram,
    /// Nanoseconds for one whole pass, including reading and writing sockets.
    /// Sampled.
    pass_ns: Histogram,
    sessions_accepted: u64,
    sessions_refused: u64,
    /// Sessions dropped for owing more than they are allowed to queue.
    sessions_shed: u64,
}

impl Metrics {
    /// True when this pass should be timed. Every 64th, so the clock reading
    /// costs under a nanosecond a pass amortised.
    #[must_use]
    pub const fn sampling(&self) -> bool {
        self.passes.is_multiple_of(SAMPLE_EVERY)
    }

    pub const fn pass(&mut self, commands: usize) {
        self.passes += 1;
        if commands > 0 {
            self.groups += 1;
            self.commands += commands as u64;
            self.group_size.record(commands as u64);
        }
    }

    pub const fn commit_took(&mut self, elapsed: Duration) {
        self.commit_ns.record(elapsed.as_nanos() as u64);
    }

    pub const fn pass_took(&mut self, elapsed: Duration) {
        self.pass_ns.record(elapsed.as_nanos() as u64);
    }

    pub const fn accepted(&mut self) {
        self.sessions_accepted += 1;
    }

    pub const fn refused(&mut self) {
        self.sessions_refused += 1;
    }

    pub const fn shed(&mut self) {
        self.sessions_shed += 1;
    }

    #[must_use]
    pub const fn commands(&self) -> u64 {
        self.commands
    }

    #[must_use]
    pub const fn passes(&self) -> u64 {
        self.passes
    }

    #[must_use]
    pub const fn groups(&self) -> u64 {
        self.groups
    }

    #[must_use]
    pub const fn group_size(&self) -> &Histogram {
        &self.group_size
    }

    #[must_use]
    pub const fn commit_ns(&self) -> &Histogram {
        &self.commit_ns
    }

    #[must_use]
    pub const fn pass_ns(&self) -> &Histogram {
        &self.pass_ns
    }

    #[must_use]
    pub const fn sessions_accepted(&self) -> u64 {
        self.sessions_accepted
    }

    #[must_use]
    pub const fn sessions_refused(&self) -> u64 {
        self.sessions_refused
    }

    #[must_use]
    pub const fn sessions_shed(&self) -> u64 {
        self.sessions_shed
    }

    /// One block an operator can read, or a log can carry.
    #[must_use]
    pub fn report(&self) -> String {
        use std::fmt::Write;
        let mut out = String::with_capacity(512);
        let _ = writeln!(
            out,
            "commands {} in {} groups over {} passes",
            self.commands, self.groups, self.passes
        );
        let _ = writeln!(
            out,
            "  group size    median {:>8}  p99 {:>8}  max {:>8}",
            self.group_size.percentile(0.5),
            self.group_size.percentile(0.99),
            self.group_size.max()
        );
        for (name, held) in [("commit ns", &self.commit_ns), ("pass ns", &self.pass_ns)] {
            let _ = writeln!(
                out,
                "  {name:<12}  median {:>8}  p99 {:>8}  max {:>8}   ({} sampled)",
                held.percentile(0.5),
                held.percentile(0.99),
                held.max(),
                held.count()
            );
        }
        let _ = write!(
            out,
            "  sessions      accepted {}  refused {}  shed {}",
            self.sessions_accepted, self.sessions_refused, self.sessions_shed
        );
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_values_land_in_their_own_buckets() {
        // Most of what is measured here is small -- a group of one, a pass of a
        // few hundred nanoseconds -- so the bottom of the range must not be
        // lumped together.
        for value in 0..4 {
            assert_eq!(Histogram::bucket(value), value as usize);
            assert_eq!(Histogram::floor(Histogram::bucket(value)), value);
        }
    }

    #[test]
    fn a_bucket_never_claims_a_value_above_the_one_recorded() {
        // The reported figure is the bucket's floor, so a quoted p99 is a number
        // the venue actually reached rather than one rounded up into.
        for value in [4_u64, 5, 7, 8, 100, 1_000, 12_345, 1 << 40, u64::MAX] {
            let floor = Histogram::floor(Histogram::bucket(value));
            assert!(
                floor <= value,
                "{value} lands in a bucket whose floor is {floor}"
            );
        }
    }

    #[test]
    fn buckets_are_monotonic_in_the_value() {
        let mut last = 0;
        for value in 0..100_000_u64 {
            let bucket = Histogram::bucket(value);
            assert!(bucket >= last, "bucket went backwards at {value}");
            assert!(bucket < BUCKETS, "{value} escaped the array");
            last = bucket;
        }
    }

    #[test]
    fn the_error_stays_inside_the_promised_band() {
        // Thirty-two steps per power of two, so a value is never reported as
        // more than one part in thirty-two below what it was.
        for value in 1..200_000_u64 {
            let floor = Histogram::floor(Histogram::bucket(value));
            let error = (value - floor) as f64 / value as f64;
            assert!(error <= 0.032, "{value} -> {floor} is {error} off");
        }
    }

    #[test]
    fn percentiles_come_back_in_the_right_order() {
        let mut held = Histogram::default();
        for value in 1..=1_000 {
            held.record(value);
        }
        assert_eq!(held.count(), 1_000);
        assert_eq!(held.max(), 1_000);
        assert!(held.percentile(0.5) <= held.percentile(0.99));
        assert!(held.percentile(0.99) <= held.max());
        // The median of 1..=1000 is 500, within a bucket's width of it.
        let median = held.percentile(0.5);
        assert!(
            (448..=512).contains(&median),
            "median came back as {median}"
        );
    }

    #[test]
    fn an_empty_histogram_reports_zero_rather_than_dividing_by_it() {
        let held = Histogram::default();
        assert_eq!(held.percentile(0.99), 0);
        assert_eq!(held.count(), 0);
        assert_eq!(held.max(), 0);
    }

    #[test]
    fn only_every_sixty_fourth_pass_is_sampled() {
        // The whole reason timings are affordable. If this became "every pass",
        // a clock reading would land on a 190 ns path.
        let mut metrics = Metrics::default();
        let mut sampled = 0;
        for _ in 0..640 {
            if metrics.sampling() {
                sampled += 1;
            }
            metrics.pass(1);
        }
        assert_eq!(sampled, 10);
    }

    #[test]
    fn a_pass_with_nothing_in_it_is_not_counted_as_a_group() {
        // An idle venue polls constantly. Counting those as groups of zero would
        // drag every percentile to the floor and hide real load.
        let mut metrics = Metrics::default();
        for _ in 0..100 {
            metrics.pass(0);
        }
        metrics.pass(7);
        assert_eq!(metrics.passes(), 101);
        assert_eq!(metrics.groups(), 1);
        assert_eq!(metrics.commands(), 7);
        assert_eq!(metrics.group_size().count(), 1);
    }

    #[test]
    fn the_report_names_what_it_measured() {
        let mut metrics = Metrics::default();
        metrics.pass(64);
        metrics.commit_took(Duration::from_nanos(1_500));
        metrics.accepted();
        let report = metrics.report();
        assert!(report.contains("commands 64"), "{report}");
        assert!(report.contains("group size"), "{report}");
        assert!(report.contains("accepted 1"), "{report}");
    }
}

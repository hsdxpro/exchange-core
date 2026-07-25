# Measurement method

The binary verifies itself and then benchmarks itself, so the honest way to get numbers
is to run it on the machine you care about:

```bash
cargo run --release --bin bx-bench
```

It prints all 43 named check groups with a pass/fail total, then p50, p99, p99.9 and a
per-item figure for each scenario, with a one-line description of the work each scenario
performs. A failed check aborts the run before any timing is reported.

## What the numbers are

**Batch-normalized throughput-equivalent service times.** Operations run in batches, the
batch is timed once, and the duration is divided by the operation count. Percentiles are
computed across those normalized batch samples.

They are therefore *not*:

- per-operation timestamp samples, so the percentiles are not message tail latency;
- end-to-end exchange latency. Gateway parsing, sequencing, risk, journaling, network and
  queueing all sit outside this engine;
- representative of a book larger than cache. Every scenario here fits in cache. At
  production book depth a direct-ID cancel chases a pointer into a working set that does
  not, and costs roughly an order of magnitude more. See the note at the end.

## How the harness works

- State construction and reset happen outside every timed region.
- Batching amortizes the clock call, which would otherwise dominate a 5 ns operation.
- A data-dependent hash sink is accumulated and printed, so nothing is optimized away.
- The default run repeats three times; `--quick` reduces both the workload and the
  repetitions.
- Sparse scenarios spread live prices across most of the 65,536-tick domain, so traversal
  cannot benefit from adjacency.

## Reading the results honestly

On an unpinned desktop this measurement is noisy. Running the *same binary* repeatedly,
p50 varies by a factor of two to four depending on machine load, and the p99.9 column is
dominated by scheduler preemption, not by anything the engine does. A p99.9 of 300 µs
against a p50 of 5 µs is one preemption inside one batch.

Two consequences:

1. **Quote the per-item column, not the raw p50.** It is a ratio computed inside a single
   run, so it is far more stable than an absolute time, and it is the figure that actually
   expresses the design claim.
2. **Take the minimum across runs.** Contention only ever adds time, so the smallest
   observation is the best estimate of the uncontended cost. The table in the README is a
   minimum of three runs.

If you need numbers you can defend, run on an isolated core with frequency locked, turbo
and C-states off, and use `perf stat` counters instead of wall clock. Cache misses and
retired instructions are counts, not durations, so scheduler noise barely touches them.

## What to look for

The result worth checking is the shape, not the absolute values:

- Per-level traversal cost is flat whether 10 levels or 1,000 levels are occupied, and
  whether those levels are adjacent or spread across the full price domain. That is the
  bitmap doing its job: empty ticks are never visited.
- Per-fill cost is flat from one fill to 64 fills at the same price.
- A 1,000-price sweep pays extra queue-unlink, bitmap-transition and fill-report work per
  fill, and still never scans the empty ticks in between.

## A note on scale

The benchmark books fit in cache. A separate experiment with 400,000 live orders, about
9.6 MB of order slots and well beyond L2, measured random-order cancel at roughly
75–90 ns per operation instead of the ~9 ns reported here.

That is not a defect. It is what pointer-chasing costs once the working set leaves cache,
and it applies to any order book of that shape. It does mean the figures in the README are
a cache-resident best case, and a real venue running deep books should expect the
memory-bound regime.

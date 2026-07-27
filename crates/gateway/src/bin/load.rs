//! A client, as a process. Measures what a client actually experiences.
//!
//! Two numbers, because they answer different questions:
//!
//! - **Round trip** is one order sent, then its event read back, with nothing
//!   else in flight. It is what a latency-sensitive client feels, and it
//!   includes both socket traversals, the venue's poll interval, and a commit
//!   covering a group of one. Reported as the minimum, because contention only
//!   ever adds.
//! - **Throughput** is a pipeline: push orders as fast as the socket takes
//!   them, on the assumption a real venue has many clients doing exactly that.
//!   The group grows on its own under this load, which is the behaviour worth
//!   measuring.
//!
//! - **Concurrent throughput** is many clients at once, which is what a venue
//!   actually faces and the only shape where the group grows on its own: the
//!   group is whatever arrived since the last pass, so more clients means larger
//!   groups and a sync amortised further. One pipelined client understates it.
//!
//! - **An audience** is `--subscribers N`: sessions that follow the book
//!   channel for the whole run and count what they receive. Every top-of-book
//!   change fans out to all of them, so this is what prices the venue's
//!   outbound side -- and what proves a slow or huge audience degrades into
//!   shed sessions rather than unbounded memory.
//!
//! ```text
//! load [address] [--orders N] [--clients N] [--subscribers N]
//! ```

use bx_gateway::codec::{FRAME_LEN, encode};
use bx_pipeline::{limit_order, subscribe};
use bx_protocol::{ChannelKind, Command, Event, EventKind, RejectReason, Side, Ticks};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};
use zerocopy::FromBytes;

const SYMBOL: u32 = 1;
const FLOOR: Ticks = 10_000;
const ACCOUNT: u64 = 1;
/// Round-trip samples. Each is one order and one blocking read.
const PROBES: usize = 2_000;
/// Accounts the venue funds at startup. One per connection, so a client never
/// shares a private feed and can count its own answers exactly.
const ACCOUNTS: u64 = 256;

/// What came back for the commands sent.
///
/// Split because "how many were acknowledged" and "how many got an answer" are
/// different questions, and conflating them is what made this tool hang. Every
/// command produces exactly one of these two events: an acknowledgement if it
/// reached the sequencer, a rejection if the gateway refused it first -- which
/// is what a rate limit does. Waiting only for acknowledgements therefore waits
/// forever the moment a single order is refused, and the failure surfaces ten
/// seconds later as a socket timeout that names nothing.
#[derive(Clone, Copy, Debug, Default)]
struct Responses {
    acknowledged: u64,
    rejected: u64,
    /// Rejections tallied by reason discriminant, so the warning at the end
    /// says *why* instead of guessing. A run refused for duplicate order IDs --
    /// which is what running this tool twice against one venue produces, since
    /// the first run's orders are still resting under the same IDs -- needs
    /// different advice than a run refused by the rate limit, and only the
    /// venue knows which happened.
    reasons: [u64; 32],
}

impl Responses {
    /// Commands the venue has answered, one way or the other.
    const fn answered(&self) -> u64 {
        self.acknowledged + self.rejected
    }

    fn count(&mut self, event: &Event) {
        if event.kind == EventKind::Received as u8 {
            self.acknowledged += 1;
        } else if event.kind == EventKind::Rejected as u8 {
            self.rejected += 1;
            let slot = (event.reject_reason as usize).min(self.reasons.len() - 1);
            self.reasons[slot] += 1;
        }
    }

    fn absorb(&mut self, other: &Self) {
        self.acknowledged += other.acknowledged;
        self.rejected += other.rejected;
        for (mine, theirs) in self.reasons.iter_mut().zip(other.reasons.iter()) {
            *mine += theirs;
        }
    }

    /// The reason that refused the most commands, with its count.
    fn dominant_reason(&self) -> Option<(RejectReason, u64)> {
        let (index, count) = self
            .reasons
            .iter()
            .enumerate()
            .max_by_key(|(_, count)| **count)?;
        if *count == 0 {
            return None;
        }
        let probe = Event {
            reject_reason: index as u8,
            ..Event::default()
        };
        probe.reject_reason().map(|reason| (reason, *count))
    }
}

/// Whether an error is the peer going away rather than a fault worth reporting.
fn is_disconnect(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::UnexpectedEof
    )
}

fn connect(address: &str) -> std::io::Result<TcpStream> {
    let stream = TcpStream::connect(address)?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    Ok(stream)
}

/// Reads until the venue answers the order just sent, returning what it said.
///
/// Answered means acknowledged or rejected, not "some bytes arrived". Counting
/// any frame as a reply is how this tool reported a healthy 10 microsecond round
/// trip against a venue that required authentication and was in fact sending
/// nothing but a challenge: the number was the client reading a refusal to talk
/// to it, timed to the microsecond and completely meaningless.
fn await_answer(
    stream: &mut TcpStream,
    scratch: &mut [u8],
    held: &mut usize,
) -> std::io::Result<EventKind> {
    loop {
        let bytes = stream.read(&mut scratch[*held..])?;
        if bytes == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "the venue closed the connection without answering",
            ));
        }
        let filled = *held + bytes;
        let whole = filled / FRAME_LEN;
        let mut answer = None;
        for index in 0..whole {
            let start = index * FRAME_LEN;
            if let Ok(event) = Event::read_from_bytes(&scratch[start..start + FRAME_LEN]) {
                if event.kind == EventKind::Received as u8 {
                    answer = Some(EventKind::Received);
                } else if event.kind == EventKind::Rejected as u8 && answer.is_none() {
                    answer = Some(EventKind::Rejected);
                } else if event.kind == EventKind::Challenge as u8 {
                    return Err(std::io::Error::other(
                        "the venue requires authentication and this tool cannot \
                         authenticate; set `authentication = open` in the venue's \
                         configuration to measure it",
                    ));
                }
            }
        }
        scratch.copy_within(whole * FRAME_LEN..filled, 0);
        *held = filled - whole * FRAME_LEN;
        if let Some(kind) = answer {
            return Ok(kind);
        }
    }
}

fn round_trip(address: &str, first_order: u64) -> std::io::Result<(f64, f64)> {
    let mut stream = connect(address)?;
    let mut scratch = vec![0_u8; FRAME_LEN * 64];
    let mut held = 0_usize;
    let mut bytes = Vec::with_capacity(FRAME_LEN);
    let mut samples = Vec::with_capacity(PROBES);
    let mut rejected = 0_u64;

    for probe in 0..PROBES {
        let order_id = first_order + probe as u64;
        // Prices step so orders rest rather than crossing each other.
        let price = FLOOR + 1_000 + (probe % 4_000) as Ticks;
        let command = limit_order(ACCOUNT, SYMBOL, order_id, Side::Bid, price, 1);

        bytes.clear();
        encode(&command, &mut bytes);

        let started = Instant::now();
        stream.write_all(&bytes)?;
        if await_answer(&mut stream, &mut scratch, &mut held)? == EventKind::Rejected {
            rejected += 1;
        }
        samples.push(started.elapsed().as_secs_f64() * 1e6);
    }

    if rejected > 0 {
        eprintln!(
            "WARNING: {rejected} of {PROBES} round-trip probes were rejected, so this \
             latency is the cost of being refused rather than of being matched."
        );
    }
    samples.sort_by(f64::total_cmp);
    Ok((samples[0], samples[samples.len() / 2]))
}

fn throughput(address: &str, orders: u64, first_order: u64) -> std::io::Result<(f64, Responses)> {
    let mut stream = connect(address)?;
    let mut reader = stream.try_clone()?;

    let commands: Vec<Command> = (0..orders)
        .map(|i| {
            let price = FLOOR + 1_000 + (i % 4_000) as Ticks;
            limit_order(ACCOUNT, SYMBOL, first_order + i, Side::Bid, price, 1)
        })
        .collect();
    let mut bytes = Vec::with_capacity(commands.len() * FRAME_LEN);
    for command in &commands {
        encode(command, &mut bytes);
    }

    // Drain on another thread, so the writer is never blocked by the venue's
    // replies filling the socket buffer -- which would measure the test, not
    // the venue.
    let wanted = orders;
    let drain = std::thread::spawn(move || -> std::io::Result<Responses> {
        let mut scratch = vec![0_u8; FRAME_LEN * 1_024];
        let mut tally = Responses::default();
        let mut partial = 0_usize;
        while tally.answered() < wanted {
            let bytes = match reader.read(&mut scratch[partial..]) {
                Ok(0) => break,
                Ok(bytes) => bytes,
                // The venue sheds a session that owes more than it may queue,
                // and a shed session is a closed socket. That is the venue
                // working as designed under a client that will not read fast
                // enough, so it is reported at the end rather than thrown as an
                // I/O error that hides how far the run actually got.
                Err(e) if is_disconnect(&e) => break,
                Err(e) => return Err(e),
            };
            let filled = partial + bytes;
            let whole = filled / FRAME_LEN;
            for index in 0..whole {
                let start = index * FRAME_LEN;
                if let Ok(event) = Event::read_from_bytes(&scratch[start..start + FRAME_LEN]) {
                    tally.count(&event);
                }
            }
            scratch.copy_within(whole * FRAME_LEN..filled, 0);
            partial = filled - whole * FRAME_LEN;
        }
        Ok(tally)
    });

    let started = Instant::now();
    stream.write_all(&bytes)?;
    stream.flush()?;
    let tally = drain.join().expect("reader thread panicked")?;
    let elapsed = started.elapsed().as_secs_f64();

    Ok((orders as f64 / elapsed, tally))
}

/// Many clients at once, each on its own account and its own slice of the
/// ladder so orders rest rather than crossing. Returns aggregate orders a second
/// and how many were acknowledged.
fn concurrent_throughput(
    address: &str,
    clients: u64,
    orders_each: u64,
    first_order: u64,
) -> std::io::Result<(f64, Responses)> {
    let ready = std::sync::Arc::new(std::sync::Barrier::new(clients as usize + 1));
    let mut workers = Vec::with_capacity(clients as usize);

    for client in 0..clients {
        let address = address.to_string();
        let gate = std::sync::Arc::clone(&ready);
        workers.push(std::thread::spawn(move || -> std::io::Result<Responses> {
            // Accounts the venue funds at startup. Clients sharing an account is
            // fine here: every order is a resting bid in its own price band, so
            // nothing crosses and nothing self-matches.
            // One account per connection. Sharing accounts put two clients on
            // one private feed, so each counted the other's acknowledgements and
            // a run could report more answers than it sent orders.
            let account = 1 + client % ACCOUNTS;
            let base = FLOOR + 1_000 + (client as Ticks) * 37;
            let mut stream = connect(&address)?;
            let mut reader = stream.try_clone()?;

            let mut bytes = Vec::with_capacity(orders_each as usize * FRAME_LEN);
            for i in 0..orders_each {
                let order = first_order + client * orders_each + i;
                let price = base + (i % 31) as Ticks;
                encode(
                    &limit_order(account, SYMBOL, order, Side::Bid, price, 1),
                    &mut bytes,
                );
            }

            // Read on another thread, or a client blocks against its own receive
            // buffer and measures itself rather than the venue.
            let drain = std::thread::spawn(move || -> std::io::Result<Responses> {
                let mut scratch = vec![0_u8; FRAME_LEN * 256];
                let mut tally = Responses::default();
                let mut partial = 0_usize;
                while tally.answered() < orders_each {
                    let bytes = match reader.read(&mut scratch[partial..]) {
                        Ok(0) => break,
                        Ok(bytes) => bytes,
                        Err(e) if is_disconnect(&e) => break,
                        Err(e) => return Err(e),
                    };
                    let filled = partial + bytes;
                    let whole = filled / FRAME_LEN;
                    for index in 0..whole {
                        let start = index * FRAME_LEN;
                        if let Ok(event) =
                            Event::read_from_bytes(&scratch[start..start + FRAME_LEN])
                        {
                            tally.count(&event);
                        }
                    }
                    scratch.copy_within(whole * FRAME_LEN..filled, 0);
                    partial = filled - whole * FRAME_LEN;
                }
                Ok(tally)
            });

            // Every client starts writing at the same moment, so the venue sees
            // real concurrency rather than a staggered ramp.
            gate.wait();
            stream.write_all(&bytes)?;
            stream.flush()?;
            drain.join().expect("reader thread panicked")
        }));
    }

    ready.wait();
    let started = Instant::now();
    let mut total = Responses::default();
    for worker in workers {
        let tally = worker.join().expect("client thread panicked")?;
        total.absorb(&tally);
    }
    let elapsed = started.elapsed().as_secs_f64();
    Ok(((clients * orders_each) as f64 / elapsed, total))
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let address = args
        .first()
        .filter(|a| !a.starts_with("--"))
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:7070".to_string());
    let number = |name: &str, fallback: u64| -> u64 {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .and_then(|n| n.parse().ok())
            .unwrap_or(fallback)
    };
    let orders = number("--orders", 200_000);
    let clients = number("--clients", 32);
    let audience = number("--subscribers", 0);

    let (watchers, feeds) = subscribers(&address, audience)?;

    println!("\nClient-observed latency against {address}");
    println!("{}", "-".repeat(72));

    let (min, median) = round_trip(&address, 1)?;
    println!(
        "{:<40}{min:>10.1} us{median:>12.1} us",
        "round trip, one order in flight"
    );

    let (per_second, received) = throughput(&address, orders, 10_000_000)?;
    println!(
        "{:<40}{:>10.0} orders/sec",
        "pipelined throughput", per_second
    );
    println!(
        "{:<40}{:>10} of {orders}",
        "orders acknowledged", received.acknowledged
    );

    let each = (orders / clients).max(1);
    let (concurrent, answered) = concurrent_throughput(&address, clients, each, 20_000_000)?;
    println!(
        "{:<40}{concurrent:>10.0} orders/sec",
        format!("{clients} clients at once, {each} each")
    );
    println!(
        "{:<40}{:>10} of {}",
        "orders acknowledged",
        answered.acknowledged,
        clients * each
    );
    println!("{}", "-".repeat(72));

    // A run where orders were refused is not the run it claims to be: a refused
    // order skips matching, so a saturated venue reports a throughput it cannot
    // sustain. This project has measured the reject path by accident three
    // times, every time because nothing said so out loud.
    //
    // Rejections are named separately from silence. They are different faults
    // with different fixes: a rejection means the venue answered and said no,
    // which at these rates is almost always the per-account rate limit, and
    // silence means the run ended before the venue finished answering.
    report("pipelined", received, orders);
    report("concurrent", answered, clients * each);

    if audience > 0 {
        // Let the venue drain its outboxes before the audience hangs up.
        std::thread::sleep(Duration::from_millis(500));
        for feed in &feeds {
            let _ = feed.shutdown(std::net::Shutdown::Both);
        }
        let mut counts: Vec<u64> = watchers
            .into_iter()
            .map(|w| w.join().expect("subscriber thread panicked"))
            .collect();
        counts.sort_unstable();
        let total: u64 = counts.iter().sum();
        println!(
            "{:<40}{:>10} total   min {}   max {}",
            format!("{audience} subscribers, book channel"),
            total,
            counts.first().copied().unwrap_or(0),
            counts.last().copied().unwrap_or(0)
        );
        if counts.first().is_some_and(|&least| least == 0) {
            eprintln!(
                "WARNING: at least one subscriber received nothing. Either the                  run produced no top-of-book changes, or the venue shed it --                  the venue's own counters say which."
            );
        }
    }
    Ok(())
}

/// Connects `count` sessions that subscribe to the book channel and read
/// until the socket closes, each returning how many frames it received.
///
/// Counting is bytes divided by the frame length, because an audience member
/// does not need to decode a feed to weigh it -- and decoding here would put
/// the measuring instrument's cost into the venue's number.
fn subscribers(
    address: &str,
    count: u64,
) -> std::io::Result<(Vec<std::thread::JoinHandle<u64>>, Vec<TcpStream>)> {
    let mut watchers = Vec::with_capacity(count as usize);
    let mut feeds = Vec::with_capacity(count as usize);
    for member in 0..count {
        let stream = connect(address)?;
        let mut reader = stream.try_clone()?;
        let mut frame = Vec::with_capacity(FRAME_LEN);
        encode(
            &subscribe(1 + member % ACCOUNTS, SYMBOL, ChannelKind::Book),
            &mut frame,
        );
        (&stream).write_all(&frame)?;
        feeds.push(stream);
        watchers.push(std::thread::spawn(move || -> u64 {
            let mut scratch = vec![0_u8; 16 * 1024];
            let mut received = 0_u64;
            loop {
                match reader.read(&mut scratch) {
                    Ok(0) => break,
                    Ok(bytes) => received += bytes as u64,
                    Err(_) => break,
                }
            }
            received / FRAME_LEN as u64
        }));
    }
    Ok((watchers, feeds))
}

/// Says what happened to the commands that were not acknowledged.
fn report(phase: &str, tally: Responses, sent: u64) {
    if let Some((reason, count)) = tally.dominant_reason() {
        let advice = match reason {
            RejectReason::RateLimited => {
                " Raise `max_commands_per_second` in the venue's configuration, or \
                 lower --orders."
            }
            RejectReason::DuplicateOrderId => {
                " The usual cause is running this tool twice against one venue: the \
                 first run's orders are still resting under the same IDs. Restart \
                 the venue between runs."
            }
            _ => "",
        };
        eprintln!(
            "WARNING: the venue refused {} of {sent} {phase} orders ({count} of \
             them: {reason}), so this figure is measuring the reject path rather \
             than matching.{advice}",
            tally.rejected
        );
    }
    let unanswered = sent.saturating_sub(tally.answered());
    if unanswered > 0 {
        eprintln!(
            "WARNING: {unanswered} of {sent} {phase} orders were never answered at \
             all. The connection was closed before the venue finished, which is what \
             shedding a session that queued more than it may look like."
        );
    }
}

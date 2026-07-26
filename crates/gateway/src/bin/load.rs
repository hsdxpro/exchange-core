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
//! ```text
//! load [address] [--orders N] [--clients N]
//! ```

use bx_gateway::codec::{FRAME_LEN, encode};
use bx_pipeline::limit_order;
use bx_protocol::{Command, Event, EventKind, Side, Ticks};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};
use zerocopy::FromBytes;

const SYMBOL: u32 = 1;
const FLOOR: Ticks = 10_000;
const ACCOUNT: u64 = 1;
/// Round-trip samples. Each is one order and one blocking read.
const PROBES: usize = 2_000;

fn connect(address: &str) -> std::io::Result<TcpStream> {
    let stream = TcpStream::connect(address)?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    Ok(stream)
}

/// Reads until at least one whole event arrives, returning how many.
fn read_some(stream: &mut TcpStream, scratch: &mut [u8]) -> std::io::Result<usize> {
    let bytes = stream.read(scratch)?;
    Ok(bytes / FRAME_LEN)
}

fn round_trip(address: &str, first_order: u64) -> std::io::Result<(f64, f64)> {
    let mut stream = connect(address)?;
    let mut scratch = vec![0_u8; FRAME_LEN * 64];
    let mut bytes = Vec::with_capacity(FRAME_LEN);
    let mut samples = Vec::with_capacity(PROBES);

    for probe in 0..PROBES {
        let order_id = first_order + probe as u64;
        // Prices step so orders rest rather than crossing each other.
        let price = FLOOR + 1_000 + (probe % 4_000) as Ticks;
        let command = limit_order(ACCOUNT, SYMBOL, order_id, Side::Bid, price, 1);

        bytes.clear();
        encode(&command, &mut bytes);

        let started = Instant::now();
        stream.write_all(&bytes)?;
        let mut seen = 0;
        while seen == 0 {
            seen = read_some(&mut stream, &mut scratch)?;
        }
        samples.push(started.elapsed().as_secs_f64() * 1e6);
    }

    samples.sort_by(f64::total_cmp);
    Ok((samples[0], samples[samples.len() / 2]))
}

fn throughput(address: &str, orders: u64, first_order: u64) -> std::io::Result<(f64, u64)> {
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
    let drain = std::thread::spawn(move || -> std::io::Result<u64> {
        let mut scratch = vec![0_u8; FRAME_LEN * 1_024];
        let mut seen = 0_u64;
        let mut partial = 0_usize;
        while seen < wanted {
            let bytes = reader.read(&mut scratch[partial..])?;
            if bytes == 0 {
                break;
            }
            let filled = partial + bytes;
            let whole = filled / FRAME_LEN;
            for index in 0..whole {
                let start = index * FRAME_LEN;
                // Count acknowledgements only. Every command produces exactly
                // one, where the total event count also includes depth updates
                // and prints and would finish the measurement early.
                if let Ok(event) = Event::read_from_bytes(&scratch[start..start + FRAME_LEN])
                    && event.kind == EventKind::Received as u8
                {
                    seen += 1;
                }
            }
            scratch.copy_within(whole * FRAME_LEN..filled, 0);
            partial = filled - whole * FRAME_LEN;
        }
        Ok(seen)
    });

    let started = Instant::now();
    stream.write_all(&bytes)?;
    stream.flush()?;
    let received = drain.join().expect("reader thread panicked")?;
    let elapsed = started.elapsed().as_secs_f64();

    Ok((orders as f64 / elapsed, received))
}

/// Many clients at once, each on its own account and its own slice of the
/// ladder so orders rest rather than crossing. Returns aggregate orders a second
/// and how many were acknowledged.
fn concurrent_throughput(
    address: &str,
    clients: u64,
    orders_each: u64,
    first_order: u64,
) -> std::io::Result<(f64, u64)> {
    let ready = std::sync::Arc::new(std::sync::Barrier::new(clients as usize + 1));
    let mut workers = Vec::with_capacity(clients as usize);

    for client in 0..clients {
        let address = address.to_string();
        let gate = std::sync::Arc::clone(&ready);
        workers.push(std::thread::spawn(move || -> std::io::Result<u64> {
            // Accounts the venue funds at startup. Clients sharing an account is
            // fine here: every order is a resting bid in its own price band, so
            // nothing crosses and nothing self-matches.
            let account = 1 + client % 16;
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
            let drain = std::thread::spawn(move || -> std::io::Result<u64> {
                let mut scratch = vec![0_u8; FRAME_LEN * 256];
                let mut seen = 0_u64;
                let mut partial = 0_usize;
                while seen < orders_each {
                    let bytes = reader.read(&mut scratch[partial..])?;
                    if bytes == 0 {
                        break;
                    }
                    let filled = partial + bytes;
                    let whole = filled / FRAME_LEN;
                    for index in 0..whole {
                        let start = index * FRAME_LEN;
                        if let Ok(event) =
                            Event::read_from_bytes(&scratch[start..start + FRAME_LEN])
                            && event.kind == EventKind::Received as u8
                        {
                            seen += 1;
                        }
                    }
                    scratch.copy_within(whole * FRAME_LEN..filled, 0);
                    partial = filled - whole * FRAME_LEN;
                }
                Ok(seen)
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
    let mut acknowledged = 0;
    for worker in workers {
        acknowledged += worker.join().expect("client thread panicked")?;
    }
    let elapsed = started.elapsed().as_secs_f64();
    Ok(((clients * orders_each) as f64 / elapsed, acknowledged))
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
    println!("{:<40}{received:>10} of {orders}", "orders acknowledged");

    let each = (orders / clients).max(1);
    let (concurrent, acknowledged) = concurrent_throughput(&address, clients, each, 20_000_000)?;
    println!(
        "{:<40}{concurrent:>10.0} orders/sec",
        format!("{clients} clients at once, {each} each")
    );
    println!(
        "{:<40}{acknowledged:>10} of {}",
        "orders acknowledged",
        clients * each
    );
    println!("{}", "-".repeat(72));

    // A run where orders were refused is not the run it claims to be: a refused
    // order skips matching, so a saturated venue reports a throughput it cannot
    // sustain. This project has measured the reject path by accident three
    // times, every time because nothing said so out loud.
    if acknowledged < clients * each {
        eprintln!(
            "WARNING: {} of {} concurrent orders were not acknowledged. If the venue \
             reports \"open order limit reached\", its pool filled and this figure \
             is measuring refusals, not matching.",
            clients * each - acknowledged,
            clients * each
        );
    }
    if received < orders {
        eprintln!(
            "WARNING: {} of {orders} pipelined orders were not acknowledged, so this \
             figure is not measuring what it says.",
            orders - received
        );
    }
    Ok(())
}

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
//! ```text
//! load [address] [--orders N]
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

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let address = args
        .first()
        .filter(|a| !a.starts_with("--"))
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:7070".to_string());
    let orders: u64 = args
        .iter()
        .position(|a| a == "--orders")
        .and_then(|i| args.get(i + 1))
        .and_then(|n| n.parse().ok())
        .unwrap_or(200_000);

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
    println!("{}", "-".repeat(72));

    if received < orders {
        eprintln!("warning: the venue returned fewer events than orders sent");
    }
    Ok(())
}

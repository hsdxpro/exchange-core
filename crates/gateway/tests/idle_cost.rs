//! What an idle connection costs the venue.
//!
//! A venue open to the public holds far more connections than are active at any
//! instant. The loop reads every session each pass, so an idle session is not
//! free: it is a syscall per pass, plus a cursor check per channel it follows.
//! That cost is paid by every active client, because it lands in the same pass.
//!
//! This measures it rather than assuming it, because the answer decides whether
//! the loop needs readiness notification. A number here that grows linearly and
//! steeply means the venue cannot hold many connections whatever else is true of
//! it.

use bx_gateway::codec::encode;
use bx_gateway::tcp::Server;
use bx_journal::MemoryLog;
use bx_pipeline::instrument::{Instrument, Instruments};
use bx_pipeline::{limit_order, subscribe};
use bx_protocol::{ChannelKind, Side};
use std::io::Write;
use std::net::TcpStream;
use std::time::{Duration, Instant};

const BTC: u32 = 1;
const USD: u32 = 2;
const SYMBOL: u32 = 1;
const RETAINED: usize = 4_096;
const MAX_SESSIONS: usize = 1_024;
const MAX_RECORDS: usize = 64;

fn instruments() -> Instruments {
    let mut instruments = Instruments::new();
    instruments.insert(Instrument::new(SYMBOL, BTC, USD, 10_000, 1_000_000, 65_536));
    instruments
}

/// Median nanoseconds one `poll` costs with `idle` connections attached and
/// nothing to do.
fn idle_poll_cost(idle: usize, subscribed: bool) -> f64 {
    let mut server = Server::bind(
        "127.0.0.1:0",
        MemoryLog::new(),
        instruments(),
        RETAINED,
        MAX_RECORDS,
        MAX_SESSIONS,
    )
    .unwrap();
    server.venue_mut().deposit(1, USD, u64::MAX / 4).unwrap();
    let address = server.address().unwrap();

    // Hold the sockets open for the whole measurement.
    let mut clients = Vec::with_capacity(idle);
    for _ in 0..idle {
        let mut stream = TcpStream::connect(address).unwrap();
        stream.set_nodelay(true).unwrap();
        // Every session sends one message so it is attributed an account, and
        // optionally subscribes, since a cursor per channel is part of the cost.
        let mut bytes = Vec::new();
        encode(&limit_order(1, SYMBOL, 1, Side::Bid, 10_000, 1), &mut bytes);
        if subscribed {
            encode(&subscribe(1, SYMBOL, ChannelKind::Book), &mut bytes);
            encode(&subscribe(1, SYMBOL, ChannelKind::Trades), &mut bytes);
        }
        stream.write_all(&bytes).unwrap();
        clients.push(stream);
        // Accept as we go. Opening hundreds of sockets without accepting
        // overflows the listener backlog, and the OS refuses them before the
        // venue ever sees them.
        server.poll().unwrap();
    }

    // Drain the connect burst, then let it settle.
    let settle = Instant::now() + Duration::from_millis(300);
    while Instant::now() < settle {
        server.poll().unwrap();
    }
    assert_eq!(server.sessions(), idle, "not every client stayed connected");

    let mut samples = Vec::with_capacity(200);
    for _ in 0..200 {
        let started = Instant::now();
        server.poll().unwrap();
        samples.push(started.elapsed().as_secs_f64() * 1e9);
    }
    samples.sort_by(f64::total_cmp);
    drop(clients);
    samples[samples.len() / 2]
}

#[test]
fn an_idle_connection_costs_the_venue_a_syscall_every_pass() {
    // Reported rather than merely asserted: the shape of this curve is the point.
    let mut measured = Vec::new();
    for idle in [1_usize, 16, 64, 256] {
        let cost = idle_poll_cost(idle, true);
        measured.push((idle, cost));
        println!(
            "{idle:>4} idle sessions: {cost:>9.0} ns per pass, {:>7.0} ns each",
            cost / idle as f64
        );
    }

    let (_, one) = measured[0];
    let (many, lots) = measured[measured.len() - 1];
    let per_session = (lots - one) / many as f64;
    println!("marginal cost of an idle session: {per_session:.0} ns per pass");

    // The venue must still make progress. This is the assertion that matters:
    // whatever the per-session cost, a pass with 256 idle sessions has to stay
    // fast enough that an active client is not waiting behind them.
    assert!(
        lots < 5_000_000.0,
        "256 idle sessions cost {lots:.0} ns a pass, which an active client pays for"
    );
    // And the cost has to be roughly linear rather than worse: a superlinear
    // curve would mean the loop is doing something quadratic in connections.
    let ratio = lots / one.max(1.0);
    assert!(
        ratio < many as f64 * 4.0,
        "cost grew {ratio:.1}x for {many}x the sessions, which is worse than linear"
    );
}

#[test]
fn an_active_client_still_gets_served_behind_idle_ones() {
    const IDLE: usize = 256;
    let mut server = Server::bind(
        "127.0.0.1:0",
        MemoryLog::new(),
        instruments(),
        RETAINED,
        MAX_RECORDS,
        MAX_SESSIONS,
    )
    .unwrap();
    server.venue_mut().deposit(2, USD, u64::MAX / 4).unwrap();
    let address = server.address().unwrap();

    let mut idle = Vec::with_capacity(IDLE);
    for _ in 0..IDLE {
        let stream = TcpStream::connect(address).unwrap();
        stream.set_nodelay(true).unwrap();
        idle.push(stream);
        server.poll().unwrap();
    }
    let settle = Instant::now() + Duration::from_millis(200);
    while Instant::now() < settle {
        server.poll().unwrap();
    }

    // One real client, behind all of them.
    let mut active = TcpStream::connect(address).unwrap();
    active.set_nodelay(true).unwrap();
    let mut bytes = Vec::new();
    encode(
        &limit_order(2, SYMBOL, 500, Side::Bid, 10_100, 5),
        &mut bytes,
    );
    active.write_all(&bytes).unwrap();

    let mut applied = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while applied == 0 && Instant::now() < deadline {
        applied = server.poll().unwrap();
    }
    assert_eq!(applied, 1, "the active client's order never got through");
    assert_eq!(
        server.venue().book(SYMBOL).unwrap().depth(Side::Bid, 10),
        vec![(10_100, 5)]
    );
    drop(idle);
}

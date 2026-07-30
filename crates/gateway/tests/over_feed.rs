//! Public market data on its own port, served by a thread that is not the
//! venue's.
//!
//! The properties worth pinning are not "events arrive" -- they are the ones
//! that make a separate feed worth having at all: the audience never touches
//! the order path, a subscriber that stops reading is shed rather than
//! accumulated for, and nothing private is reachable from a port that asks for
//! no credentials.

use bx_gateway::feed::Feed;
use bx_gateway::handoff::Handoff;
use bx_gateway::tcp::Server;
use bx_journal::MemoryLog;
use bx_pipeline::instrument::{Instrument, Instruments};
use bx_pipeline::{limit_order, subscribe};
use bx_protocol::{ChannelKind, Command, Event, EventKind, Side};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use zerocopy::{FromBytes, IntoBytes};

const SYMBOL: u32 = 1;
const BTC: u32 = 1;
const USD: u32 = 2;
const FRAME: usize = size_of::<Event>();
const RETAINED: usize = 4_096;

struct Running {
    orders: String,
    feed: String,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    _distributor: Feed,
}

impl Running {
    fn start() -> Self {
        let mut instruments = Instruments::new();
        instruments.insert(Instrument::new(SYMBOL, BTC, USD, 10_000, 1_000_000, 65_536));
        let mut server = Server::bind(
            "127.0.0.1:0",
            MemoryLog::new(),
            instruments,
            RETAINED,
            256,
            64,
        )
        .unwrap();
        for account in 1..=4 {
            for asset in [USD, BTC] {
                server
                    .venue_mut()
                    .deposit(account, asset, u64::MAX / 4)
                    .unwrap();
            }
        }
        let handoff = Handoff::new();
        server.publish_to(handoff.clone());
        let distributor = Feed::start("127.0.0.1:0", handoff, RETAINED, RETAINED * FRAME).unwrap();

        let orders = server.address().unwrap().to_string();
        let feed = distributor.address().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            while !flag.load(Ordering::Relaxed) {
                server.poll().expect("the venue failed to commit");
                std::thread::sleep(Duration::from_micros(200));
            }
        });
        Self {
            orders,
            feed,
            stop,
            thread: Some(thread),
            _distributor: distributor,
        }
    }

    fn connect(address: &str) -> TcpStream {
        let stream = TcpStream::connect(address).unwrap();
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        stream
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn send(stream: &mut TcpStream, commands: &[Command]) {
    let mut bytes = Vec::new();
    for command in commands {
        bytes.extend_from_slice(command.as_bytes());
    }
    stream.write_all(&bytes).unwrap();
    stream.flush().unwrap();
}

fn collect_until(
    stream: &mut TcpStream,
    window: Duration,
    enough: impl Fn(&[Event]) -> bool,
) -> Vec<Event> {
    let mut seen = Vec::new();
    let mut buffer = vec![0_u8; FRAME * 256];
    let mut held = 0;
    let deadline = Instant::now() + window;
    while Instant::now() < deadline && !enough(&seen) {
        let Ok(read) = stream.read(&mut buffer[held..]) else {
            continue;
        };
        if read == 0 {
            break;
        }
        let filled = held + read;
        let whole = filled / FRAME;
        for index in 0..whole {
            let at = index * FRAME;
            if let Ok(event) = Event::read_from_bytes(&buffer[at..at + FRAME]) {
                seen.push(event);
            }
        }
        buffer.copy_within(whole * FRAME..filled, 0);
        held = filled - whole * FRAME;
    }
    seen
}

/// Rests an ask and crosses it, which produces a trade on the tape.
fn trade(venue: &Running, maker: u64, taker: u64) {
    let mut seller = Running::connect(&venue.orders);
    send(
        &mut seller,
        &[limit_order(1, SYMBOL, maker, Side::Ask, 10_100, 1)],
    );
    let _ = collect_until(&mut seller, Duration::from_secs(2), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Resting as u8)
    });
    let mut buyer = Running::connect(&venue.orders);
    send(
        &mut buyer,
        &[limit_order(2, SYMBOL, taker, Side::Bid, 10_100, 1)],
    );
    let _ = collect_until(&mut buyer, Duration::from_secs(2), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Filled as u8)
    });
}

#[test]
fn the_tape_arrives_on_the_feed_port() {
    let venue = Running::start();
    let mut watcher = Running::connect(&venue.feed);
    send(&mut watcher, &[subscribe(0, SYMBOL, ChannelKind::Trades)]);
    // Subscribed before the trade, so this is the live increment path rather
    // than a replay of the ring.
    std::thread::sleep(Duration::from_millis(50));
    trade(&venue, 1, 2);

    let seen = collect_until(&mut watcher, Duration::from_secs(10), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Trade as u8)
    });
    assert!(
        seen.iter().any(|e| e.kind == EventKind::Trade as u8),
        "the trade never reached the feed port: {seen:?}"
    );
}

#[test]
fn the_feed_numbers_its_channel_so_a_gap_is_arithmetic() {
    let venue = Running::start();
    let mut watcher = Running::connect(&venue.feed);
    send(&mut watcher, &[subscribe(0, SYMBOL, ChannelKind::Trades)]);
    std::thread::sleep(Duration::from_millis(50));
    for id in 0..4_u64 {
        trade(&venue, 10 + id * 2, 11 + id * 2);
    }

    let seen = collect_until(&mut watcher, Duration::from_secs(10), |seen| {
        seen.iter()
            .filter(|e| e.kind == EventKind::Trade as u8)
            .count()
            >= 4
    });
    let trades: Vec<&Event> = seen
        .iter()
        .filter(|e| e.kind == EventKind::Trade as u8)
        .collect();
    assert!(trades.len() >= 4, "not every trade arrived: {trades:?}");
    for pair in trades.windows(2) {
        assert_eq!(
            pair[1].sequence,
            pair[0].sequence + 1,
            "the feed left a hole in its own numbering, so a client could not \
             tell a gap from a quiet market"
        );
    }
}

#[test]
fn nothing_private_is_reachable_from_the_feed_port() {
    // The port asks for no credentials, which is only safe because nothing
    // private is served from it. A client naming an account channel must get
    // silence, not somebody else's fills.
    let venue = Running::start();
    let mut watcher = Running::connect(&venue.feed);
    send(&mut watcher, &[subscribe(1, SYMBOL, ChannelKind::Account)]);
    std::thread::sleep(Duration::from_millis(50));
    trade(&venue, 30, 31);

    let seen = collect_until(&mut watcher, Duration::from_secs(2), |_| false);
    assert!(
        seen.is_empty(),
        "the feed port served private events to a client that simply asked: {seen:?}"
    );
}

#[test]
fn the_order_path_keeps_serving_while_the_feed_is_ignored() {
    // The whole reason the feed is a separate thread and port. A subscriber
    // that connects and never reads must not slow, stall or shed the client
    // sending orders.
    let venue = Running::start();
    let mut deadbeat = Running::connect(&venue.feed);
    send(&mut deadbeat, &[subscribe(0, SYMBOL, ChannelKind::Trades)]);

    let mut trader = Running::connect(&venue.orders);
    for id in 100..160_u64 {
        send(
            &mut trader,
            &[limit_order(
                1,
                SYMBOL,
                id,
                Side::Bid,
                10_000 + (id % 50) as i64,
                1,
            )],
        );
        let seen = collect_until(&mut trader, Duration::from_secs(5), |seen| {
            seen.iter()
                .any(|e| e.kind == EventKind::Resting as u8 && e.order_id == id)
        });
        assert!(
            seen.iter()
                .any(|e| e.kind == EventKind::Resting as u8 && e.order_id == id),
            "order {id} was not served while a subscriber sat idle: {seen:?}"
        );
    }
    drop(deadbeat);
}

//! End-to-end over real sockets.
//!
//! A server on a real TCP port, clients on real connections, orders encoded as
//! bytes and events decoded from bytes. Nothing is stubbed: the same framing,
//! the same group-commit loop, and the same fan-out a deployed venue would run.
//!
//! The server runs on its own thread and the clients on the test thread, so a
//! deadlock or a lost wakeup shows up as a test timeout rather than as a pass.

use bx_gateway::codec::encode;
use bx_gateway::tcp::{Server, read_events};
use bx_journal::MemoryLog;
use bx_pipeline::instrument::{Instrument, Instruments};
use bx_pipeline::{limit_order, market_order};
use bx_protocol::{Command, EventKind, Side, Ticks};
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const BTC: u32 = 1;
const USD: u32 = 2;
const SYMBOL: u32 = 1;
const FLOOR: Ticks = 10_000;
/// Retention window. Sized so the burst test below stays inside it when a pass
/// is bounded (a few hundred events at a time) and would blow straight through
/// it if a pass were unbounded, which is the regression being guarded.
const RETAINED: usize = 32_768;
const MAX_RECORDS: usize = 256;

fn instruments() -> Instruments {
    let mut instruments = Instruments::new();
    instruments.insert(Instrument::new(SYMBOL, BTC, USD, FLOOR, 1_000_000, 65_536));
    instruments
}

/// A running venue, and the handle that stops it.
struct Running {
    address: String,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Running {
    fn start() -> Self {
        let mut server = Server::bind(
            "127.0.0.1:0",
            MemoryLog::new(),
            instruments(),
            RETAINED,
            MAX_RECORDS,
        )
        .unwrap();
        // Funded well beyond anything these tests spend. An underfunded account
        // rejects for insufficient balance part way through a run, which looks
        // exactly like the venue dropping commands.
        for account in 1..=4 {
            for asset in [USD, BTC] {
                server
                    .venue_mut()
                    .deposit(account, asset, u64::MAX / 4)
                    .unwrap();
            }
        }
        let address = server.address().unwrap().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);

        let thread = std::thread::spawn(move || {
            while !flag.load(Ordering::Relaxed) {
                server.poll().expect("the venue failed to commit");
                // Nothing to do until a socket is readable. A deployed venue
                // busy-polls a pinned core here; a test does not need to.
                std::thread::sleep(Duration::from_micros(200));
            }
        });

        Self {
            address,
            stop,
            thread: Some(thread),
        }
    }

    fn connect(&self) -> TcpStream {
        let stream = TcpStream::connect(&self.address).unwrap();
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
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
        encode(command, &mut bytes);
    }
    stream.write_all(&bytes).unwrap();
    stream.flush().unwrap();
}

#[test]
fn orders_sent_over_a_socket_trade_and_come_back_as_events() {
    let venue = Running::start();
    let mut trader = venue.connect();

    send(
        &mut trader,
        &[
            limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5),
            limit_order(1, SYMBOL, 102, Side::Bid, 10_090, 3),
        ],
    );

    let mut events = Vec::new();
    read_events(&mut trader, 2, &mut events).unwrap();
    assert!(
        events.iter().all(|e| e.kind == EventKind::BookDelta as u8),
        "expected depth updates, got {events:?}"
    );
    let prices: Vec<Ticks> = events.iter().map(|e| e.price).collect();
    assert!(prices.contains(&10_100) && prices.contains(&10_090));
}

#[test]
fn a_second_client_sees_the_public_feed_of_the_first_clients_trading() {
    let venue = Running::start();
    let mut maker = venue.connect();
    // The watcher connects first so it is subscribed before anything trades.
    let mut watcher = venue.connect();

    send(
        &mut maker,
        &[limit_order(1, SYMBOL, 101, Side::Ask, 10_100, 5)],
    );
    // Give the maker's order time to rest before the taker crosses it.
    std::thread::sleep(Duration::from_millis(50));
    send(&mut maker, &[market_order(2, SYMBOL, 201, Side::Bid, 5)]);

    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let mut batch = Vec::new();
        read_events(&mut watcher, 1, &mut batch).unwrap();
        seen.extend(batch);
        if seen.iter().any(|e| e.kind == EventKind::Trade as u8) {
            break;
        }
    }

    let trades: Vec<_> = seen
        .iter()
        .filter(|e| e.kind == EventKind::Trade as u8)
        .collect();
    assert!(!trades.is_empty(), "the watcher never saw the print");
    assert_eq!((trades[0].price, trades[0].quantity), (10_100, 5));
    assert_eq!(trades[0].account, 0, "the public tape leaked an account");
}

#[test]
fn a_record_split_across_two_writes_is_still_understood() {
    let venue = Running::start();
    let mut trader = venue.connect();

    let mut bytes = Vec::new();
    encode(
        &limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5),
        &mut bytes,
    );
    // Deliberately tear the record in half, with a pause between the pieces.
    trader.write_all(&bytes[..30]).unwrap();
    trader.flush().unwrap();
    std::thread::sleep(Duration::from_millis(30));
    trader.write_all(&bytes[30..]).unwrap();
    trader.flush().unwrap();

    let mut events = Vec::new();
    read_events(&mut trader, 1, &mut events).unwrap();
    assert_eq!(events[0].kind, EventKind::BookDelta as u8);
    assert_eq!(events[0].price, 10_100);
}

#[test]
fn many_orders_pushed_at_once_are_applied_as_one_group_and_all_arrive() {
    const ORDERS: usize = 200;
    let venue = Running::start();
    let mut trader = venue.connect();

    let commands: Vec<Command> = (0..ORDERS)
        .map(|i| limit_order(1, SYMBOL, 100 + i as u64, Side::Bid, 10_000 + i as Ticks, 1))
        .collect();
    send(&mut trader, &commands);

    let mut events = Vec::new();
    read_events(&mut trader, ORDERS, &mut events).unwrap();
    let deltas = events
        .iter()
        .filter(|e| e.kind == EventKind::BookDelta as u8)
        .count();
    assert_eq!(
        deltas, ORDERS,
        "a command was lost between the socket and the book"
    );
}

#[test]
fn a_burst_larger_than_the_retention_window_does_not_drop_the_client() {
    // The venue used to read a socket until it blocked, so one client pushing a
    // large write handed it a group big enough to wrap the subscription rings
    // inside a single pass: 8,192 orders produce about 24,000 events against a
    // 32,768 window, which one unbounded pass overruns and bounded passes of a
    // few hundred never approach. Everyone was then dropped for lagging, including
    // clients that had not been sent anything yet. A pass is now bounded, so
    // the feed keeps up with the book however hard one client writes.
    const ORDERS: usize = 8_192;
    let venue = Running::start();
    let mut writer = venue.connect();
    let mut reader = writer.try_clone().unwrap();

    // Read on another thread. A client that writes half a megabyte without
    // reading deadlocks against its own receive buffer, which is a bug in the
    // client and would hide the one being tested for.
    let counter = std::thread::spawn(move || {
        let mut acknowledged = 0;
        let deadline = Instant::now() + Duration::from_secs(30);
        while acknowledged < ORDERS && Instant::now() < deadline {
            let mut batch = Vec::new();
            if read_events(&mut reader, 1, &mut batch).is_err() || batch.is_empty() {
                break;
            }
            acknowledged += batch
                .iter()
                .filter(|e| e.kind == EventKind::Received as u8)
                .count();
        }
        acknowledged
    });

    let commands: Vec<Command> = (0..ORDERS)
        .map(|i| {
            limit_order(
                1,
                SYMBOL,
                100 + i as u64,
                Side::Bid,
                FLOOR + 1_000 + (i % 4_000) as Ticks,
                1,
            )
        })
        .collect();
    send(&mut writer, &commands);

    assert_eq!(
        counter.join().unwrap(),
        ORDERS,
        "the venue dropped a client that was reading as fast as it could"
    );
}

#[test]
fn a_client_that_disconnects_does_not_disturb_the_venue() {
    let venue = Running::start();
    {
        let mut leaving = venue.connect();
        send(
            &mut leaving,
            &[limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5)],
        );
        let mut events = Vec::new();
        read_events(&mut leaving, 1, &mut events).unwrap();
    } // dropped: the socket closes mid-session

    std::thread::sleep(Duration::from_millis(50));

    // The venue is still trading, and still knows about the departed client's
    // resting order.
    let mut arriving = venue.connect();
    send(
        &mut arriving,
        &[limit_order(2, SYMBOL, 201, Side::Ask, 10_100, 5)],
    );

    let mut events = Vec::new();
    read_events(&mut arriving, 1, &mut events).unwrap();
    assert!(
        events
            .iter()
            .any(|e| e.kind == EventKind::BookDelta as u8 || e.kind == EventKind::Trade as u8),
        "the venue stopped working after a disconnect: {events:?}"
    );
}

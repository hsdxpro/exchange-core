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
use bx_gateway::multicast::{HEADER_LEN, Header, Multicast};
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
        let distributor =
            Feed::start("127.0.0.1:0", handoff, RETAINED, RETAINED * FRAME, None).unwrap();

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

#[test]
fn the_same_events_go_out_as_packets_and_to_tcp_subscribers() {
    // One drain, two outputs. A venue running both must not have to choose,
    // and the packet path must carry the same market the socket path does.
    let receiver = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    receiver
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let group = receiver.local_addr().unwrap().to_string();

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
    let sender = Multicast::open(&[group], 0x5151).unwrap();
    let distributor = Feed::start(
        "127.0.0.1:0",
        handoff,
        RETAINED,
        RETAINED * FRAME,
        Some(sender),
    )
    .unwrap();
    let venue = Running {
        orders: server.address().unwrap().to_string(),
        feed: distributor.address().to_string(),
        stop: Arc::new(AtomicBool::new(false)),
        thread: None,
        _distributor: distributor,
    };
    let stop = Arc::clone(&venue.stop);
    let running = std::thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            server.poll().expect("the venue failed to commit");
            std::thread::sleep(Duration::from_micros(200));
        }
    });

    let mut watcher = Running::connect(&venue.feed);
    send(&mut watcher, &[subscribe(0, SYMBOL, ChannelKind::Trades)]);
    std::thread::sleep(Duration::from_millis(50));
    trade(&venue, 1, 2);

    // The socket path.
    let seen = collect_until(&mut watcher, Duration::from_secs(10), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Trade as u8)
    });
    assert!(
        seen.iter().any(|e| e.kind == EventKind::Trade as u8),
        "the trade never reached the TCP subscriber: {seen:?}"
    );

    // The packet path, carrying the same market.
    let mut buffer = vec![0_u8; 2_048];
    let mut trades = 0;
    let deadline = Instant::now() + Duration::from_secs(10);
    while trades == 0 && Instant::now() < deadline {
        let Ok(read) = receiver.recv(&mut buffer) else {
            break;
        };
        let header = Header::read(&buffer[..read]).expect("a packet arrived with no header");
        assert_eq!(header.session, 0x5151, "the run identifier was lost");
        assert_eq!(
            read,
            HEADER_LEN + header.count as usize * FRAME,
            "the packet length disagrees with its own count"
        );
        for index in 0..header.count as usize {
            let at = HEADER_LEN + index * FRAME;
            let event = Event::read_from_bytes(&buffer[at..at + FRAME]).unwrap();
            if event.kind == EventKind::Trade as u8 {
                trades += 1;
            }
        }
    }
    assert!(trades > 0, "the trade never reached the multicast group");

    venue.stop.store(true, Ordering::Relaxed);
    let _ = running.join();
}

#[test]
fn no_private_event_is_ever_put_on_a_group() {
    // A multicast group is joinable by anyone who can reach the network. A
    // private feed on one would not be private, and this is the last place that
    // can be enforced.
    for channel in [
        bx_pipeline::hub::Channel::Account(1),
        bx_pipeline::hub::Channel::Account(u64::MAX),
    ] {
        assert!(
            bx_gateway::multicast::wire_channel(channel).is_none(),
            "a private channel was given a wire name, so it could be broadcast"
        );
    }
}

#[test]
fn a_receiver_that_missed_events_recovers_them_from_the_feed() {
    // The other half of an incremental feed. Multicast never retransmits and
    // never waits, so a receiver that sees a hole in the sequence has to ask
    // somewhere -- and asking must not reach into the fast path. It asks here,
    // on the same port subscriptions use, and is served from the retention ring
    // by the thread that owns it.
    let venue = Running::start();

    // Trades happen with nobody watching, which is exactly the position a
    // receiver is in after losing packets.
    for id in 0..3_u64 {
        trade(&venue, 40 + id * 2, 41 + id * 2);
    }

    let mut recovering = Running::connect(&venue.feed);
    send(
        &mut recovering,
        &[Command::resuming(0, SYMBOL, ChannelKind::Trades, 0)],
    );
    let seen = collect_until(&mut recovering, Duration::from_secs(10), |seen| {
        seen.iter()
            .filter(|e| e.kind == EventKind::Trade as u8)
            .count()
            >= 3
    });
    let trades: Vec<&Event> = seen
        .iter()
        .filter(|e| e.kind == EventKind::Trade as u8)
        .collect();
    assert!(
        trades.len() >= 3,
        "the feed did not replay what the receiver missed: {trades:?}"
    );
    // Contiguous from the position asked for: no gap inside the repair, and no
    // repeat, which is what makes the recovered range usable as-is.
    assert_eq!(
        trades[0].sequence, 0,
        "the repair started somewhere other than where it was asked to"
    );
    for pair in trades.windows(2) {
        assert_eq!(
            pair[1].sequence,
            pair[0].sequence + 1,
            "the repair itself had a hole in it"
        );
    }
}

#[test]
fn asking_for_a_position_the_feed_never_reached_does_not_hang_the_client() {
    // A cursor from a previous run of the venue names a sequence this one has
    // never issued. Answering with silence would leave a receiver waiting on a
    // repair that is never coming; it is placed at the live edge instead, and
    // the jump in its numbering is the instruction to reconcile.
    let venue = Running::start();
    let mut client = Running::connect(&venue.feed);
    send(
        &mut client,
        &[Command::resuming(0, SYMBOL, ChannelKind::Trades, 9_000_000)],
    );
    std::thread::sleep(Duration::from_millis(50));
    trade(&venue, 60, 61);

    let seen = collect_until(&mut client, Duration::from_secs(10), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Trade as u8)
    });
    assert!(
        seen.iter().any(|e| e.kind == EventKind::Trade as u8),
        "a client holding an impossible position was never served again: {seen:?}"
    );
}

#[test]
fn a_client_joining_mid_stream_is_given_the_book_before_the_increments() {
    // The half of recovery that retransmission cannot do. Increments alone
    // never tell a joiner what stood at a price before it arrived, so a client
    // that connects to a moving market would build a book out of whatever
    // happened next and be wrong about everything else.
    let venue = Running::start();

    // A market forms with nobody watching the feed.
    let mut maker = Running::connect(&venue.orders);
    for (id, price) in [(1_u64, 10_050_i64), (2, 10_040), (3, 10_030)] {
        send(
            &mut maker,
            &[limit_order(1, SYMBOL, id, Side::Bid, price, 5)],
        );
        let _ = collect_until(&mut maker, Duration::from_secs(2), |seen| {
            seen.iter()
                .any(|e| e.kind == EventKind::Resting as u8 && e.order_id == id)
        });
    }
    std::thread::sleep(Duration::from_millis(100));

    // Only now does anybody subscribe.
    let mut joiner = Running::connect(&venue.feed);
    send(&mut joiner, &[subscribe(0, SYMBOL, ChannelKind::Book)]);
    let seen = collect_until(&mut joiner, Duration::from_secs(10), |seen| {
        seen.iter()
            .filter(|e| e.kind == EventKind::BookSnapshot as u8)
            .count()
            >= 3
    });
    let stated: Vec<&Event> = seen
        .iter()
        .filter(|e| e.kind == EventKind::BookSnapshot as u8)
        .collect();
    assert_eq!(
        stated.len(),
        3,
        "the joining client was not told the book that already existed: {seen:?}"
    );
    // Best first, and the prices the venue actually holds.
    let prices: Vec<i64> = stated.iter().map(|e| e.price).collect();
    assert_eq!(
        prices,
        vec![10_050, 10_040, 10_030],
        "the stated book was not best-first or not the venue's"
    );
    assert!(
        stated.iter().all(|e| e.quantity == 5),
        "the stated depth is not what rests on the venue: {stated:?}"
    );

    // And the increments continue from where the snapshot was taken, so
    // applying them on top lands on the venue's book rather than beside it.
    send(
        &mut maker,
        &[limit_order(1, SYMBOL, 9, Side::Bid, 10_060, 7)],
    );
    let after = collect_until(&mut joiner, Duration::from_secs(10), |seen| {
        seen.iter().any(|e| e.kind == EventKind::BookDelta as u8)
    });
    let moved: Vec<&Event> = after
        .iter()
        .filter(|e| e.kind == EventKind::BookDelta as u8)
        .collect();
    assert!(
        moved.iter().any(|e| e.price == 10_060 && e.quantity == 7),
        "the increments after the snapshot did not arrive: {after:?}"
    );
}

#[test]
fn market_data_does_not_wait_out_the_feeds_idle_timer() {
    // The feed waits on sockets, and the venue has no socket here to make
    // readable -- so a batch handed over in a quiet moment used to sit until
    // that wait timed out. Two hundred microseconds of market-data latency on a
    // venue that measures order entry in tens, and invisible in every test,
    // because every test allows seconds.
    //
    // The venue wakes the thread instead. This measures that: the idle timer is
    // long now precisely because nothing depends on it, so an event that waits
    // for it would take tens of milliseconds rather than a fraction of one.
    let venue = Running::start();
    let mut watcher = Running::connect(&venue.feed);
    send(&mut watcher, &[subscribe(0, SYMBOL, ChannelKind::Trades)]);
    std::thread::sleep(Duration::from_millis(50));

    let mut worst = Duration::ZERO;
    for id in 0..5_u64 {
        let started = Instant::now();
        trade(&venue, 200 + id * 2, 201 + id * 2);
        let seen = collect_until(&mut watcher, Duration::from_secs(5), |seen| {
            seen.iter().any(|e| e.kind == EventKind::Trade as u8)
        });
        assert!(
            seen.iter().any(|e| e.kind == EventKind::Trade as u8),
            "a trade never arrived at all"
        );
        worst = worst.max(started.elapsed());
    }
    // Generous against the idle timer and still far below it: the trade itself
    // costs two round trips to the order port, so this is not a latency figure,
    // only proof that nothing is waiting on a timeout.
    assert!(
        worst < Duration::from_millis(25),
        "market data took {worst:?}, which is the feed sleeping through it \
         rather than being woken"
    );
}

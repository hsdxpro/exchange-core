//! Who may act, and how fast — over real sockets.
//!
//! Everything a client does here goes through the same framing, the same
//! challenge, and the same group-commit loop a deployment runs. Two properties
//! are worth the process: that an unproven session cannot reach the book at all,
//! and that a captured proof is worthless against the next connection. Neither
//! can be shown by calling a verifier directly, because both are about what the
//! *server loop* does with a socket.

use bx_gateway::auth::Credentials;
use bx_gateway::codec::encode;
use bx_gateway::limit::RateLimit;
use bx_gateway::tcp::{Server, read_events};
use bx_journal::MemoryLog;
use bx_pipeline::instrument::{Instrument, Instruments};
use bx_pipeline::{deposit, limit_order, withdraw};
use bx_protocol::{
    CHALLENGE_LEN, Command, Event, EventKind, PROOF_LEN, RejectReason, SIGNATURE_LEN, Side, Ticks,
};
use ed25519_dalek::{Signer, SigningKey};
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

const BTC: u32 = 1;
const USD: u32 = 2;
const SYMBOL: u32 = 1;
const FLOOR: Ticks = 10_000;

/// Commands one session may hand the venue in a single pass, and so the largest
/// batch the discard path can be asked to process at once. Deliberately the
/// deployment default rather than a small test number: the cost of refusing a
/// batch is a property of its size, and a batch of 256 would hide it.
const BATCH: usize = 4_096;

const ALICE: u64 = 1;
const BOB: u64 = 2;
/// Signing keys, from fixed seeds so a failure is reproducible.
fn alice_key() -> SigningKey {
    SigningKey::from_bytes(&[0x11; 32])
}

fn bob_key() -> SigningKey {
    SigningKey::from_bytes(&[0x22; 32])
}

/// A running venue, and the handle that stops it.
struct Running {
    address: String,
    stop: Arc<AtomicBool>,
    throttled: Arc<AtomicU64>,
    rejected_proofs: Arc<AtomicU64>,
    commands: Arc<AtomicU64>,
    sessions_accepted: Arc<AtomicU64>,
    band_rejects: Arc<AtomicU64>,
    duplicate_rejects: Arc<AtomicU64>,
    thread: Option<std::thread::JoinHandle<()>>,
}

/// Nanoseconds since the epoch, the same clock the venue stamps with.
fn wall_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos() as u64)
}

impl Running {
    /// A venue that stamps arrival and match times, which a deployment does and
    /// a measurement run does not.
    fn start_stamped() -> Self {
        Self::configured(false, None, true)
    }

    fn start(authenticated: bool, rate: Option<RateLimit>) -> Self {
        Self::configured(authenticated, rate, false)
    }

    /// A venue with an administrator, so the privileged commands are reachable.
    fn start_with_admin(admin: u64) -> Self {
        Self::build(true, None, false, Some(admin))
    }

    fn configured(authenticated: bool, rate: Option<RateLimit>, timestamps: bool) -> Self {
        Self::build(authenticated, rate, timestamps, None)
    }

    fn build(
        authenticated: bool,
        rate: Option<RateLimit>,
        timestamps: bool,
        admin: Option<u64>,
    ) -> Self {
        let mut instruments = Instruments::new();
        instruments.insert(Instrument::new(SYMBOL, BTC, USD, FLOOR, 1_000_000, 65_536));
        let mut server = Server::bind(
            "127.0.0.1:0",
            MemoryLog::new(),
            instruments,
            4_096,
            BATCH,
            64,
        )
        .expect("the venue could not bind");

        if authenticated {
            let mut credentials = Credentials::new();
            credentials
                .insert(ALICE, alice_key().verifying_key().to_bytes())
                .unwrap();
            credentials
                .insert(BOB, bob_key().verifying_key().to_bytes())
                .unwrap();
            server.require_authentication(credentials);
        }
        if let Some(rate) = rate {
            server.rate_limit(rate);
        }
        server.stamp_times(timestamps);
        if let Some(admin) = admin {
            server.administrator(admin);
        }
        for account in [ALICE, BOB] {
            for asset in [USD, BTC] {
                server
                    .venue_mut()
                    .deposit(account, asset, u64::MAX / 4)
                    .expect("funding failed");
            }
        }

        let address = server.address().unwrap().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let throttled = Arc::new(AtomicU64::new(0));
        let throttle_count = Arc::clone(&throttled);
        let rejected_proofs = Arc::new(AtomicU64::new(0));
        let proof_count = Arc::clone(&rejected_proofs);
        let commands = Arc::new(AtomicU64::new(0));
        let command_count = Arc::clone(&commands);
        let sessions_accepted = Arc::new(AtomicU64::new(0));
        let accept_count = Arc::clone(&sessions_accepted);
        let band_rejects = Arc::new(AtomicU64::new(0));
        let band_count = Arc::clone(&band_rejects);
        let duplicate_rejects = Arc::new(AtomicU64::new(0));
        let duplicate_count = Arc::clone(&duplicate_rejects);

        let thread = std::thread::spawn(move || {
            while !flag.load(Ordering::Relaxed) {
                server.poll().expect("the venue failed to commit");
                throttle_count.store(server.throttled(), Ordering::Relaxed);
                proof_count.store(server.rejected_proofs(), Ordering::Relaxed);
                command_count.store(server.metrics().commands(), Ordering::Relaxed);
                accept_count.store(server.metrics().sessions_accepted(), Ordering::Relaxed);
                let refused = server.venue().exchange().rejects();
                band_count.store(
                    refused[RejectReason::OutsidePriceBand as usize],
                    Ordering::Relaxed,
                );
                duplicate_count.store(
                    refused[RejectReason::DuplicateOrderId as usize],
                    Ordering::Relaxed,
                );
                std::thread::sleep(Duration::from_micros(200));
            }
        });

        Self {
            address,
            stop,
            throttled,
            rejected_proofs,
            commands,
            sessions_accepted,
            band_rejects,
            duplicate_rejects,
            thread: Some(thread),
        }
    }

    fn connect(&self) -> TcpStream {
        let stream = TcpStream::connect(&self.address).expect("could not connect");
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
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
    stream.write_all(&bytes).expect("the venue stopped reading");
}

/// Collects events until `enough` is satisfied or the window closes.
fn collect_until(
    stream: &mut TcpStream,
    window: Duration,
    enough: impl Fn(&[Event]) -> bool,
) -> Vec<Event> {
    stream
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let mut seen = Vec::new();
    let deadline = Instant::now() + window;
    while Instant::now() < deadline && !enough(&seen) {
        let mut batch = Vec::new();
        match read_events(stream, 1, &mut batch) {
            Ok(()) if batch.is_empty() => break, // peer closed
            Ok(()) => seen.extend(batch),
            Err(_) => {}
        }
    }
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    seen
}

/// Waits for the challenge the venue sends the moment it accepts a connection.
fn challenge_from(stream: &mut TcpStream) -> [u8; CHALLENGE_LEN] {
    let events = collect_until(stream, Duration::from_secs(5), |seen| !seen.is_empty());
    let first = events.first().expect("the venue sent no challenge");
    assert_eq!(
        first.kind(),
        Some(EventKind::Challenge),
        "the first thing a client hears must be the challenge"
    );
    first.challenge()
}

/// The whole client side of authentication: read the nonce, sign it, send both
/// halves of the signature.
fn authenticate(stream: &mut TcpStream, account: u64, key: &SigningKey) -> Vec<Event> {
    let nonce = challenge_from(stream);
    send(
        stream,
        &Command::authenticating(account, &signature(key, &nonce)),
    );
    collect_until(stream, Duration::from_secs(5), |seen| !seen.is_empty())
}

/// A signature over what the venue will verify: the domain, then the nonce.
fn signature(key: &SigningKey, nonce: &[u8; CHALLENGE_LEN]) -> [u8; SIGNATURE_LEN] {
    key.sign(&bx_gateway::auth::signed_message(nonce))
        .to_bytes()
}

fn order(account: u64, id: u64, price: Ticks) -> Command {
    limit_order(account, SYMBOL, id, Side::Bid, price, 1)
}

fn kinds(events: &[Event], kind: EventKind) -> usize {
    events.iter().filter(|e| e.kind == kind as u8).count()
}

/// Commands the venue has answered, accepted or refused.
///
/// The unit that matters when counting a flood. Counting *events* instead is a
/// trap: an accepted order produces two (an acknowledgement and a resting), a
/// refused one produces a single reject, so a window that stops at "100 events"
/// stops after 10 acceptances and 80 refusals and reports a limiter ten times
/// tighter than it is.
fn answered(events: &[Event]) -> usize {
    kinds(events, EventKind::Received) + kinds(events, EventKind::Rejected)
}

/// Polls a counter until it reaches `want`, or gives up.
///
/// The venue publishes its counters after a pass, but the socket it closed
/// during that same pass is visible to the client immediately — so a test that
/// reads a counter the instant it sees a disconnect reads it one store too
/// early. That is a race in the harness, not in the venue, and waiting is the
/// honest fix.
fn reaches(counter: &AtomicU64, want: u64) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let held = counter.load(Ordering::Relaxed);
        if held >= want {
            return held;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    counter.load(Ordering::Relaxed)
}

fn rejects_for(events: &[Event], reason: RejectReason) -> usize {
    events
        .iter()
        .filter(|e| e.kind == EventKind::Rejected as u8 && e.reject_reason == reason as u8)
        .count()
}

/// True once the peer has closed. Distinguished from "nothing to read yet",
/// which is what a plain failed read cannot tell you.
fn is_closed(stream: &mut TcpStream) -> bool {
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    let mut scratch = [0_u8; 64];
    loop {
        match stream.read(&mut scratch) {
            Ok(0) => return true,
            Ok(_) => {} // queued events still draining; keep reading
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {
                return false;
            }
            Err(_) => return true,
        }
    }
}

// ----------------------------------------------------------- authentication

#[test]
fn an_order_sent_before_proving_anything_never_reaches_the_book() {
    // The property the whole feature exists for. Before this, a session stated
    // its account on its first command and was believed.
    let venue = Running::start(true, None);
    let mut client = venue.connect();
    let _nonce = challenge_from(&mut client);

    send(&mut client, &[order(ALICE, 1, 10_500)]);
    let events = collect_until(&mut client, Duration::from_secs(2), |seen| !seen.is_empty());

    assert_eq!(
        rejects_for(&events, RejectReason::NotAuthenticated),
        1,
        "an unproven order was not refused with a reason: {events:?}"
    );
    assert_eq!(
        kinds(&events, EventKind::Received),
        0,
        "an unproven order was acknowledged"
    );
    assert_eq!(
        kinds(&events, EventKind::Resting),
        0,
        "an unproven order reached the book"
    );
}

#[test]
fn a_correct_proof_admits_the_session_and_it_can_then_trade() {
    let venue = Running::start(true, None);
    let mut client = venue.connect();

    let events = authenticate(&mut client, ALICE, &alice_key());
    assert_eq!(
        kinds(&events, EventKind::Authenticated),
        1,
        "a correct proof was not accepted: {events:?}"
    );

    send(&mut client, &[order(ALICE, 1, 10_500)]);
    let events = collect_until(&mut client, Duration::from_secs(2), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Resting as u8)
    });
    assert_eq!(
        kinds(&events, EventKind::Resting),
        1,
        "an authenticated session could not trade: {events:?}"
    );
}

#[test]
fn a_wrong_proof_closes_the_connection() {
    let venue = Running::start(true, None);
    let mut client = venue.connect();
    let nonce = challenge_from(&mut client);

    // Right account, wrong secret.
    send(
        &mut client,
        &Command::authenticating(ALICE, &signature(&bob_key(), &nonce)),
    );
    let events = collect_until(&mut client, Duration::from_secs(2), |seen| !seen.is_empty());
    assert_eq!(
        rejects_for(&events, RejectReason::NotAuthenticated),
        1,
        "a wrong proof was not reported before the drop: {events:?}"
    );
    assert!(
        is_closed(&mut client),
        "a session that failed to prove itself was left connected"
    );
    assert_eq!(
        reaches(&venue.rejected_proofs, 1),
        1,
        "a failed proof was not counted"
    );
}

#[test]
fn a_proof_captured_from_one_connection_does_not_open_another() {
    // The reason a nonce exists at all. On a transport with no TLS an
    // eavesdropper sees every byte of a successful login; this is the test that
    // says seeing it buys nothing.
    let venue = Running::start(true, None);

    let mut first = venue.connect();
    let nonce = challenge_from(&mut first);
    let captured = signature(&alice_key(), &nonce);
    send(&mut first, &Command::authenticating(ALICE, &captured));
    let events = collect_until(&mut first, Duration::from_secs(2), |seen| !seen.is_empty());
    assert_eq!(
        kinds(&events, EventKind::Authenticated),
        1,
        "the capture was taken from a login that did not work"
    );

    // Replayed verbatim onto a fresh connection, which gets a fresh nonce.
    let mut second = venue.connect();
    let replayed_nonce = challenge_from(&mut second);
    assert_ne!(nonce, replayed_nonce, "the venue reissued a nonce");
    send(&mut second, &Command::authenticating(ALICE, &captured));

    let events = collect_until(&mut second, Duration::from_secs(2), |seen| !seen.is_empty());
    assert_eq!(
        kinds(&events, EventKind::Authenticated),
        0,
        "a replayed proof was accepted: {events:?}"
    );
    assert!(is_closed(&mut second), "the replaying session was not shed");
}

#[test]
fn one_accounts_secret_does_not_open_another_account() {
    let venue = Running::start(true, None);
    let mut client = venue.connect();
    let nonce = challenge_from(&mut client);

    // Bob's own secret, correctly signed -- but claiming to be Alice.
    send(
        &mut client,
        &Command::authenticating(ALICE, &signature(&bob_key(), &nonce)),
    );
    let events = collect_until(&mut client, Duration::from_secs(2), |seen| !seen.is_empty());
    assert_eq!(kinds(&events, EventKind::Authenticated), 0);
    assert!(is_closed(&mut client));
}

#[test]
fn an_unknown_account_is_refused_like_a_bad_proof() {
    // Same outcome and same timing, so the venue cannot be used to enumerate
    // which accounts exist.
    let venue = Running::start(true, None);
    let mut client = venue.connect();
    let nonce = challenge_from(&mut client);
    send(
        &mut client,
        &Command::authenticating(9_999, &signature(&alice_key(), &nonce)),
    );
    let events = collect_until(&mut client, Duration::from_secs(2), |seen| !seen.is_empty());
    assert_eq!(
        rejects_for(&events, RejectReason::NotAuthenticated),
        1,
        "{events:?}"
    );
    assert!(is_closed(&mut client));
}

#[test]
fn a_client_may_pipeline_its_opening_orders_behind_the_proof() {
    // A market maker has already spent one round trip collecting the challenge.
    // Making it spend a second before it can quote would be a latency cost paid
    // by the client that matters most.
    let venue = Running::start(true, None);
    let mut client = venue.connect();
    let nonce = challenge_from(&mut client);

    let mut batch = Command::authenticating(ALICE, &signature(&alice_key(), &nonce)).to_vec();
    for id in 1..=5_i64 {
        batch.push(order(ALICE, id as u64, 10_500 - id));
    }
    send(&mut client, &batch);

    let events = collect_until(&mut client, Duration::from_secs(3), |seen| {
        seen.iter()
            .filter(|e| e.kind == EventKind::Resting as u8)
            .count()
            >= 5
    });
    assert_eq!(
        kinds(&events, EventKind::Authenticated),
        1,
        "the proof in a pipelined batch was not accepted: {events:?}"
    );
    assert_eq!(
        kinds(&events, EventKind::Resting),
        5,
        "orders behind the proof in the same write were lost: {events:?}"
    );
}

#[test]
fn an_open_venue_sends_no_challenge_and_takes_the_account_as_declared() {
    // The measurement path, and the one every benchmark uses. Worth pinning:
    // if a challenge leaked into open mode, every existing client would hang
    // waiting to be asked for something.
    let venue = Running::start(false, None);
    let mut client = venue.connect();
    send(&mut client, &[order(ALICE, 1, 10_500)]);
    let events = collect_until(&mut client, Duration::from_secs(2), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Resting as u8)
    });
    assert_eq!(kinds(&events, EventKind::Challenge), 0);
    assert_eq!(kinds(&events, EventKind::Resting), 1, "{events:?}");
}

// ------------------------------------------------------------ rate limiting

#[test]
fn a_flood_is_cut_to_the_allowance_and_told_why() {
    // One a second, bursting to 20: a hundred at once should leave 20 through
    // and refuse the rest, rather than dropping the connection or quietly
    // swallowing them. The rate is slow so that the tokens earned while the
    // test watches cannot move the answer — a bucket is a function of elapsed
    // time, and on a loaded machine a test's duration is not a constant.
    let venue = Running::start(false, Some(RateLimit::new(1, 20)));
    let mut client = venue.connect();

    let flood: Vec<Command> = (1..=100).map(|id| order(ALICE, id, 10_500)).collect();
    send(&mut client, &flood);

    let events = collect_until(&mut client, Duration::from_secs(3), |seen| {
        answered(seen) >= 100
    });
    let accepted = kinds(&events, EventKind::Received);
    let refused = rejects_for(&events, RejectReason::RateLimited);

    assert_eq!(
        accepted + refused,
        100,
        "commands went missing rather than being refused: {accepted} accepted, {refused} refused"
    );
    // The burst, plus at most a few tokens earned while the window was open.
    assert!(
        (20..=30).contains(&accepted),
        "a burst of 20 at 1/sec let {accepted} commands through"
    );
    assert!(
        reaches(&venue.throttled, 70) >= 70,
        "throttled commands were not counted"
    );
}

#[test]
fn a_throttled_session_stays_open_and_recovers() {
    // Being told "too fast" is not being disconnected. A client that backs off
    // must be able to carry on.
    let venue = Running::start(false, Some(RateLimit::new(1_000, 10)));
    let mut client = venue.connect();

    let flood: Vec<Command> = (1..=50).map(|id| order(ALICE, id, 10_500)).collect();
    send(&mut client, &flood);
    let _ = collect_until(&mut client, Duration::from_secs(2), |seen| {
        answered(seen) >= 50
    });
    assert!(
        !is_closed(&mut client),
        "a session was dropped for sending too fast rather than throttled"
    );

    // A tenth of a second at a thousand a second refills well past the burst.
    std::thread::sleep(Duration::from_millis(100));
    send(&mut client, &[order(ALICE, 500, 10_400)]);
    let events = collect_until(&mut client, Duration::from_secs(3), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Resting as u8)
    });
    assert!(
        kinds(&events, EventKind::Resting) >= 1,
        "a throttled session never recovered: {events:?}"
    );
}

#[test]
fn a_second_connection_does_not_buy_a_second_allowance() {
    // The reason the bucket is keyed by account rather than by session. If it
    // were per connection, the limit would be advice: open ten sockets, send ten
    // times as much.
    //
    // A token bucket is a function of elapsed time, so any assertion about how
    // much got through is partly an assertion about how long the test took —
    // and under a loaded machine a workspace run stretches by seconds. The rate
    // is therefore set far below the difference being detected: at one a second
    // the refill would need a hundred seconds to muddy a hundred-token burst,
    // and the windows below cap out at six.
    let venue = Running::start(false, Some(RateLimit::new(1, 100)));
    let mut first = venue.connect();
    let mut second = venue.connect();

    // Declare each session's account before either floods, so both are attached
    // to the same allowance when the flood arrives.
    send(&mut first, &[order(ALICE, 1, 10_500)]);
    send(&mut second, &[order(ALICE, 2, 10_500)]);
    let _ = collect_until(&mut first, Duration::from_secs(2), |seen| !seen.is_empty());
    let _ = collect_until(&mut second, Duration::from_secs(2), |seen| !seen.is_empty());

    // Counted by order ID, not by socket. Both sessions trade as ALICE and so
    // both follow ALICE's private feed, which means each is sent the other's
    // acknowledgements — a count taken per socket reads one session's burst as
    // the other's and reports a limit that is not being applied.
    let mine = |events: &[Event], range: std::ops::Range<u64>| -> usize {
        events
            .iter()
            .filter(|e| e.kind == EventKind::Received as u8 && range.contains(&e.order_id))
            .count()
    };

    // The first session drains the shared allowance...
    let flood: Vec<Command> = (1_000..1_120).map(|id| order(ALICE, id, 10_500)).collect();
    send(&mut first, &flood);
    let drained = collect_until(&mut first, Duration::from_secs(3), |seen| {
        answered(seen) >= 120
    });
    assert!(
        mine(&drained, 1_000..1_120) >= 90,
        "the first session did not get its burst, so nothing was drained"
    );

    // ...so the second finds it empty. With a bucket of its own it would find a
    // full hundred.
    let flood: Vec<Command> = (2_000..2_120).map(|id| order(ALICE, id, 10_500)).collect();
    send(&mut second, &flood);
    let after = collect_until(&mut second, Duration::from_secs(3), |seen| {
        answered(seen) >= 120
    });
    let accepted = mine(&after, 2_000..2_120);
    assert!(
        accepted <= 20,
        "a second session on the same account got {accepted} commands through an \
         allowance the first had already drained, so the limit is per connection"
    );
}

#[test]
fn an_allowance_is_forgotten_when_the_account_disconnects() {
    // Otherwise the table grows by one entry for every account that ever
    // connected, which at a million users is a leak that only shows up in
    // production.
    // One a second: a bucket that survived the disconnect would be drained, and
    // could not refill to twenty inside the window however long the test waits.
    let venue = Running::start(false, Some(RateLimit::new(1, 20)));
    {
        let mut client = venue.connect();
        let flood: Vec<Command> = (1..=40).map(|id| order(ALICE, id, 10_500)).collect();
        send(&mut client, &flood);
        let _ = collect_until(&mut client, Duration::from_secs(2), |seen| {
            answered(seen) >= 40
        });
    }
    // The socket is gone; give the venue a pass to notice.
    std::thread::sleep(Duration::from_millis(200));

    // A fresh connection for the same account starts from a full bucket, which
    // is only true if the old allowance was dropped.
    let mut client = venue.connect();
    let batch: Vec<Command> = (100..=119).map(|id| order(ALICE, id, 10_400)).collect();
    send(&mut client, &batch);
    let events = collect_until(&mut client, Duration::from_secs(3), |seen| {
        answered(seen) >= 20
    });
    assert!(
        kinds(&events, EventKind::Received) >= 20,
        "a reconnecting account did not get a fresh allowance: {events:?}"
    );
}

#[test]
fn authentication_and_rate_limiting_compose() {
    // Both on, which is the deployment shape. The proof itself must not be
    // charged against the allowance in a way that starves the first orders.
    let venue = Running::start(true, Some(RateLimit::new(1_000, 50)));
    let mut client = venue.connect();
    let events = authenticate(&mut client, BOB, &bob_key());
    assert_eq!(kinds(&events, EventKind::Authenticated), 1, "{events:?}");

    let batch: Vec<Command> = (1..=20).map(|id| order(BOB, id, 10_400)).collect();
    send(&mut client, &batch);
    let events = collect_until(&mut client, Duration::from_secs(3), |seen| {
        seen.iter()
            .filter(|e| e.kind == EventKind::Resting as u8)
            .count()
            >= 20
    });
    assert_eq!(kinds(&events, EventKind::Resting), 20, "{events:?}");
}

#[test]
fn a_proof_is_never_charged_to_the_venue_as_a_command() {
    // A secret in the journal would be written to disk and replayed on every
    // recovery. The gateway takes it out of the stream; this checks the sequence
    // never moved for it.
    let venue = Running::start(true, None);
    let mut client = venue.connect();
    let events = authenticate(&mut client, ALICE, &alice_key());
    let admitted = events
        .iter()
        .find(|e| e.kind == EventKind::Authenticated as u8)
        .expect("no acceptance");
    assert_eq!(
        admitted.sequence, 0,
        "the acceptance carried a sequence, so the proof entered the stream"
    );

    // Two orders with another proof between them. If authentication consumed a
    // sequence the gap would be two, not one. Comparing them to each other
    // rather than to zero is deliberate: the venue was funded by four journalled
    // deposits before any client connected, and a test that asserted "the first
    // order is sequence zero" would be asserting the funding, not the proof.
    let sequence_of = |client: &mut TcpStream, id: u64| -> u64 {
        send(client, &[order(ALICE, id, 10_500)]);
        let events = collect_until(client, Duration::from_secs(2), |seen| {
            seen.iter().any(|e| e.kind == EventKind::Received as u8)
        });
        events
            .iter()
            .find(|e| e.kind == EventKind::Received as u8)
            .expect("no acknowledgement")
            .cause_sequence
    };
    let first = sequence_of(&mut client, 1);
    send(
        &mut client,
        &Command::authenticating(ALICE, &[0; SIGNATURE_LEN]),
    );
    let second = sequence_of(&mut client, 2);
    assert_eq!(
        second,
        first + 1,
        "a proof consumed a sequence, so it was journalled"
    );
}

/// A signature survives the split into two records and back.
///
/// Truncation is the kind of thing that happens silently when a record is
/// repacked, and half a signature verifies against nothing -- so this checks the
/// bytes a client signs are exactly the bytes the venue reassembles.
#[test]
fn a_signature_survives_the_split_into_two_records() {
    assert_eq!(PROOF_LEN, 32);
    assert_eq!(SIGNATURE_LEN, 64);
    assert_eq!(CHALLENGE_LEN, 16);

    let signed = signature(&alice_key(), &[0; CHALLENGE_LEN]);
    let [first, second] = Command::authenticating(ALICE, &signed);
    assert_eq!(first.kind(), Some(bx_protocol::CommandKind::Authenticate));
    assert_eq!(
        second.kind(),
        Some(bx_protocol::CommandKind::AuthenticateContinued)
    );
    assert_eq!(first.account, ALICE);
    assert_eq!(second.account, ALICE, "both halves must name one account");

    let mut rebuilt = [0_u8; SIGNATURE_LEN];
    rebuilt[..PROOF_LEN].copy_from_slice(&first.proof());
    rebuilt[PROOF_LEN..].copy_from_slice(&second.proof());
    assert_eq!(rebuilt, signed, "the signature did not survive the split");
}

// ----------------------------------------------------------------- metrics

#[test]
fn the_venue_reports_what_it_actually_did() {
    // Counts are exact, so they can be checked against traffic sent. Timings
    // are sampled and cannot be, which is the trade the sampling buys.
    let venue = Running::start(false, None);
    let mut client = venue.connect();
    let batch: Vec<Command> = (1..=40).map(|id| order(ALICE, id, 10_500)).collect();
    send(&mut client, &batch);
    let events = collect_until(&mut client, Duration::from_secs(3), |seen| {
        answered(seen) >= 40
    });
    assert_eq!(kinds(&events, EventKind::Received), 40, "{events:?}");

    let report = reaches(&venue.commands, 40);
    assert!(
        report >= 40,
        "the venue applied 40 commands and counted {report}"
    );
    assert!(
        venue.sessions_accepted.load(Ordering::Relaxed) >= 1,
        "a connection was accepted and not counted"
    );
}

#[test]
fn a_refusal_is_counted_against_the_reason_it_was_refused() {
    // "Why is this client's fill rate down" is answered by which of these
    // climbs, so a refusal that is counted generically is no answer at all.
    let venue = Running::start(false, None);
    let mut client = venue.connect();

    // Outside the ladder, which is the price band.
    send(&mut client, &[order(ALICE, 1, 1)]);
    // Duplicate ID, after a good one.
    send(&mut client, &[order(ALICE, 2, 10_500)]);
    send(&mut client, &[order(ALICE, 2, 10_500)]);
    let _ = collect_until(&mut client, Duration::from_secs(3), |seen| {
        answered(seen) >= 3
    });

    let named = reaches(&venue.band_rejects, 1);
    assert!(
        named >= 1,
        "an order outside the band was refused but not counted as such"
    );
    assert!(
        reaches(&venue.duplicate_rejects, 1) >= 1,
        "a duplicate order ID was refused but not counted as such"
    );
}

// -------------------------------------------------------------- timestamps

#[test]
fn an_acknowledgement_carries_when_the_venue_saw_the_order() {
    // The field spent its first life as a reserved zero. What makes it real is
    // that it is stamped before sequencing and travels inside the command, so a
    // replay reproduces it instead of reading a clock.
    let venue = Running::start_stamped();
    let mut client = venue.connect();

    let before = wall_ns();
    send(&mut client, &[order(ALICE, 1, 10_500)]);
    let events = collect_until(&mut client, Duration::from_secs(3), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Received as u8)
    });
    let after = wall_ns();

    let ack = events
        .iter()
        .find(|e| e.kind == EventKind::Received as u8)
        .expect("no acknowledgement");
    let ingress = ack.ingress_ns();
    let matched = ack.match_ns();

    assert!(
        (before..=after).contains(&ingress),
        "arrival stamped {ingress}, outside the {before}..{after} the test spanned"
    );
    assert!(
        (before..=after).contains(&matched),
        "match stamped {matched}, outside the {before}..{after} the test spanned"
    );
    assert!(
        matched >= ingress,
        "the venue matched an order {} ns before it arrived",
        ingress - matched
    );
}

#[test]
fn a_venue_without_timestamps_stamps_nothing_rather_than_guessing() {
    // The measurement configuration, and the one every benchmark runs. A zero
    // here is a stated absence; a plausible-looking number would be a lie that
    // survives into somebody's compliance report.
    let venue = Running::start(false, None);
    let mut client = venue.connect();
    send(&mut client, &[order(ALICE, 1, 10_500)]);
    let events = collect_until(&mut client, Duration::from_secs(3), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Received as u8)
    });
    let ack = events
        .iter()
        .find(|e| e.kind == EventKind::Received as u8)
        .expect("no acknowledgement");
    assert_eq!(ack.ingress_ns(), 0);
    assert_eq!(ack.match_ns(), 0);
}

#[test]
fn every_order_in_one_group_shares_an_arrival_time() {
    // Stated rather than accidental: the clock is read once a pass, so the
    // resolution is the pass. A client comparing two orders from one write must
    // not read a difference into stamps that were never measured apart.
    let venue = Running::start_stamped();
    let mut client = venue.connect();
    let batch: Vec<Command> = (1..=32).map(|id| order(ALICE, id, 10_500)).collect();
    send(&mut client, &batch);

    let events = collect_until(&mut client, Duration::from_secs(3), |seen| {
        answered(seen) >= 32
    });
    let stamps: std::collections::BTreeSet<u64> = events
        .iter()
        .filter(|e| e.kind == EventKind::Received as u8)
        .map(Event::ingress_ns)
        .collect();
    assert!(!stamps.is_empty(), "nothing was acknowledged");
    assert!(
        stamps.iter().all(|stamp| *stamp > 0),
        "an order was acknowledged with no arrival time"
    );
    // A pass reads one buffer per session, so 32 orders may span a few passes;
    // what matters is that it is a handful of readings, not thirty-two.
    assert!(
        stamps.len() <= 8,
        "32 orders produced {} distinct arrival times, so the clock is being \
         read per command rather than per pass",
        stamps.len()
    );
}

/// Proving who you are must also bind what you may act as.
///
/// Authentication established identity at connect and then nothing tied a
/// command to it: a session that proved it held Alice's secret could put Bob's
/// account in the `account` field of an order, and the pipeline reserved Bob's
/// balance. That makes the whole handshake decorative -- anyone with any valid
/// credential could trade every account on the venue.
#[test]
fn an_authenticated_session_cannot_act_for_another_account() {
    let venue = Running::start(true, None);
    let mut client = venue.connect();
    let events = authenticate(&mut client, ALICE, &alice_key());
    assert_eq!(
        events.first().and_then(Event::kind),
        Some(EventKind::Authenticated),
        "Alice should have been let in"
    );

    // Alice, authenticated, sending an order that names Bob.
    send(&mut client, &[order(BOB, 9_001, 10_050)]);
    let events = collect_until(&mut client, Duration::from_secs(5), |seen| !seen.is_empty());

    assert_eq!(
        kinds(&events, EventKind::Received),
        0,
        "an order for another account was accepted: {events:?}"
    );
    let refusals: Vec<_> = events
        .iter()
        .filter(|e| e.kind == EventKind::Rejected as u8)
        .collect();
    assert_eq!(
        refusals.len(),
        1,
        "expected exactly one refusal: {events:?}"
    );
    assert_eq!(
        refusals[0].reject_reason(),
        Some(RejectReason::NotPermitted),
        "refused, but not for acting as somebody else"
    );

    // And Alice can still trade as herself, so the rule bounds nothing else.
    send(&mut client, &[order(ALICE, 9_002, 10_050)]);
    let events = collect_until(&mut client, Duration::from_secs(5), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Received as u8)
    });
    assert_eq!(kinds(&events, EventKind::Received), 1);
}

/// The same rule in the open venue: a session may not change identity midway.
///
/// Without authentication a session is attributed from its first command, which
/// is the venue trusting it. That is a measurement setting and documented as
/// one -- but it must still mean *one* account per session, or the private
/// channel and the rate-limit bucket belong to whoever the last command claimed.
#[test]
fn an_open_session_is_held_to_the_account_it_first_claimed() {
    let venue = Running::start(false, None);
    let mut client = venue.connect();

    send(&mut client, &[order(ALICE, 100, 10_050)]);
    let events = collect_until(&mut client, Duration::from_secs(5), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Received as u8)
    });
    assert_eq!(kinds(&events, EventKind::Received), 1);

    // Waits for a refusal specifically. Stopping at the first event of any kind
    // catches the trailing `Resting` from the order above and reports a pass or
    // a failure depending on scheduling.
    send(&mut client, &[order(BOB, 101, 10_050)]);
    let events = collect_until(&mut client, Duration::from_secs(5), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Rejected as u8)
    });
    assert!(
        events.iter().any(|e| e.kind == EventKind::Rejected as u8
            && e.reject_reason() == Some(RejectReason::NotPermitted)),
        "a session switched accounts midway: {events:?}"
    );
    assert_eq!(
        kinds(&events, EventKind::Received),
        0,
        "the order for another account was also acknowledged: {events:?}"
    );
}

/// Only the administrator may halt a symbol or stop an account.
///
/// The check sits in the gateway, before sequencing, so an unauthorised halt
/// never reaches the journal and never replays. A venue where any client can
/// halt the book has no kill switch, it has a denial of service.
#[test]
fn a_privileged_command_from_an_ordinary_account_is_refused() {
    let venue = Running::start_with_admin(ALICE);
    let mut client = venue.connect();
    authenticate(&mut client, BOB, &bob_key());

    send(
        &mut client,
        &[bx_pipeline::set_symbol_state(
            BOB,
            SYMBOL,
            bx_protocol::TradingState::Halted,
        )],
    );
    let events = collect_until(&mut client, Duration::from_secs(5), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Rejected as u8)
    });
    assert!(
        events.iter().any(|e| e.kind == EventKind::Rejected as u8
            && e.reject_reason() == Some(RejectReason::NotPermitted)),
        "an ordinary account halted a symbol: {events:?}"
    );

    // The symbol is still trading: Bob can place an order.
    send(&mut client, &[order(BOB, 1, 10_050)]);
    let events = collect_until(&mut client, Duration::from_secs(5), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Received as u8)
    });
    assert_eq!(
        kinds(&events, EventKind::Received),
        1,
        "the refused halt took effect anyway: {events:?}"
    );
}

/// And the administrator's halt does take effect, end to end.
#[test]
fn the_administrator_can_halt_a_symbol_and_orders_stop() {
    let venue = Running::start_with_admin(ALICE);
    let mut admin = venue.connect();
    authenticate(&mut admin, ALICE, &alice_key());

    let mut trader = venue.connect();
    authenticate(&mut trader, BOB, &bob_key());
    send(&mut trader, &[order(BOB, 10, 10_050)]);
    let events = collect_until(&mut trader, Duration::from_secs(5), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Received as u8)
    });
    assert_eq!(kinds(&events, EventKind::Received), 1);

    send(
        &mut admin,
        &[bx_pipeline::set_symbol_state(
            ALICE,
            SYMBOL,
            bx_protocol::TradingState::Halted,
        )],
    );
    // Wait for the venue to have applied it.
    let _ = collect_until(&mut admin, Duration::from_secs(5), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Received as u8)
    });

    send(&mut trader, &[order(BOB, 11, 10_050)]);
    let events = collect_until(&mut trader, Duration::from_secs(5), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Rejected as u8)
    });
    assert!(
        events.iter().any(|e| e.kind == EventKind::Rejected as u8
            && e.reject_reason() == Some(RejectReason::SymbolNotTrading)),
        "the halt did not reach the order path: {events:?}"
    );
}

/// An account may flatten itself without being the administrator.
///
/// Deliberately unprivileged: a client that has lost track of its own state must
/// be able to get out. Requiring an operator for that is how a bad afternoon
/// becomes a bad week.
#[test]
fn an_account_may_cancel_all_of_its_own_orders() {
    let venue = Running::start_with_admin(ALICE);
    let mut client = venue.connect();
    authenticate(&mut client, BOB, &bob_key());

    for id in 20..23 {
        send(&mut client, &[order(BOB, id, 10_050 + id as Ticks)]);
    }
    let _ = collect_until(&mut client, Duration::from_secs(5), |seen| {
        kinds(seen, EventKind::Resting) >= 3
    });

    send(&mut client, &[bx_pipeline::cancel_all(BOB, SYMBOL)]);
    let events = collect_until(&mut client, Duration::from_secs(5), |seen| {
        kinds(seen, EventKind::Canceled) >= 3
    });
    assert!(
        kinds(&events, EventKind::Canceled) >= 3,
        "cancel-all left orders resting: {events:?}"
    );
}

// ------------------------------------------------- the two-record handshake

/// A first half on its own proves nothing and lets nothing through.
///
/// The dangerous shape would be a venue that treated the first record as an
/// attempt and admitted the session on 32 of the 64 bytes.
#[test]
fn half_a_signature_does_not_authenticate() {
    let venue = Running::start(true, None);
    let mut client = venue.connect();
    let nonce = challenge_from(&mut client);
    let [first, _second] = Command::authenticating(ALICE, &signature(&alice_key(), &nonce));

    send(&mut client, &[first]);
    let events = collect_until(&mut client, Duration::from_secs(2), |seen| !seen.is_empty());
    assert_eq!(
        kinds(&events, EventKind::Authenticated),
        0,
        "half a signature authenticated: {events:?}"
    );

    // And the session still cannot trade.
    send(&mut client, &[order(ALICE, 1, 10_050)]);
    let events = collect_until(&mut client, Duration::from_secs(2), |seen| !seen.is_empty());
    assert!(
        events.iter().any(|e| e.kind == EventKind::Rejected as u8
            && e.reject_reason() == Some(RejectReason::NotAuthenticated)),
        "a half-authenticated session was allowed to send: {events:?}"
    );
}

/// A continuation with no first half is refused, not treated as a whole one.
#[test]
fn a_continuation_without_a_first_half_is_refused() {
    let venue = Running::start(true, None);
    let mut client = venue.connect();
    let nonce = challenge_from(&mut client);
    let [_first, second] = Command::authenticating(ALICE, &signature(&alice_key(), &nonce));

    send(&mut client, &[second]);
    let events = collect_until(&mut client, Duration::from_secs(2), |seen| !seen.is_empty());
    assert_eq!(kinds(&events, EventKind::Authenticated), 0);
    assert!(events.iter().any(|e| e.kind == EventKind::Rejected as u8));
}

/// The two halves must be adjacent. Anything between them abandons the attempt.
///
/// Otherwise a client could interleave two attempts and have the venue assemble
/// whichever pairing happened to verify.
#[test]
fn a_signature_split_by_other_traffic_is_abandoned() {
    let venue = Running::start(true, None);
    let mut client = venue.connect();
    let nonce = challenge_from(&mut client);
    let [first, second] = Command::authenticating(ALICE, &signature(&alice_key(), &nonce));

    // First half, then an order, then the continuation.
    send(&mut client, &[first, order(ALICE, 1, 10_050), second]);
    let events = collect_until(&mut client, Duration::from_secs(2), |seen| seen.len() >= 2);
    assert_eq!(
        kinds(&events, EventKind::Authenticated),
        0,
        "an interleaved signature authenticated: {events:?}"
    );
}

/// Halves from two different accounts cannot be spliced together.
#[test]
fn halves_naming_different_accounts_do_not_combine() {
    let venue = Running::start(true, None);
    let mut client = venue.connect();
    let nonce = challenge_from(&mut client);
    let [alice_first, _] = Command::authenticating(ALICE, &signature(&alice_key(), &nonce));
    let [_, bob_second] = Command::authenticating(BOB, &signature(&bob_key(), &nonce));

    send(&mut client, &[alice_first, bob_second]);
    let events = collect_until(&mut client, Duration::from_secs(2), |seen| !seen.is_empty());
    assert_eq!(
        kinds(&events, EventKind::Authenticated),
        0,
        "spliced halves authenticated: {events:?}"
    );
}

/// A signature over the bare nonce -- what a wallet signing an opaque string
/// elsewhere would produce -- is refused end to end.
///
/// The unit test covers the verifier; this covers the whole path, so a gateway
/// that reconstructed the message differently would be caught.
#[test]
fn a_signature_missing_the_domain_prefix_is_refused_over_the_wire() {
    let venue = Running::start(true, None);
    let mut client = venue.connect();
    let nonce = challenge_from(&mut client);
    let bare = alice_key().sign(&nonce).to_bytes();

    send(&mut client, &Command::authenticating(ALICE, &bare));
    let events = collect_until(&mut client, Duration::from_secs(2), |seen| !seen.is_empty());
    assert_eq!(
        kinds(&events, EventKind::Authenticated),
        0,
        "a signature without the venue's domain prefix authenticated: {events:?}"
    );
}

/// Both halves in one write are fine: a client pipelines them, as it should.
#[test]
fn both_halves_in_a_single_write_authenticate() {
    let venue = Running::start(true, None);
    let mut client = venue.connect();
    let events = authenticate(&mut client, ALICE, &alice_key());
    assert_eq!(kinds(&events, EventKind::Authenticated), 1);
}

// -------------------------------------------------------------- revocation

/// Revoking a key closes the sessions using it and stops the next logon.
///
/// Closing the open ones is the point. Dropping the key alone stops only the
/// *next* connection, and a stolen key whose session is already open could go on
/// cancelling orders -- cancels stay permitted under every other restriction, so
/// revocation has to reach the connection itself.
#[test]
fn revoking_a_key_closes_its_sessions_and_stops_the_next_logon() {
    let venue = Running::start_with_admin(ALICE);
    let mut admin = venue.connect();
    authenticate(&mut admin, ALICE, &alice_key());

    let mut victim = venue.connect();
    let events = authenticate(&mut victim, BOB, &bob_key());
    assert_eq!(kinds(&events, EventKind::Authenticated), 1);

    send(&mut admin, &[Command::revoking_key(BOB)]);
    let _ = collect_until(&mut admin, Duration::from_secs(5), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Received as u8)
    });

    // The open session is gone: the socket reaches end of stream.
    let mut closed = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let mut batch = Vec::new();
        match read_events(&mut victim, 1, &mut batch) {
            Ok(()) if batch.is_empty() => {
                closed = true;
                break;
            }
            Ok(()) => {}
            Err(_) => {}
        }
    }
    assert!(closed, "a revoked account's session stayed open");

    // And a fresh logon with the same key is refused.
    let mut again = venue.connect();
    let events = authenticate(&mut again, BOB, &bob_key());
    assert_eq!(
        kinds(&events, EventKind::Authenticated),
        0,
        "a revoked key authenticated again: {events:?}"
    );
}

/// Only the administrator may revoke.
#[test]
fn an_ordinary_account_cannot_revoke_a_key() {
    let venue = Running::start_with_admin(ALICE);
    let mut client = venue.connect();
    authenticate(&mut client, BOB, &bob_key());

    send(&mut client, &[Command::revoking_key(ALICE)]);
    let events = collect_until(&mut client, Duration::from_secs(5), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Rejected as u8)
    });
    assert!(
        events.iter().any(|e| e.kind == EventKind::Rejected as u8
            && e.reject_reason() == Some(RejectReason::NotPermitted)),
        "an ordinary account revoked a key: {events:?}"
    );

    // Alice's key still works.
    let mut admin = venue.connect();
    let events = authenticate(&mut admin, ALICE, &alice_key());
    assert_eq!(kinds(&events, EventKind::Authenticated), 1);
}

/// Funding is the venue's business, not a client's.
///
/// A deposit names the account it credits and nothing else, so a session that
/// may send one for its own account may credit itself any amount it likes.
/// Nothing else in the venue can catch it: the command is well formed, the
/// account is the session's own, and the exchange applies it exactly as an
/// operator's. Balance is the only thing standing between an order and the
/// book, so this is the whole risk system.
///
/// The same argument covers a withdrawal, which is the operator's tool for
/// moving an allotment between partitions.
#[test]
fn a_client_cannot_fund_or_drain_its_own_account() {
    let venue = Running::start_with_admin(ALICE);
    let mut stream = venue.connect();
    authenticate(&mut stream, BOB, &bob_key());

    for (name, command) in [
        ("deposit", deposit(BOB, USD, 1_000_000)),
        ("withdraw", withdraw(BOB, USD, 1_000_000)),
    ] {
        send(&mut stream, &[command]);
        let events = collect_until(&mut stream, Duration::from_secs(2), |seen| {
            seen.iter().any(|event| {
                event.kind == EventKind::Rejected as u8
                    && event.reject_reason() == Some(RejectReason::NotPermitted)
            })
        });
        assert!(
            events.iter().any(|event| {
                event.kind == EventKind::Rejected as u8
                    && event.reject_reason() == Some(RejectReason::NotPermitted)
            }),
            "a client sent a {name} for itself and the venue did not refuse it, \
             so any account can set its own balance"
        );
    }
}

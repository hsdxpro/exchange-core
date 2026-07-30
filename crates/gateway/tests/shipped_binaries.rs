//! The binaries a deployment actually runs, against the configuration file it
//! actually ships.
//!
//! Every other test in this crate drives the venue in-process, building a
//! `Server` directly and choosing its settings in Rust. That covers the venue's
//! behaviour and misses the thing a new user hits first: whether `venue` and
//! `load`, started the way the README says to start them, can talk to each
//! other at all. They could not. `venue.conf` shipped with authentication
//! required and `load` has no authentication support, so the documented
//! quickstart connected, was challenged, never answered, and reported a
//! "round trip" that was the client reading the challenge rather than an order
//! being matched -- a number that looked like a good result.
//!
//! These tests spawn the real executable. They are slower than the in-process
//! ones and they are the only ones that can catch a mismatch between a binary
//! and the file that configures it.

use bx_gateway::codec::encode;
use bx_pipeline::limit_order;
use bx_protocol::{Command, Event, EventKind, Side, Ticks};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command as Process, Stdio};
use std::time::{Duration, Instant};
use zerocopy::FromBytes;

const FRAME_LEN: usize = size_of::<Command>();
const SYMBOL: u32 = 1;
const FLOOR: Ticks = 10_000;
const ACCOUNT: u64 = 1;

/// A `venue` process, its working directory, and the port it chose.
struct Venue {
    child: Child,
    address: String,
    #[allow(dead_code)]
    dir: PathBuf,
}

impl Venue {
    /// Starts the shipped binary on a free port, with `edits` applied to the
    /// shipped configuration file.
    ///
    /// The configuration is copied from the repository's own `venue.conf`
    /// rather than written from scratch, because a test that writes its own
    /// configuration cannot catch the shipped one being wrong -- which is
    /// exactly the failure this file exists for.
    /// The measurement configuration, used exactly as shipped.
    fn from_bench_conf() -> Option<Self> {
        Self::from_file("bench.conf", &[])
    }

    fn start(edits: &[(&str, &str)]) -> Option<Self> {
        Self::from_file("venue.conf", edits)
    }

    fn from_file(name: &str, edits: &[(&str, &str)]) -> Option<Self> {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let shipped = repo.join(name);
        let text = std::fs::read_to_string(&shipped).ok()?;

        // A directory nobody else will pick, without asking the OS for a port
        // and then giving it back -- see `wait_until_listening` for why that
        // guess was worse than it looked.
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos());
        let dir = std::env::temp_dir().join(format!("bx_shipped_{}_{stamp}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).ok()?;

        let mut out = String::new();
        for line in text.lines() {
            // Section headers are matched whole; settings are matched on their
            // key. Both can be deleted, which is what opening authentication
            // needs: the parser refuses credentials that would never be checked,
            // so the `[credential]` header has to go with its contents.
            let trimmed = line.trim();
            let key = if trimmed.starts_with('[') {
                trimmed
            } else {
                line.split('=').next().unwrap_or("").trim()
            };
            let replaced = edits
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| (*value).to_string());
            match replaced {
                Some(value) if value.is_empty() => {}
                Some(value) => out.push_str(&format!("{key} = {value}\n")),
                None => {
                    out.push_str(line);
                    out.push('\n');
                }
            }
        }
        // Every fixed port in the shipped file goes, not only the order-entry
        // one. `bench.conf` names a market-data port so the load harness has
        // somewhere predictable to watch; a test that inherited it had two
        // venues fighting over one socket, and the loser spent a minute being
        // waited on for an address it was never going to give.
        let mut kept = String::with_capacity(out.len());
        for line in out.lines() {
            let head = line.trim_start();
            if ["feed_listen", "metrics_listen", "tls_listen"]
                .iter()
                .any(|fixed| head.starts_with(fixed))
            {
                continue;
            }
            kept.push_str(line);
            kept.push('\n');
        }
        out = kept;

        // The venue picks its own port and says which; see
        // `wait_until_listening` for why choosing one here was worse than it
        // looked. The journal must not be shared with the developer's own runs.
        out.push_str("\nlisten = 127.0.0.1:0\n");
        out.push_str(&format!("journal = {}\n", dir.join("v.log").display()));
        out.push_str(&format!("snapshot = {}\n", dir.join("v.snap").display()));

        let config = dir.join("venue.conf");
        std::fs::write(&config, out).ok()?;

        let child = Process::new(env!("CARGO_BIN_EXE_venue"))
            .arg("--config")
            .arg(&config)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .ok()?;

        let mut venue = Self {
            child,
            address: String::new(),
            dir,
        };
        venue.wait_until_listening();
        Some(venue)
    }

    /// Waits for the venue to say which port it took, and takes that as the
    /// address.
    ///
    /// This used to pick a port itself -- bind zero, read the number, close the
    /// socket, write it into the configuration -- and then poll until a connect
    /// succeeded. Both halves were wrong in the same way. The port could be
    /// taken between the close and the venue's bind, and two tests could be
    /// handed the same number; worse, a *successful* connect was taken as proof
    /// that our venue had started, so when somebody else held the port the test
    /// talked to them and never noticed its own child had died. That is exactly
    /// how it failed: an earlier test's venue, running open, answered a load
    /// client that should have been refused for want of credentials, and the
    /// assertion about authentication fired against the wrong process.
    ///
    /// Asking the venue removes the guess. It binds zero itself and prints the
    /// address it got, so the test cannot be confused about whose venue it is
    /// speaking to.
    ///
    /// Panics rather than returning, because a venue that will not start is a
    /// failure worth seeing. Returning `None` here made a broken configuration
    /// look like an absent binary and quietly skipped the test.
    fn wait_until_listening(&mut self) {
        let Some(stdout) = self.child.stdout.take() else {
            panic!("venue was spawned without a pipe to read its address from");
        };
        let seen = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let filling = std::sync::Arc::clone(&seen);
        std::thread::spawn(move || {
            let mut reader = std::io::BufReader::new(stdout);
            let mut line = String::new();
            while std::io::BufRead::read_line(&mut reader, &mut line).unwrap_or(0) > 0 {
                if let Ok(mut held) = filling.lock() {
                    held.push_str(&line);
                }
                line.clear();
            }
        });

        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            if let Ok(held) = seen.lock()
                && let Some(address) = held
                    .lines()
                    .find_map(|line| line.strip_prefix("listening "))
            {
                self.address = address.trim().to_string();
                return;
            }
            // Checked before the address, not only when a connect fails: a
            // venue that exited must be reported as itself rather than waited
            // out.
            if let Ok(Some(status)) = self.child.try_wait() {
                let mut stderr = String::new();
                if let Some(mut pipe) = self.child.stderr.take() {
                    let _ = pipe.read_to_string(&mut stderr);
                }
                let logged = seen.lock().map(|held| held.clone()).unwrap_or_default();
                panic!(
                    "venue exited with {status} before listening:
{logged}
{stderr}"
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let logged = seen.lock().map(|held| held.clone()).unwrap_or_default();
        panic!(
            "venue never said which port it took within 60s:
{logged}"
        );
    }

    fn connect(&self) -> TcpStream {
        let stream = TcpStream::connect(&self.address).expect("venue refused a connection");
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_millis(250)))
            .unwrap();
        stream
    }
}

/// Reads until `enough` is satisfied or `window` closes, returning every whole
/// event that arrived.
///
/// A predicate rather than a fixed window, for two reasons that pull in
/// opposite directions. Guessing an exact event count blocks until the socket
/// times out when the guess is high. A fixed window fails on a loaded machine
/// when the venue -- one of several processes these tests start at once -- takes
/// longer than the window to say anything at all. Waiting for the condition and
/// capping it generously satisfies both: fast when the venue answers, tolerant
/// when the machine is busy.
fn collect_until(
    stream: &mut TcpStream,
    window: Duration,
    enough: impl Fn(&[Event]) -> bool,
) -> Vec<Event> {
    let mut scratch = vec![0_u8; FRAME_LEN * 1_024];
    let mut held = 0_usize;
    let mut events = Vec::new();
    let deadline = Instant::now() + window;
    while Instant::now() < deadline && !enough(&events) {
        match stream.read(&mut scratch[held..]) {
            Ok(0) => break,
            Ok(bytes) => {
                let filled = held + bytes;
                let whole = filled / FRAME_LEN;
                for index in 0..whole {
                    let start = index * FRAME_LEN;
                    if let Ok(event) = Event::read_from_bytes(&scratch[start..start + FRAME_LEN]) {
                        events.push(event);
                    }
                }
                scratch.copy_within(whole * FRAME_LEN..filled, 0);
                held = filled - whole * FRAME_LEN;
            }
            Err(_) => {}
        }
    }
    events
}

fn orders(count: u64, first: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(count as usize * FRAME_LEN);
    for i in 0..count {
        let price = FLOOR + 1_000 + (i % 4_000) as Ticks;
        encode(
            &limit_order(ACCOUNT, SYMBOL, first + i, Side::Bid, price, 1),
            &mut bytes,
        );
    }
    bytes
}

fn count_of(events: &[Event], kind: EventKind) -> usize {
    events.iter().filter(|e| e.kind == kind as u8).count()
}

/// The shipped configuration, unmodified, with a client that does not
/// authenticate.
///
/// This is the documented quickstart. It must not look like it worked. Before
/// this test the venue answered an unauthenticated client with a challenge and
/// nothing else, and `load` counted that challenge as a round trip -- so the
/// broken case printed a plausible latency instead of an error.
#[test]
fn the_shipped_config_refuses_an_unauthenticated_client_visibly() {
    let Some(venue) = Venue::start(&[]) else {
        eprintln!("skipping: venue binary or venue.conf unavailable");
        return;
    };
    let mut client = venue.connect();

    client.write_all(&orders(20, 1)).unwrap();
    client.flush().unwrap();
    // Waits for the first event, however long the loaded machine takes to
    // deliver it; a fixed short window here failed whenever the other tests in
    // this file had venues starting at the same moment.
    let events = collect_until(&mut client, Duration::from_secs(15), |seen| {
        !seen.is_empty()
    });

    // Whatever else happens, orders must not be acknowledged: the session never
    // proved who it is.
    assert_eq!(
        count_of(&events, EventKind::Received),
        0,
        "the venue acknowledged orders from a session that never authenticated"
    );
    // And the client must be told something, rather than left to time out.
    assert!(
        !events.is_empty(),
        "an unauthenticated client was told nothing at all, so it cannot tell \
         a venue that requires authentication from one that has hung"
    );
}

/// The same binary with authentication opened: orders are acknowledged, one
/// acknowledgement per order.
///
/// This is the property `load` relies on and had no test behind it.
#[test]
fn every_order_is_acknowledged_exactly_once() {
    let Some(venue) = Venue::start(&[
        ("authentication", "open"),
        ("[credential]", ""),
        ("account", ""),
        ("public_key", ""),
    ]) else {
        eprintln!("skipping: venue binary or venue.conf unavailable");
        return;
    };

    const COUNT: u64 = 200;
    let mut client = venue.connect();
    client.write_all(&orders(COUNT, 10_000_000)).unwrap();
    client.flush().unwrap();

    let events = collect_until(&mut client, Duration::from_secs(20), |seen| {
        count_of(seen, EventKind::Received) as u64 >= COUNT
    });
    assert_eq!(
        count_of(&events, EventKind::Received) as u64,
        COUNT,
        "expected one acknowledgement per order; got {} events of kinds {:?}",
        events.len(),
        {
            let mut kinds: Vec<u8> = events.iter().map(|e| e.kind).collect();
            kinds.sort_unstable();
            kinds.dedup();
            kinds
        }
    );
}

/// The documented benchmark: `venue --config bench.conf` with `load` pointed at
/// it, every phase completing and every order accounted for.
///
/// This is the whole point of the file. Both binaries can be individually
/// correct and still be unable to work together, and nothing else in the suite
/// runs them as a pair. Before this existed the documented commands produced a
/// fabricated latency and then an I/O error.
#[test]
fn the_documented_benchmark_completes_with_every_order_accounted_for() {
    let Some(venue) = Venue::from_bench_conf() else {
        eprintln!("skipping: venue binary or bench.conf unavailable");
        return;
    };

    // Small enough to stay quick, large enough to exercise every phase.
    let output = Process::new(env!("CARGO_BIN_EXE_load"))
        .arg(&venue.address)
        .args(["--orders", "4000", "--clients", "4"])
        .output()
        .expect("load binary failed to start");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "the documented benchmark failed.\nstatus: {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status
    );
    for phase in [
        "round trip",
        "pipelined throughput",
        "orders acknowledged",
        "clients at once",
    ] {
        assert!(
            stdout.contains(phase),
            "`load` never reported the `{phase}` phase.\nstdout:\n{stdout}"
        );
    }
    // A benchmark configuration that provokes a warning is not measuring what
    // it says. Rejections mean the rate limiter is the bottleneck; unanswered
    // orders mean the run ended early. Either makes the numbers meaningless,
    // and both were happening.
    assert!(
        !stderr.contains("WARNING"),
        "the benchmark configuration produced warnings, so its numbers do not \
         measure matching:\n{stderr}"
    );
}

/// Against the shipped `venue.conf`, `load` says why it cannot measure.
///
/// The failure being guarded is not that it fails -- it should, since it cannot
/// authenticate -- but that it used to fail while printing a plausible
/// latency. It counted the venue's authentication challenge as a reply and
/// reported ten microseconds for a session that never traded.
#[test]
fn against_the_shipped_config_load_names_the_reason_instead_of_inventing_a_number() {
    let Some(venue) = Venue::start(&[]) else {
        eprintln!("skipping: venue binary or venue.conf unavailable");
        return;
    };

    let output = Process::new(env!("CARGO_BIN_EXE_load"))
        .arg(&venue.address)
        .args(["--orders", "100", "--clients", "2"])
        .output()
        .expect("load binary failed to start");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let combined = format!("{stdout}{}", String::from_utf8_lossy(&output.stderr));

    assert!(
        !output.status.success(),
        "load claimed success against a venue it cannot authenticate to:\n{stdout}"
    );
    assert!(
        combined.contains("authentication"),
        "load failed without naming authentication as the cause:\n{combined}"
    );
    // The specific regression: a latency line for a session that never traded.
    assert!(
        !stdout.contains("us"),
        "load printed a latency measurement for a session that never placed an \
         order:\n{stdout}"
    );
}

/// A second connection on an account that has already traded is acknowledged
/// too.
///
/// `load` opens one connection for its round-trip phase and a fresh one for its
/// throughput phase, both as account 1, so this ordering is the one the
/// documented command actually exercises.
#[test]
fn a_second_connection_on_a_traded_account_is_acknowledged() {
    let Some(venue) = Venue::start(&[
        ("authentication", "open"),
        ("[credential]", ""),
        ("account", ""),
        ("public_key", ""),
    ]) else {
        eprintln!("skipping: venue binary or venue.conf unavailable");
        return;
    };

    {
        let mut first = venue.connect();
        first.write_all(&orders(50, 1_000)).unwrap();
        first.flush().unwrap();
        let seen = collect_until(&mut first, Duration::from_secs(15), |seen| {
            count_of(seen, EventKind::Received) >= 50
        });
        assert_eq!(
            count_of(&seen, EventKind::Received),
            50,
            "the first connection was not fully acknowledged"
        );
    }

    let mut second = venue.connect();
    second.write_all(&orders(100, 20_000_000)).unwrap();
    second.flush().unwrap();
    let events = collect_until(&mut second, Duration::from_secs(15), |seen| {
        count_of(seen, EventKind::Received) >= 100
    });
    assert_eq!(
        count_of(&events, EventKind::Received),
        100,
        "a second connection on an account that had already traded was not \
         acknowledged; this is the sequence the documented `load` command runs"
    );
}

//! A scrape endpoint, on a thread that cannot touch the venue.
//!
//! The counters already exist and already print to the log. What was missing is
//! a way for a monitoring system to read them without a person: a degraded
//! majority, a rising shed count or a stalled commit has to page somebody while
//! the venue keeps serving, and nothing pages on stdout.
//!
//! Two rules shape this, and both are about not letting observability become an
//! outage:
//!
//! - **The venue never blocks on a scraper.** The trading thread hands over a
//!   finished string and moves on; it never waits for a socket, and if the
//!   serving thread happens to hold the lock the publish is skipped rather than
//!   queued. A scrape is a snapshot, so the next one is as good as this one.
//! - **The venue never formats on demand.** Text is produced on the cadence the
//!   venue already reports on, not when a scraper asks. An endpoint that did
//!   work per request would let whoever scrapes decide how much work the venue
//!   does.
//!
//! HTTP by hand rather than a framework: the response is a status line, two
//! headers and a body, and the alternative is an async runtime and a few hundred
//! crates reaching into a process whose whole point is that nothing unexamined
//! runs in it.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// How long to wait between accept attempts when nothing is connecting.
const IDLE: Duration = Duration::from_millis(50);

/// How long one connection may take to say what it wants.
///
/// Requests are served one at a time: this endpoint has a thread, not a thread
/// pool, because it exists to answer a scraper every fifteen seconds and a pool
/// is machinery for a problem it does not have. The cost of that choice is that
/// a client which connects and says nothing delays the next scrape, so the wait
/// is short and the answer is sent whether the request arrived or not -- there
/// is only one thing to say, and saying it does not depend on being asked
/// nicely. With a five-second wait here, one silent connection blinded
/// monitoring for five seconds; the test that found it is below.
const MOST_SILENCE: Duration = Duration::from_millis(200);

/// The most of a request this reads before answering.
///
/// A scraper sends a request line and a few headers. Anything larger is not a
/// scraper, and reading it to the end would let a client decide how much memory
/// this holds.
const MOST_REQUEST: usize = 4 * 1024;

/// Serves the latest published text to whoever scrapes it.
#[derive(Debug)]
pub struct Exporter {
    /// Where it actually bound, which matters when the port was left to the OS.
    bound: std::net::SocketAddr,
    latest: Arc<Mutex<String>>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Exporter {
    /// Binds the endpoint and starts serving on its own thread.
    ///
    /// # Errors
    /// Fails if the address cannot be parsed or bound.
    pub fn start(address: &str) -> std::io::Result<Self> {
        let listener = TcpListener::bind(address)?;
        let bound = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let latest = Arc::new(Mutex::new(String::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let served = Arc::clone(&latest);
        let flag = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            while !flag.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => answer(stream, &served),
                    // Nothing waiting. Sleeping rather than spinning: this
                    // thread shares a machine with a venue that wants its
                    // cores.
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(IDLE);
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(Self {
            bound,
            latest,
            stop,
            thread: Some(thread),
        })
    }

    /// Where the endpoint ended up. Useful when the port was left to the OS.
    #[must_use]
    pub const fn address(&self) -> std::net::SocketAddr {
        self.bound
    }

    /// Hands over the text the next scrape will receive.
    ///
    /// Never blocks. If the serving thread holds the lock this pass, the text is
    /// dropped and the next publish carries fresher numbers anyway.
    pub fn publish(&self, text: String) {
        if let Ok(mut held) = self.latest.try_lock() {
            *held = text;
        }
    }
}

impl Drop for Exporter {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// One request, one response. Nothing about the request is honoured beyond
/// reading it off the socket: this endpoint has one thing to say.
fn answer(mut stream: std::net::TcpStream, latest: &Arc<Mutex<String>>) {
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(MOST_SILENCE));
    let _ = stream.set_write_timeout(Some(MOST_SILENCE));
    let mut buffer = [0_u8; MOST_REQUEST];
    // Read once, and do not insist. Draining what the client sent keeps the
    // close from arriving as a reset that truncates the response; whether it
    // sent anything does not change what comes back.
    let _ = stream.read(&mut buffer);
    let body = latest.lock().map(|held| held.clone()).unwrap_or_default();
    let response = format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: text/plain; version=0.0.4\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpStream;

    /// One scrape, spoken the way a scraper speaks it.
    fn scrape(address: std::net::SocketAddr) -> String {
        let mut stream = TcpStream::connect(address).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: venue\r\n\r\n")
            .unwrap();
        let mut answer = String::new();
        let _ = stream.read_to_string(&mut answer);
        answer
    }

    #[test]
    fn a_scrape_gets_the_last_published_text() {
        let exporter = Exporter::start("127.0.0.1:0").unwrap();
        exporter.publish("bx_commands_total 7\n".to_string());
        let answer = scrape(exporter.address());
        assert!(answer.starts_with("HTTP/1.1 200 OK"), "{answer}");
        assert!(
            answer.contains("Content-Type: text/plain"),
            "a scraper needs the content type: {answer}"
        );
        assert!(
            answer.ends_with("bx_commands_total 7\n"),
            "the body was not the published text: {answer}"
        );
    }

    #[test]
    fn a_scrape_before_anything_is_published_still_answers() {
        // A venue scraped in its first seconds has counted nothing yet. An
        // empty body is the honest answer; a refused connection would page
        // somebody about a venue that is fine.
        let exporter = Exporter::start("127.0.0.1:0").unwrap();
        let answer = scrape(exporter.address());
        assert!(answer.starts_with("HTTP/1.1 200 OK"), "{answer}");
        assert!(answer.contains("Content-Length: 0"), "{answer}");
    }

    #[test]
    fn publishing_never_blocks_on_a_scraper() {
        // The property that keeps observability from becoming an outage: the
        // venue hands over text and moves on, whatever a scraper is doing.
        let exporter = Exporter::start("127.0.0.1:0").unwrap();
        let held = exporter.latest.lock().unwrap();
        let started = std::time::Instant::now();
        exporter.publish("dropped\n".to_string());
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "publish waited on a held lock"
        );
        drop(held);
        // And the skipped publish is not fatal: the next one lands.
        exporter.publish("bx_commands_total 1\n".to_string());
        assert!(scrape(exporter.address()).ends_with("bx_commands_total 1\n"));
    }

    #[test]
    fn a_client_that_says_nothing_does_not_hold_the_endpoint() {
        // Connect and stay silent. The read timeout has to bound it, or one
        // idle connection would stop every later scrape.
        let exporter = Exporter::start("127.0.0.1:0").unwrap();
        exporter.publish("bx_commands_total 3\n".to_string());
        let silent = TcpStream::connect(exporter.address()).unwrap();
        // Second scrape behaves, and must not wait on the first.
        let answer = scrape(exporter.address());
        assert!(
            answer.ends_with("bx_commands_total 3\n"),
            "a silent client blocked the endpoint: {answer}"
        );
        drop(silent);
    }
}

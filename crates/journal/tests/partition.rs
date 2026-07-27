//! A partitioned follower, emulated honestly on one machine.
//!
//! The scenario that could not be tested here before: a follower that is alive
//! at the TCP level and gone at the application level -- wedged, paused, or on
//! the far side of a black-holed link. The leader's write must hit its
//! deadline and fail, not block the venue's only thread.
//!
//! What made it untestable was Windows' dynamic send buffering: with no
//! explicit SO_SNDBUF, the kernel absorbed 128 MB from a socket whose peer
//! never read a byte, so `write_all` returned instantly and the write timeout
//! guarded a path that could not be reached on loopback. A test written that
//! way passed with the guard removed, which is worse than no test.
//!
//! Replication sockets now set their buffer sizes explicitly -- which is also
//! what bounds the kernel memory a stalled follower can cost. That makes the
//! blocked-write path reachable on loopback: fill both bounded buffers and the
//! write must wait on the peer, exactly as it would across a real network with
//! a closed receive window.

use bx_journal::{LogStorage, MemoryLog, ReplicatedLog, bound_listener};
use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

/// Comfortably past both bounded socket buffers (4 MiB each way), so the write
/// cannot complete into the kernel and must actually wait on the peer.
const GROUP: usize = 32 << 20;

const ACK_TIMEOUT: Duration = Duration::from_millis(250);

#[test]
fn a_partitioned_follower_fails_the_write_deadline_instead_of_blocking_forever() {
    // The listener every real follower uses, so the fake one negotiates the
    // same bounded window at the handshake. This is load-bearing, and the two
    // wrong versions were both measured absorbing all 32 MiB on loopback: a
    // plain listener lets the platform auto-tune the window to whatever it
    // likes, and shrinking the buffer after `accept` is too late because the
    // window was already advertised. Bounded at the listener, the sender
    // stopped inside 64 KiB.
    let listener = bound_listener("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();

    // Alive at the TCP level, gone at the application level. The socket is
    // held open long enough that nothing below can be explained by a reset.
    //
    // When it finally wakes it drains what the kernel accepted on its behalf
    // and reports the count. That number is the venue's actual exposure: every
    // byte of it was leader-side memory a stalled follower tied up. It is also
    // what catches the subtlest regression here -- writes issued as one giant
    // send get absorbed in full, sailing past the buffer bounds and the write
    // deadline both, and every other observable in this test looks identical
    // when that happens.
    let stalled = thread::spawn(move || {
        use std::io::Read;
        let (mut stream, _) = listener.accept().unwrap();
        thread::sleep(Duration::from_secs(12));
        stream
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        let mut sink = vec![0_u8; 1 << 20];
        let mut drained = 0_usize;
        loop {
            match stream.read(&mut sink) {
                Ok(0) | Err(_) => break,
                Ok(bytes) => drained += bytes,
            }
        }
        drained
    });

    let mut log = ReplicatedLog::connect(MemoryLog::new(), &[address], ACK_TIMEOUT, 1).unwrap();
    log.append(&vec![0_u8; GROUP]).unwrap();

    // The sync runs on its own thread so that the failure mode being guarded
    // against -- a write with no deadline blocking forever -- fails this test
    // at the deadline below instead of hanging the whole suite. If the write
    // does block, the stalled peer's socket drops after twelve seconds and the
    // thread unblocks on the reset, so nothing leaks either way.
    let (finished, awaited) = mpsc::channel();
    let sync = thread::spawn(move || {
        let started = Instant::now();
        let outcome = log.sync();
        let _ = finished.send((outcome.is_err(), started.elapsed()));
    });

    let (errored, waited) = awaited.recv_timeout(Duration::from_secs(6)).expect(
        "the leader has been blocked for six seconds writing to a follower \
         that stopped reading; without a write deadline this is the venue's \
         one thread stopping for as long as the partition lasts",
    );
    sync.join().unwrap();

    assert!(
        errored,
        "a group no quorum ever read was reported as acknowledged"
    );
    // The deadline is 250ms per socket operation; a couple of seconds covers
    // scheduler noise on a loaded machine. What is being asserted is the
    // *order of magnitude*: a bounded wait, not the length of the partition.
    assert!(
        waited < Duration::from_secs(4),
        "the write deadline took {waited:?} to fire against a 250ms timeout"
    );

    // The exposure bound. Buffers are 4 MiB a side and flow control was
    // measured stopping the sender at about three buffers' worth, so half the
    // group is a generous ceiling. If the whole 32 MiB is drainable, the
    // kernel absorbed the group wholesale -- which is what a single unsliced
    // send does, and what makes both the deadline and the memory bound above
    // theater.
    let drained = stalled.join().unwrap();
    assert!(
        drained < GROUP / 2,
        "a follower that never read a byte while the leader was writing was \
         nevertheless owed {drained} of {GROUP} bytes by the kernel; the \
         buffer bounds are not bounding anything"
    );
}

/// The buffers really are bounded, on both platforms this builds for.
///
/// Without this, the partition test above could pass for the wrong reason on a
/// platform whose defaults happen to be small, while the actual configuration
/// silently failed and left kernel memory unbounded on the platform where it
/// matters.
#[test]
fn replication_sockets_carry_bounded_buffers() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let accepted = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        thread::sleep(Duration::from_millis(500));
        drop(stream);
    });

    let log = ReplicatedLog::connect(MemoryLog::new(), &[address], ACK_TIMEOUT, 1).unwrap();
    let (send, recv) = log.follower_buffer_sizes().expect("socket query failed");

    // The configured value is 4 MiB. Windows reports back what was set; Linux
    // reports double, accounting its own bookkeeping. Both bounds matter: the
    // floor catches the configuration being silently dropped -- an unset
    // Windows socket reports its 64 KiB nominal default while dynamically
    // absorbing whatever is written, which is exactly the unbounded state this
    // guards against -- and the ceiling catches it being set absurdly large.
    let configured = 4 << 20;
    for (name, size) in [("send", send), ("recv", recv)] {
        assert!(
            size >= configured && size <= configured * 2,
            "the {name} buffer reports {size} bytes against a configured \
             {configured}; the explicit bound was not applied"
        );
    }
    accepted.join().unwrap();
}

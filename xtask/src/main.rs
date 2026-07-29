//! Local task runner. Everything goes through cargo; there is no CI, no make,
//! no just, and nothing to install.
//!
//!   cargo x            format check, lint, all tests
//!   cargo x test       all tests
//!   cargo x e2e        end-to-end tests only, with output
//!   cargo x latency    pipeline latency
//!   cargo x engine     the matching engine's own verification and benchmark
//!   cargo x all        everything above, in order

use std::process::{Command, ExitCode};

fn run(args: &[&str]) -> bool {
    println!("\n\x1b[1m==> cargo {}\x1b[0m", args.join(" "));
    Command::new(env!("CARGO"))
        .args(args)
        .status()
        .is_ok_and(|status| status.success())
}

/// The whole suite, with the gateway's tests held to one at a time.
///
/// Everything in `bx-gateway`'s test directory binds a port, and most of it
/// spawns a venue process or the load binary alongside. Run in parallel on a
/// machine with cores to spare that is fine; run in parallel on a shared
/// two-core runner it is a queue, and the symptoms are not obviously about
/// contention -- a loopback connect that times out after twenty seconds, a
/// venue that takes longer to print `listening` than the test waited. Both
/// were seen in CI, on tests that were not broken.
///
/// The rest of the workspace is compute, not ports and processes, and keeps
/// its parallelism.
fn tests() -> bool {
    run(&["test", "--workspace", "--exclude", "bx-gateway"])
        && run(&["test", "-p", "bx-gateway", "--", "--test-threads=1"])
}

fn gate() -> bool {
    run(&["fmt", "--all", "--", "--check"])
        && run(&[
            "clippy",
            "--workspace",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ])
        && tests()
}

fn main() -> ExitCode {
    let task = std::env::args().nth(1).unwrap_or_else(|| "gate".to_owned());

    let ok = match task.as_str() {
        "gate" => gate(),
        "test" => tests(),
        "e2e" => run(&[
            "test",
            "-p",
            "bx-pipeline",
            "--test",
            "end_to_end",
            "--",
            "--nocapture",
        ]),
        "latency" => run(&["run", "--release", "-p", "bx-pipeline", "--bin", "latency"]),
        "engine" => run(&["run", "--release", "-p", "bx-engine", "--bin", "bx-bench"]),
        "all" => {
            gate()
                && run(&[
                    "run",
                    "--release",
                    "-p",
                    "bx-engine",
                    "--bin",
                    "bx-bench",
                    "--",
                    "--quick",
                ])
                && run(&["run", "--release", "-p", "bx-pipeline", "--bin", "latency"])
        }
        other => {
            eprintln!("unknown task: {other}");
            eprintln!("try: gate, test, e2e, latency, engine, all");
            return ExitCode::FAILURE;
        }
    };

    if ok {
        println!("\n\x1b[1;32mOK\x1b[0m");
        ExitCode::SUCCESS
    } else {
        println!("\n\x1b[1;31mFAILED\x1b[0m");
        ExitCode::FAILURE
    }
}

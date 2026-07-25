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
        && run(&["test", "--workspace"])
}

fn main() -> ExitCode {
    let task = std::env::args().nth(1).unwrap_or_else(|| "gate".to_owned());

    let ok = match task.as_str() {
        "gate" => gate(),
        "test" => run(&["test", "--workspace"]),
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

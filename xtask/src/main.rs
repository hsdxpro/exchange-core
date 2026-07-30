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

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    /// Run of spaces that separates a mangled literal from a deliberate one.
    ///
    /// Aligned output uses a few spaces on purpose -- the metrics report lines
    /// up columns with two to six. A joined line carries the indentation of the
    /// continuation as well, which is a dozen or more. Ten sits in the gap.
    const MANGLED: usize = 10;

    fn sources(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name == "target") {
                    continue;
                }
                sources(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }

    /// No string literal carries a run of spaces where a line continuation was
    /// meant.
    ///
    /// Writing Rust through a shell heredoc turns `\` at end of line into a
    /// literal escape, and the joined string keeps the indentation of the line
    /// below as run-on spaces. It is invisible to the compiler, invisible to
    /// the formatter, and perfectly visible to whoever reads the message --
    /// which twice was an operator, in a venue's startup output and in a
    /// configuration refusal. It happened five times before this test existed.
    #[test]
    fn no_string_literal_has_a_line_continuation_flattened_into_spaces() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xtask sits inside the workspace")
            .to_path_buf();
        let mut files = Vec::new();
        sources(&root, &mut files);
        assert!(
            !files.is_empty(),
            "found no sources to check under {root:?}"
        );

        let run = " ".repeat(MANGLED);
        let mut found = Vec::new();
        for path in &files {
            let Ok(text) = std::fs::read_to_string(path) else {
                continue;
            };
            for (number, line) in text.lines().enumerate() {
                // Only inside a literal: indentation is spaces too, and every
                // line of a continued string starts with plenty of it.
                let Some(opened) = line.find('"') else {
                    continue;
                };
                if line[opened..].contains(&run) {
                    found.push(format!(
                        "{}:{}: {}",
                        path.display(),
                        number + 1,
                        line.trim()
                    ));
                }
            }
        }
        assert!(
            found.is_empty(),
            "string literals with {MANGLED}+ spaces inside them, which is a \
             line continuation that was eaten rather than text anybody wrote:\n{}",
            found.join("\n")
        );
    }
}

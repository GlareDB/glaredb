pub mod args;
mod printer;
pub mod trial;

use std::process::{self, ExitCode};
use std::time::Instant;

use clap::Args;
use printer::Printer;
use trial::{Outcome, TestInfo, Trial};

pub use crate::args::{Arguments, ColorSetting, FormatSetting};

/// Contains information about the entire test run. Is returned by[`run`].
///
/// This type is marked as `#[must_use]`. Usually, you just call `exit()` on the
/// result of `run` to exit the application with the correct exit code. But you
/// can also store this value and inspect its data.
#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use = "Call `exit()` or `exit_if_failed()` to set the correct return code"]
pub struct Conclusion {
    /// Number of tests and benchmarks that were filtered out (either by the
    /// filter-in pattern or by `--skip` arguments).
    pub num_filtered_out: u64,
    /// Number of passed tests.
    pub num_passed: u64,
    /// Number of failed tests and benchmarks.
    pub num_failed: u64,
    /// Number of ignored tests and benchmarks.
    pub num_ignored: u64,
    /// Number of benchmarks that successfully ran.
    pub num_measured: u64,
}

impl Conclusion {
    /// Returns an exit code that can be returned from `main` to signal
    /// success/failure to the calling process.
    pub fn exit_code(&self) -> ExitCode {
        if self.has_failed() {
            ExitCode::from(101)
        } else {
            ExitCode::SUCCESS
        }
    }

    /// Returns whether there have been any failures.
    pub fn has_failed(&self) -> bool {
        self.num_failed > 0
    }

    /// Exits the application with an appropriate error code (0 if all tests
    /// have passed, 101 if there have been failures). This uses
    /// [`process::exit`], meaning that destructors are not ran. Consider
    /// using [`Self::exit_code`] instead for a proper program cleanup.
    pub fn exit(&self) -> ! {
        self.exit_if_failed();
        process::exit(0);
    }

    /// Exits the application with error code 101 if there were any failures.
    /// Otherwise, returns normally. This uses [`process::exit`], meaning that
    /// destructors are not ran. Consider using [`Self::exit_code`] instead for
    /// a proper program cleanup.
    pub fn exit_if_failed(&self) {
        if self.has_failed() {
            process::exit(101)
        }
    }

    fn empty() -> Self {
        Self {
            num_filtered_out: 0,
            num_passed: 0,
            num_failed: 0,
            num_ignored: 0,
            num_measured: 0,
        }
    }
}

/// Runs all given trials (tests & benchmarks).
///
/// This is the central function of this crate. It provides the framework for
/// the testing harness. It does all the printing and house keeping.
///
/// The returned value contains a couple of useful information. See `Conclusion`
/// for more information. If `--list` was specified, a list is printed and a
/// dummy `Conclusion` is returned.
pub fn run<A>(args: &Arguments<A>, mut tests: Vec<Trial>) -> Conclusion
where
    A: Args,
{
    let start_instant = Instant::now();
    let mut conclusion = Conclusion::empty();

    // Apply filtering
    if args.filter.is_some() || !args.skip.is_empty() || args.ignored {
        let len_before = tests.len() as u64;
        tests.retain(|test| !args.is_filtered_out(test));
        conclusion.num_filtered_out = len_before - tests.len() as u64;
    }
    let tests = tests;

    // Create printer which is used for all output.
    let mut printer = match &args.logfile {
        Some(path) => Printer::with_logfile(path, args.printer_options(), &tests),
        None => Printer::new(args.printer_options(), &tests),
    };

    // If `--list` is specified, just print the list and return.
    if args.list {
        printer.print_list(&tests, args.ignored);
        return Conclusion::empty();
    }

    // Print number of tests
    printer.print_title(tests.len() as u64);

    let mut failed_tests = Vec::new();
    let mut handle_outcome = |outcome: Outcome, test: TestInfo, printer: &mut Printer| {
        printer.print_single_outcome(&test, &outcome);

        // Handle outcome
        match outcome {
            Outcome::Passed => conclusion.num_passed += 1,
            Outcome::Failed(failed) => {
                failed_tests.push((test, failed.msg));
                conclusion.num_failed += 1;
            }
            Outcome::Ignored => conclusion.num_ignored += 1,
            Outcome::Measured(_) => conclusion.num_measured += 1,
        }
    };

    // Execute all tests.
    let test_mode = !args.bench;

    // Run test sequentially in main thread
    for test in tests {
        // Print `test foo    ...`, run the test, then print the outcome in
        // the same line.
        printer.print_test(&test.info);
        let outcome = if args.is_ignored(&test) {
            Outcome::Ignored
        } else {
            run_single(test.runner, test_mode)
        };
        handle_outcome(outcome, test.info, &mut printer);
    }

    // Print failures if there were any, and the final summary.
    if !failed_tests.is_empty() {
        printer.print_failures(&failed_tests);
    }

    printer.print_summary(&conclusion, start_instant.elapsed());

    conclusion
}

/// Runs the given runner, catching any panics and treating them as a failed test.
fn run_single<'a>(runner: Box<dyn FnOnce(bool) -> Outcome + 'a>, test_mode: bool) -> Outcome {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    catch_unwind(AssertUnwindSafe(move || runner(test_mode))).unwrap_or_else(|e| {
        // The `panic` information is just an `Any` object representing the
        // value the panic was invoked with. For most panics (which use
        // `panic!` like `println!`), this is either `&str` or `String`.
        let payload = e
            .downcast_ref::<String>()
            .map(|s| s.as_str())
            .or(e.downcast_ref::<&str>().map(|s| *s));

        let msg = match payload {
            Some(payload) => format!("test panicked: {payload}"),
            None => format!("test panicked"),
        };
        Outcome::Failed(msg.into())
    })
}

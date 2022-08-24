//! Utilities for logging and tracing.
use tracing::{subscriber, trace, Level};
use tracing_subscriber::FmtSubscriber;

/// Initialize a trace subsriber for a test.
pub fn init_test() {
    let subscriber = FmtSubscriber::builder()
        .with_test_writer()
        .with_max_level(Level::TRACE)
        .finish();
    // Failing to set the default is fine, errors if there's already a
    // subscriber set.
    let _ = subscriber::set_global_default(subscriber);
}

/// Initialize a trace subsriber printing to the console using the given
/// verbosity count.
pub fn init(verbose: u8) {
    let level = match verbose {
        0 => Level::INFO,
        _ => Level::TRACE,
    };
    let subscriber = FmtSubscriber::builder().with_max_level(level).finish();
    subscriber::set_global_default(subscriber).unwrap();
    trace!(%level, "log level set");
}

//! Utilities for logging and tracing.
use tracing::{info, subscriber, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Debug)]
pub enum Verbosity {
    Info,
    Debug,
    Trace,
}

impl From<u8> for Verbosity {
    fn from(v: u8) -> Self {
        match v {
            0 => Verbosity::Info,
            1 => Verbosity::Debug,
            _ => Verbosity::Trace,
        }
    }
}

impl From<Verbosity> for Level {
    fn from(v: Verbosity) -> Self {
        match v {
            Verbosity::Info => Level::INFO,
            Verbosity::Debug => Level::DEBUG,
            Verbosity::Trace => Level::TRACE,
        }
    }
}

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
pub fn init(verbosity: impl Into<Verbosity>) {
    let verbosity = verbosity.into();
    let level: Level = verbosity.into();
    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_line_number(true)
        .finish();
    subscriber::set_global_default(subscriber).unwrap();
    info!(%level, "log level set");
}

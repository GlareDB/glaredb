//! Utilities for logging and tracing.
use tracing::{info, subscriber, Level, Subscriber};
use tracing_subscriber::{
    filter::{EnvFilter, LevelFilter},
    fmt::SubscriberBuilder,
    prelude::*,
    FmtSubscriber,
};

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
    // Failing to set the the global log adapter is fine, another test may have
    // already set it.
    // TODO: Currently with this enabled, we get a _ton_ of logs.
    // let _ = LogTracer::init();

    let subscriber = FmtSubscriber::builder()
        .with_test_writer()
        .with_max_level(Level::TRACE)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .finish();
    let subscriber = with_env_filter(subscriber);
    // Failing to set the default is fine, errors if there's already a
    // subscriber set.
    let _ = subscriber::set_global_default(subscriber);
}

// TODO: It's likely we'll move to a "Builder" pattern for constructing the
// global logger as we continue to add more options/integrations for cloud
// logging.

/// Initialize a trace subsriber printing to the console using the given
/// verbosity count.
pub fn init(verbosity: impl Into<Verbosity>, json: bool) {
    let verbosity = verbosity.into();
    let level: Level = verbosity.into();

    // TODO: Currently with this enabled, we get a _ton_ of logs.
    // LogTracer::init().unwrap();

    if json {
        let mut builder = default_fmt_builder(level).json();
        // Flatten fields into the top-level json. This is primarily done such
        // that GCP can pull out the 'message' field and display those nicely.
        //
        // See https://cloud.google.com/logging/docs/structured-logging#special-payload-fields
        // for special fields that GCP can pick up.
        //
        // NOTE: Since this is flattening the json, there can be conflicts
        // between fields we provide in the trace, and metadata fields provided
        // by the tracing subsriber. No errors occur, they are just silently
        // overwritten.
        //
        // A list of fields that may be clobbered if used in trace macro calls:
        // - timestamp
        // - level
        // - target
        // - filename
        // - line_number
        // - span
        // - threadName
        // - threadId
        //
        // `tracing` doesn't have an easy way of customizer where/how indivdual
        // fields get printed. If we only wanted to put the 'message' field in
        // the top-level json, that woudl require writing our own `FormatEvent`
        // with a custom format. We _may_ end up doing that in the future when
        // we need more control over log formatting, but flattening everything
        // is sufficient for now.
        builder = builder.flatten_event(true);
        let subscriber = with_env_filter(builder.finish());
        subscriber::set_global_default(subscriber).unwrap();
    } else {
        let builder = default_fmt_builder(level);
        let subscriber = with_env_filter(builder.finish());
        subscriber::set_global_default(subscriber).unwrap();
    }

    info!(set_level = %level, "log level set");
}

fn default_fmt_builder(level: Level) -> SubscriberBuilder {
    FmtSubscriber::builder()
        .with_max_level(level)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_line_number(true)
        .with_file(true)
}

/// Add an env filter to a subscriber, with some default filters in place.
///
/// Default behavior:
/// - Default to TRACE if filter not specified via RUST_LOG
/// - Raise h2 to INFO, since it's very noisy at lower levels.
/// - Raise hyper to INFO, since it's very noisy at lower levels.
fn with_env_filter(subscriber: impl Subscriber) -> impl Subscriber {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::TRACE.into())
        .from_env_lossy()
        .add_directive("h2=info".parse().unwrap())
        .add_directive("hyper=info".parse().unwrap());
    subscriber.with(filter)
}

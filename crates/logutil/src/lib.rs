//! Utilities for logging and tracing.
use tracing::{info, subscriber, Level};
use tracing_subscriber::{fmt::SubscriberBuilder, FmtSubscriber};

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
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .finish();
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
        subscriber::set_global_default(builder.finish()).unwrap();
    } else {
        let builder = default_fmt_builder(level);
        subscriber::set_global_default(builder.finish()).unwrap();
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

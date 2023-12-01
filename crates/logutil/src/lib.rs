//! Utilities for logging and tracing.
use std::{fs::File, path::PathBuf, sync::Arc};

use tracing::{subscriber, trace, Level};
use tracing_subscriber::{
    filter::EnvFilter,
    fmt::{
        format::{Compact, DefaultFields, Format, Json, JsonFields, Pretty, Writer},
        time::FormatTime,
        SubscriberBuilder,
    },
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
#[derive(Default)]
pub enum LoggingMode {
    #[default]
    Full,
    Json,
    Compact,
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
        .with_env_filter(env_filter(Level::TRACE))
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
pub fn init(verbosity: impl Into<Verbosity>, mode: LoggingMode, log_file: Option<&PathBuf>) {
    let verbosity: Verbosity = verbosity.into();
    let level: Level = verbosity.into();

    // TODO: Currently with this enabled, we get a _ton_ of logs.
    // LogTracer::init().unwrap();
    let env_filter = env_filter(level);
    match mode {
        LoggingMode::Json => {
            let subscriber = json_fmt(level).with_env_filter(env_filter).finish();
            subscriber::set_global_default(subscriber)
        }
        LoggingMode::Full => {
            let subscriber = full_fmt(level).with_env_filter(env_filter);

            if let Some(file) = log_file {
                let debug_log = {
                    let file = File::create(file).expect("Failed to create log file");
                    Arc::new(file)
                };
                subscriber::set_global_default(subscriber.with_writer(debug_log).finish())
            } else {
                subscriber::set_global_default(subscriber.finish())
            }
        }
        LoggingMode::Compact => {
            let subscriber = compact_fmt(level).with_env_filter(env_filter).finish();
            subscriber::set_global_default(subscriber)
        }
    }
    .unwrap();

    trace!(set_level = %level, "log level set");
}

struct PrettyTime;
impl FormatTime for PrettyTime {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        write!(w, "{}", chrono::Local::now().format("%H:%M:%S%.3f"))
    }
}

fn compact_fmt(level: Level) -> SubscriberBuilder<DefaultFields, Format<Compact, PrettyTime>> {
    FmtSubscriber::builder()
        .with_max_level(level)
        .with_line_number(false)
        .with_file(false)
        .with_timer(PrettyTime)
        .compact()
}

fn standard_fmt(level: Level) -> SubscriberBuilder {
    FmtSubscriber::builder()
        .with_max_level(level)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_line_number(true)
        .with_file(true)
}

fn full_fmt(level: Level) -> SubscriberBuilder<Pretty, Format<Pretty>> {
    standard_fmt(level).pretty()
}

fn json_fmt(level: Level) -> SubscriberBuilder<JsonFields, Format<Json>> {
    standard_fmt(level)
        .json()
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
        .flatten_event(true)
}

/// Add an env filter to a subscriber, with some default filters in place.
///
/// Default behavior:
/// - Default to TRACE if filter not specified via RUST_LOG
/// - Raise h2 to INFO, since it's very noisy at lower levels.
/// - Raise hyper to INFO, since it's very noisy at lower levels.
fn env_filter(level: Level) -> EnvFilter {
    EnvFilter::builder()
        .with_default_directive(level.into())
        .from_env_lossy()
        .add_directive("h2=info".parse().unwrap())
        .add_directive("hyper=info".parse().unwrap())
}

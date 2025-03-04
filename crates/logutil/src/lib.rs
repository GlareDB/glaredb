use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::FmtSubscriber;

#[derive(Debug, Clone, Copy, Default)]
pub enum LogFormat {
    #[default]
    HumanReadable,
    Json,
}

/// Configures the global logger using the `tracing` library.
pub fn configure_global_logger<W>(default_level: tracing::Level, format: LogFormat, writer: W)
where
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    let env_filter = EnvFilter::builder()
        .with_default_directive(default_level.into())
        .from_env_lossy()
        .add_directive("h2=info".parse().unwrap())
        .add_directive("hyper=info".parse().unwrap())
        .add_directive("sqllogictest=info".parse().unwrap());

    match format {
        LogFormat::HumanReadable => {
            let subscriber = FmtSubscriber::builder()
                .with_writer(writer)
                .with_env_filter(env_filter)
                .with_thread_ids(true)
                .with_thread_names(true)
                .with_file(true)
                .with_line_number(true)
                .finish();
            tracing::subscriber::set_global_default(subscriber).unwrap();
        }
        LogFormat::Json => {
            let subscriber = FmtSubscriber::builder()
                .with_env_filter(env_filter)
                .with_thread_ids(true)
                .with_thread_names(true)
                .with_file(true)
                .with_line_number(true)
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
                .finish();
            tracing::subscriber::set_global_default(subscriber).unwrap();
        }
    }
}

use clap::{Parser, ValueEnum};
use tracing::info;

#[derive(Parser)]
#[clap(name = "lakefront")]
struct Arguments {
    /// Port to start the server on.
    #[clap(short, long, default_value_t = 8081)]
    port: u16,
    /// Log format.
    #[arg(value_enum, long, value_parser, default_value_t = LogFormat::Json)]
    log_format: LogFormat,
}

#[derive(Debug, Clone, Copy, Default, ValueEnum)]
enum LogFormat {
    #[default]
    Json,
    Pretty,
}

impl From<LogFormat> for logutil::LogFormat {
    fn from(value: LogFormat) -> Self {
        match value {
            LogFormat::Json => logutil::LogFormat::Json,
            LogFormat::Pretty => logutil::LogFormat::HumanReadable,
        }
    }
}

fn main() {
    let args = Arguments::parse();
    logutil::configure_global_logger(tracing::Level::DEBUG, args.log_format.into());

    info!("lakefront main")
}

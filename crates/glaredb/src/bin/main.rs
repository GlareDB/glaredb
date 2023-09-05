use anyhow::Result;
use clap::{Parser, ValueEnum};
use glaredb::args::LocalArgs;
use glaredb::commands::Commands;
use tracing::info;
#[derive(Debug, Clone, Copy, ValueEnum, Default)]
enum LoggingMode {
    #[default]
    Pretty,
    Json,
    Compact,
}

impl From<LoggingMode> for logutil::LoggingMode {
    fn from(mode: LoggingMode) -> Self {
        match mode {
            LoggingMode::Pretty => logutil::LoggingMode::Pretty,
            LoggingMode::Json => logutil::LoggingMode::Json,
            LoggingMode::Compact => logutil::LoggingMode::Compact,
        }
    }
}

#[derive(Parser)]
#[clap(name = "GlareDB")]
#[clap(version)]
#[clap(about = "CLI for GlareDB", long_about = None)]
struct Cli {
    /// Log verbosity.
    #[clap(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Output logs in json format.
    #[clap(long, value_enum)]
    log_mode: Option<LoggingMode>,

    #[clap(subcommand)]
    command: Option<Commands>,

    #[clap(flatten)]
    // This is a hack to apply the args to the local command as the default command
    local_args: LocalArgs,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // If some one runs "glaredb", we want them to default to running the local
    // version. This pulls out the args and places them in a local command to
    // keep all the below logic.
    let command = match cli.command {
        Some(command) => command,
        None => Commands::Local(cli.local_args),
    };

    // Disable logging when running locally since it'll clobber the repl
    // _unless_ the user specified a logging related option.
    match (&command, cli.log_mode, cli.verbose) {
        (Commands::Local { .. }, None, 0) => (),
        _ => logutil::init(cli.verbose, cli.log_mode.unwrap_or_default().into()),
    }

    info!(version = env!("CARGO_PKG_VERSION"), "starting...");

    command.run()
}

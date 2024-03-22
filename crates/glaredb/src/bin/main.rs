use anyhow::Result;
use clap::{Parser, ValueEnum};
use glaredb::args::LocalArgs;
use glaredb::commands::Commands;

#[derive(Debug, Clone, Copy, ValueEnum, Default)]
enum LoggingMode {
    Full,
    Json,
    #[default]
    Compact,
}

impl From<LoggingMode> for logutil::LoggingMode {
    fn from(mode: LoggingMode) -> Self {
        match mode {
            LoggingMode::Full => logutil::LoggingMode::Full,
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


    match (&command, cli.log_mode, cli.verbose) {
        (
            // User specified a log file, so we should use it.
            Commands::Local(LocalArgs {
                log_file: Some(log_file),
                ..
            }),
            _,
            _,
        ) => logutil::init(
            cli.verbose,
            // Use JSON logging by default when writing to a file.
            cli.log_mode.unwrap_or(LoggingMode::Json).into(),
            Some(log_file),
        ),
        // Disable logging when running locally since it'll clobber the repl
        // _unless_ the user specified a logging related option.
        (Commands::Local { .. }, None, 0) => (),
        _ => logutil::init(cli.verbose, cli.log_mode.unwrap_or_default().into(), None),
    }

    command.run()
}

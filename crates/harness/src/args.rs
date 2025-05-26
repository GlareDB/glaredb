use clap::{Parser, ValueEnum};

/// Command line arguments.
///
/// This type represents everything the user can specify via CLI args.
#[derive(Parser, Debug, Clone)]
pub struct Arguments {
    /// Run ignored and non-ignored tests.
    #[arg(long)]
    pub include_ignored: bool,

    /// Run only ignored tests.
    #[arg(long)]
    pub ignored: bool,

    /// Run tests, but not benchmarks.
    #[arg(long, conflicts_with = "bench")]
    pub test: bool,

    /// Run benchmarks, but not tests.
    #[arg(long)]
    pub bench: bool,

    /// List all tests and benchmarks.
    #[arg(long)]
    pub list: bool,

    /// Exactly match filters rather than by substring
    #[arg(long)]
    pub exact: bool,

    /// Display one character per test instead of one line. Alias to --format=terse
    #[arg(short = 'q', long = "quiet", conflicts_with = "format")]
    pub quiet: bool,

    /// Write logs to the specified file instead of stdout
    #[arg(long, value_name = "PATH")]
    pub logfile: Option<String>,

    /// Skip tests whose names contain FILTER (this flag can be used multiple times)
    #[arg(long = "skip", value_name = "FILTER")]
    pub skip: Vec<String>,

    /// Configure coloring of output:
    ///
    /// - auto = colorize if stdout is a tty and tests are run on serially (default)
    /// - always = always colorize output
    /// - never = never colorize output
    #[arg(long, value_enum, value_name = "auto|always|never")]
    pub color: Option<ColorSetting>,

    /// Configure formatting of output:
    ///
    /// - pretty = Print verbose output
    /// - terse = Display one character per test
    /// - json = Print json events
    #[arg(long = "format", value_enum, value_name = "pretty|terse|json")]
    pub format: Option<FormatSetting>,

    /// The FILTER string is tested against the name of all tests, and only
    /// those tests whose names contain the filter are run.
    #[arg(value_name = "FILTER")]
    pub filter: Option<String>,
}

impl Arguments {
    /// Parses the global CLI arguments given to the application.
    ///
    /// If the parsing fails (due to incorrect CLI args), an error is shown and
    /// the application exits. If help is requested (`-h` or `--help`), a help
    /// message is shown and the application exits, too.
    pub fn from_args() -> Self {
        Parser::parse()
    }

    /// Like `from_args()`, but operates on an explicit iterator and not the
    /// global arguments. Note that the first element is the executable name!
    pub fn from_iter<I>(iter: I) -> Self
    where
        Self: Sized,
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString> + Clone,
    {
        Parser::parse_from(iter)
    }
}

/// Possible values for the `--color` option.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum ColorSetting {
    /// Colorize output if stdout is a tty and tests are run on serially
    /// (default).
    #[default]
    Auto,
    /// Always colorize output.
    Always,
    /// Never colorize output.
    Never,
}

/// Possible values for the `--format` option.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum FormatSetting {
    /// One line per test. Output for humans. (default)
    #[default]
    Pretty,
    /// One character per test. Usefull for test suites with many tests.
    Terse,
    /// Json output
    Json,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_cli() {
        use clap::CommandFactory;
        Arguments::command().debug_assert();
    }
}

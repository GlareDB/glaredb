use clap::{Args, Parser, ValueEnum};

use crate::Trial;
use crate::printer::PrinterOptions;

/// Command line arguments.
///
/// This type represents everything the user can specify via CLI args.
///
/// `A` may be provided to extend the default set of arguments parsed. The extra
/// arguments are flattened.
#[derive(Parser, Debug, Clone)]
pub struct Arguments<A: Args> {
    /// Run ignored and non-ignored tests.
    #[clap(long)]
    pub include_ignored: bool,

    /// Run only ignored tests.
    #[clap(long)]
    pub ignored: bool,

    /// Run tests, but not benchmarks.
    #[clap(long, conflicts_with = "bench")]
    pub test: bool,

    /// Run benchmarks, but not tests.
    #[clap(long)]
    pub bench: bool,

    /// List all tests and benchmarks.
    #[clap(long)]
    pub list: bool,

    /// Exactly match filters rather than by substring
    #[clap(long)]
    pub exact: bool,

    /// Display one character per test instead of one line. Alias to --format=terse
    #[clap(short = 'q', long = "quiet", conflicts_with = "format")]
    pub quiet: bool,

    /// Write logs to the specified file instead of stdout
    #[clap(long, value_name = "PATH")]
    pub logfile: Option<String>,

    /// Skip tests whose names contain FILTER (this flag can be used multiple times)
    #[clap(long = "skip", value_name = "FILTER")]
    pub skip: Vec<String>,

    /// Configure coloring of output:
    ///
    /// - auto = colorize if stdout is a tty and tests are run on serially (default)
    /// - always = always colorize output
    /// - never = never colorize output
    #[clap(long, value_enum, value_name = "auto|always|never")]
    pub color: Option<ColorSetting>,

    /// Configure formatting of output:
    ///
    /// - pretty = Print verbose output
    /// - terse = Display one character per test
    /// - json = Print json events
    #[clap(long = "format", value_enum, value_name = "pretty|terse|json")]
    pub format: Option<FormatSetting>,

    /// The FILTER string is tested against the name of all tests, and only
    /// those tests whose names contain the filter are run.
    #[clap(value_name = "FILTER")]
    pub filter: Option<String>,

    /// Extra arguments we should parse.
    #[clap(flatten)]
    pub extra: A,
}

impl<A> Arguments<A>
where
    A: Args,
{
    /// Parses the global CLI arguments given to the application.
    ///
    /// If the parsing fails (due to incorrect CLI args), an error is shown and
    /// the application exits. If help is requested (`-h` or `--help`), a help
    /// message is shown and the application exits, too.
    pub fn from_args() -> Self {
        Parser::parse()
    }

    pub fn printer_options(&self) -> PrinterOptions {
        PrinterOptions {
            quiet: self.quiet,
            color: self.color,
            format: self.format,
            logfile: self.logfile.as_deref(),
        }
    }

    /// Returns `true` if the given test should be ignored.
    pub fn is_ignored(&self, test: &Trial) -> bool {
        (test.info.is_ignored && !self.ignored && !self.include_ignored)
            || (test.info.is_bench && self.test)
            || (!test.info.is_bench && self.bench)
    }

    pub fn is_filtered_out(&self, test: &Trial) -> bool {
        let test_name = test.name();
        // Match against the full test name, including the kind. This upholds the invariant that if
        // --list prints out:
        //
        // <some string>: test
        //
        // then "--exact <some string>" runs exactly that test.
        let test_name_with_kind = test.info.test_name_with_kind();

        // If a filter was specified, apply this
        if let Some(filter) = &self.filter {
            match self.exact {
                // For exact matches, we want to match against either the test name (to maintain
                // backwards compatibility with older versions of libtest-mimic), or the test kind
                // (technically more correct with respect to matching against the output of --list.)
                true if test_name != filter && &test_name_with_kind != filter => return true,
                false if !test_name_with_kind.contains(filter) => return true,
                _ => {}
            };
        }

        // If any skip pattern were specified, test for all patterns.
        for skip_filter in &self.skip {
            match self.exact {
                // For exact matches, we want to match against either the test name (to maintain
                // backwards compatibility with older versions of libtest-mimic), or the test kind
                // (technically more correct with respect to matching against the output of --list.)
                true if test_name == skip_filter || &test_name_with_kind == skip_filter => {
                    return true;
                }
                false if test_name_with_kind.contains(skip_filter) => return true,
                _ => {}
            }
        }

        if self.ignored && !test.info.is_ignored {
            return true;
        }

        false
    }
}

#[derive(Debug, Clone, Copy, Parser)]
pub struct NoExtraArgs;

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
        Arguments::<NoExtraArgs>::command().debug_assert();
    }
}

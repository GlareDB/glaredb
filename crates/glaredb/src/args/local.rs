use std::fmt::Write as _;

use anyhow::Result;
use clap::Args;
use url::Url;

use crate::args::{OutputMode, PathBuf, StorageConfigArgs};

#[derive(Args, Debug)]
pub struct LocalArgs {
    /// Execute a query, exiting upon completion.
    ///
    /// Multiple statements may be provided, and results will be printed out
    /// one after another.
    #[arg(short, long, value_parser)]
    pub query: Option<String>,

    #[clap(flatten)]
    pub opts: LocalClientOpts,

    /// File for logs to be written to
    #[arg(long, value_parser)]
    pub log_file: Option<PathBuf>,

    /// Start the tokio console subscriber to debug runtime.
    #[cfg(not(release))]
    #[arg(long, value_parser, hide = true)]
    pub debug_tokio: bool,
}

#[derive(Debug, Clone, Args)]
pub struct LocalClientOpts {
    /// Path to spill temporary files to.
    #[arg(long, value_parser)]
    pub spill_path: Option<PathBuf>,

    /// Optional file path for persisting data.
    ///
    /// Catalog data and user data will be stored in this directory.
    ///
    /// If the `--cloud-url` option is provided, nothing will be persisted in this directory.
    #[arg(short = 'f', long, value_parser)]
    pub data_dir: Option<PathBuf>,

    /// URL for Hybrid Execution with a GlareDB Cloud deployment.
    ///
    /// Sign up at <https://console.glaredb.com> to get a free deployment.
    ///
    /// Has the form of <glaredb://user:pass@host:port/deployment>.
    #[arg(short = 'c', long, value_parser)]
    pub cloud_url: Option<Url>,

    #[clap(flatten)]
    pub storage_config: StorageConfigArgs,

    #[arg(long, default_value = "false", hide = true)]
    pub timing: bool,

    /// Ignores the proxy and directly goes to the server for remote execution.
    ///
    /// (Internal)
    ///
    /// Note that:
    /// * `url` in this case should be a valid HTTP RPC URL (`--rpc-bind`
    ///   for the server).
    /// * Server should be started with `---disable-rpc-auth` arg as well.
    #[arg(long, hide = true)]
    pub ignore_rpc_auth: bool,

    /// Display output mode.
    #[arg(long, value_enum, default_value_t=OutputMode::Table)]
    pub mode: OutputMode,

    /// Max width for tables to display.
    #[arg(long)]
    pub max_width: Option<usize>,

    /// Max number of rows to display.
    #[arg(long)]
    pub max_rows: Option<usize>,

    /// Disable RPC TLS
    ///
    /// (Internal)
    ///
    /// Note: in the future, this will be 'on' by default
    ///
    /// Note: Keep in sync with py-glaredb connect
    #[arg(long, default_value="false", action = clap::ArgAction::Set, hide = true)]
    pub disable_tls: bool,

    /// Address of the GlareDB cloud server.
    ///
    /// (Internal)
    ///
    /// Note: Keep in sync with py-glaredb connect
    #[arg(long, default_value = "https://console.glaredb.com", hide = true)]
    pub cloud_addr: String,

    /// Path to a certificate that identifies this client.
    ///
    /// (Internal) - volatile, might be removed or drastically modified.
    ///
    /// This option is meaningless if either `--disable_tls`or
    /// `--ignore_rpc_auth` is set.
    ///
    /// TODO/REVIEW: We're probably going to want to store certs in data-dir,
    ///              or some well-known path on disk (like AppData). We want a
    ///              place to store them (if available) even if `--cloud-url`
    ///              is passed (unlike --data-dir). It probably makes sense to
    ///              keep an env var or arg around to enable workarounds and
    ///              debugging, but otherwise the ideal flow is having this be
    ///              managed by glaredb at a known persistence (non-temp). Must
    ///              be mindful/aware of where we persist these for rotation.
    pub identity_path: String,
}

impl LocalClientOpts {
    pub fn help_string() -> Result<String> {
        let pairs = [
            ("\\help", "Show this help text"),
            (
                "\\mode MODE",
                "Set the output mode [table, json, ndjson, csv]",
            ),
            ("\\max-rows NUM", "Max number of rows to display"),
            (
                "\\max-width NUM",
                "Maximum width of the output table to display. Defaults to terminal size.",
            ),
            ("\\open PATH", "Open a database at the given path"),
            ("\\timing", "Toggle query execution runtime display"),
            ("\\quit", "Quit this session"),
        ];

        let mut buf = String::new();
        for (cmd, help) in pairs {
            writeln!(&mut buf, "{cmd: <15} {help}")?;
        }

        Ok(buf)
    }
}

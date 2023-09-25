use super::*;

#[derive(Parser)]
pub struct LocalArgs {
    /// Execute a query, exiting upon completion.
    ///
    /// Multiple statements may be provided, and results will be printed out
    /// one after another.
    #[clap(short, long, value_parser)]
    pub query: Option<String>,

    #[clap(flatten)]
    pub opts: LocalClientOpts,
}

#[derive(Debug, Clone, Parser)]
pub struct LocalClientOpts {
    /// Path to spill temporary files to.
    #[clap(long, value_parser)]
    pub spill_path: Option<PathBuf>,

    /// Optional file path for persisting data.
    ///
    /// Catalog data and user data will be stored in this directory.
    ///
    /// If the `--cloud-url` option is provided, nothing will be persisted in this directory.
    #[clap(short = 'f', long, value_parser)]
    pub data_dir: Option<PathBuf>,

    /// URL for Hybrid Execution with a GlareDB Cloud deployment.
    ///
    /// Sign up at <https://console.glaredb.com> to get a free deployment.
    ///
    /// Has the form of <glaredb://user:pass@host:port/deployment>.
    #[clap(short = 'c', long, value_parser)]
    pub cloud_url: Option<Url>,

    /// Ignores the proxy and directly goes to the server for remote execution.
    ///
    /// (Internal)
    ///
    /// Note that:
    /// * `url` in this case should be a valid HTTP RPC URL (`--rpc-bind`
    ///   for the server).
    /// * Server should be started with `---disable-rpc-auth` arg as well.
    #[clap(long, hide = true)]
    pub ignore_rpc_auth: bool,

    /// Display output mode.
    #[clap(long, value_enum, default_value_t=OutputMode::Table)]
    pub mode: OutputMode,

    /// Max width for tables to display.
    #[clap(long)]
    pub max_width: Option<usize>,

    /// Max number of rows to display.
    #[clap(long)]
    pub max_rows: Option<usize>,

    /// Enable RPC TLS
    ///
    /// (Internal)
    ///
    /// In the future, we'll swap this for disable_tls
    #[clap(long, hide = true)]
    pub enable_tls: bool,

    /// Address of the GlareDB cloud server.
    ///
    /// (Internal)
    ///
    #[clap(long, default_value = "https://console.glaredb.com", hide = true)]
    pub cloud_api_addr: String,
}

impl LocalClientOpts {
    pub(crate) fn help_string() -> Result<String> {
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
            ("\\quit", "Quit this session"),
        ];

        let mut buf = String::new();
        for (cmd, help) in pairs {
            writeln!(&mut buf, "{cmd: <15} {help}")?;
        }

        Ok(buf)
    }
}

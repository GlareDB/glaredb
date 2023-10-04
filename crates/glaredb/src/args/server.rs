use super::*;

#[derive(Parser)]
pub struct ServerArgs {
    /// TCP address to bind to for the Postgres interface.
    #[clap(short, long, value_parser, default_value_t = String::from("0.0.0.0:6543"))]
    pub bind: String,

    /// TCP address to bind to for the RPC interface.
    #[clap(long, hide = true, value_parser)]
    pub rpc_bind: Option<String>,

    /// Address to the Metastore.
    ///
    /// If not provided and `local` is set to a true, an in-process
    /// metastore will be started.
    #[clap(short, long, hide = true, value_parser)]
    pub metastore_addr: Option<String>,

    /// Set the user used for authentication.
    ///
    /// Only has an affect if a password is also provided. If a password is
    /// not provided, the GlareDB server will not prompt for a password.
    #[clap(short, long, value_parser, default_value_t = String::from("glaredb"))]
    pub user: String,

    /// Set the password used for authentication.
    ///
    /// If unset, the GlareDB server will not prompt for a password.
    #[clap(short, long, value_parser)]
    pub password: Option<String>,

    /// Optional file path for persisting data.
    ///
    /// Catalog data and user data will be stored in this directory.
    #[clap(short = 'f', long, value_parser)]
    pub data_dir: Option<PathBuf>,

    /// Path to GCP service account to use when connecting to GCS.
    ///
    /// Sessions must be proxied through pgsrv, otherwise attempting to
    /// create a session will fail.
    #[clap(short, long, hide = true, value_parser)]
    pub service_account_path: Option<String>,

    #[clap(flatten)]
    pub storage_config: StorageConfigArgs,

    /// Path to spill temporary files to.
    #[clap(long, value_parser)]
    pub spill_path: Option<PathBuf>,

    /// Ignore authentication messages.
    ///
    /// (Internal)
    ///
    /// This is only relevant for internal development. The postgres
    /// protocol proxy will drop all authentication related messages.
    #[clap(long, hide = true, value_parser)]
    pub ignore_pg_auth: bool,

    /// Allow rpc messages directly from the client without proxy.
    ///
    /// (Internal)
    ///
    /// This is only relevant for internal development. The RPC proxy will
    /// allow `Client` messages directly. Need to use `--ignore-rpc-auth`
    /// with the `local` command to access this.
    #[clap(long, hide = true, value_parser)]
    pub disable_rpc_auth: bool,

    /// API key for segment.
    ///
    /// (Internal)
    #[clap(long, hide = true, value_parser)]
    pub segment_key: Option<String>,
}

use clap::Args;

use crate::args::{PathBuf, StorageConfigArgs};

#[derive(Args)]
pub struct ServerArgs {
    /// TCP address to bind to for the Postgres interface.
    #[arg(
        id = "PORT",
        short = 'b',
        long = "bind",
        value_parser,
        conflicts_with = "disable_postgres_api"
    )]
    pub bind: Option<String>,

    /// TCP address to bind to for the RPC/Flight SQL interface.
    #[arg(id= "RPC_PORT", long="rpc-bind", value_parser, aliases=&["flight-bind"])]
    pub rpc_bind: Option<String>,

    /// Set the user used for authentication.
    ///
    /// Only has an affect if a password is also provided. If a password is
    /// not provided, the GlareDB server will not prompt for a password.
    #[arg(short, long, value_parser, default_value_t = String::from("glaredb"), requires = "password")]
    pub user: String,

    /// Set the password used for authentication.
    ///
    /// If unset, the GlareDB server will not prompt for a password.
    #[arg(short, long, value_parser)]
    pub password: Option<String>,

    /// Optional file path for persisting data.
    ///
    /// Catalog data and user data will be stored in this directory.
    #[arg(short = 'f', long, value_parser)]
    pub data_dir: Option<PathBuf>,

    /// Path to GCP service account to use when connecting to GCS.
    ///
    /// Sessions must be proxied through pgsrv, otherwise attempting to
    /// create a session will fail.
    #[arg(short, long, hide = true, value_parser)]
    pub service_account_path: Option<String>,

    #[command(flatten)]
    pub storage_config: StorageConfigArgs,

    /// Path to spill temporary files to.
    #[arg(long, value_parser)]
    pub spill_path: Option<PathBuf>,

    /// Ignore authentication messages.
    ///
    /// (Internal)
    ///
    /// This is only relevant for internal development. The postgres
    /// protocol proxy will drop all authentication related messages.
    #[arg(long, hide = true, value_parser)]
    pub ignore_pg_auth: bool,

    /// Allow rpc messages directly from the client without proxy.
    ///
    /// (Internal)
    ///
    /// This is only relevant for internal development. The RPC proxy will
    /// allow `Client` messages directly. Need to use `--ignore-rpc-auth`
    /// with the `local` command to access this.
    #[arg(long, hide = true, value_parser)]
    pub disable_rpc_auth: bool,

    /// Enable the simple query RPC service.
    ///
    /// (Internal)
    ///
    /// Enables Cloud to query glaredb without the overhead of managing system
    /// credentials or using a proxy.
    #[arg(long, hide = true, value_parser)]
    pub enable_simple_query_rpc: bool,

    /// API key for segment.
    ///
    /// (Internal)
    #[arg(long, hide = true, value_parser)]
    pub segment_key: Option<String>,

    /// Enable the experimental Flight SQL API.
    ///
    /// This starts up a Flight SQL compatible API that can be used in place of
    /// the postgres interface. This is an experimental feature and is subject to
    /// change.
    ///
    /// By default, the Flight SQL API will be enabled on port 6789.
    /// Use the `--flight-bind` option to change the port.
    #[arg(long, default_value="false", action = clap::ArgAction::SetTrue)]
    pub enable_flight_api: bool,

    /// Disable the default postgres api.
    ///
    /// This will fully disable the postgres server on port 6543.
    #[arg(long, default_value="false", action = clap::ArgAction::SetTrue)]
    pub disable_postgres_api: bool,

    /// Bucket to use for database catalogs.
    #[clap(long, value_parser)]
    pub metastore_bucket: Option<String>,

    /// Path to GCP service account to use when connecting to GCS.
    #[clap(long, value_parser)]
    pub metastore_service_account_path: Option<String>,

    /// Local file path to store database catalog (for a local persistent
    /// store).
    #[clap(long, value_parser)]
    pub metastore_local_file_path: Option<PathBuf>,
}

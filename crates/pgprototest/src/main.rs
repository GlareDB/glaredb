use clap::Parser;
use std::time::Duration;

mod messages;
mod proto;

#[derive(Parser)]
#[clap(name = "pgprototest")]
#[clap(about = "Data driven postgres protocol testing", long_about = None)]
struct Cli {
    /// The directory containing the test files.
    #[clap(long)]
    dir: String,
    /// Address of the postgres compatible server.
    #[clap(long)]
    addr: String,
    /// User to log in as.
    #[clap(long)]
    user: String,
    /// An optional password to provide for authentication.
    #[clap(long)]
    password: Option<String>,
    /// The database name to connect to.
    #[clap(long)]
    database: String,
    /// Timeout for reading each message sent by the backend.
    #[clap(long, default_value_t = 20)]
    timeout: u64,
    /// If the test file should be rewritten with the expected output.
    ///
    /// May optionally set the 'REWRITE` environment variable instead.
    #[clap(long)]
    rewrite: bool,
    /// Print out received and sent messages for every test.
    #[clap(long, short)]
    verbose: bool,
}

fn main() {
    let cli = Cli::parse();
    let options = [
        (String::from("user"), cli.user),
        (String::from("database"), cli.database),
    ]
    .into_iter()
    .collect();
    let timeout = Duration::from_secs(cli.timeout);

    // 'datadriven' crate reads the 'REWRITE' environment variable.
    if cli.rewrite {
        std::env::set_var("REWRITE", "1");
    }

    proto::walk(
        cli.dir,
        cli.addr,
        options,
        cli.password,
        timeout,
        cli.verbose,
    );
}

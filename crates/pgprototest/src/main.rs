use clap::{Parser, Subcommand};
use std::time::Duration;

mod messages;
mod proto;

#[derive(Parser)]
#[clap(name = "pgprototest")]
#[clap(about = "Data driven postgres protocol testing", long_about = None)]
struct Cli {
    #[clap(long)]
    dir: String,
    #[clap(long)]
    addr: String,
    #[clap(long)]
    user: String,
    #[clap(long)]
    database: String,
    #[clap(long, default_value_t = 20)]
    timeout: u64,
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
    proto::walk(&cli.dir, &cli.addr, &options, timeout);
}

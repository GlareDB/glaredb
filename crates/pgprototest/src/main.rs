use clap::Parser;
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
    password: Option<String>,
    #[clap(long)]
    database: String,
    #[clap(long, default_value_t = 20)]
    timeout: u64,
    #[clap(long)]
    rewrite: bool,
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
    if cli.rewrite {
        std::env::set_var("REWRITE", "1");
    }
    proto::walk(cli.dir, cli.addr, options, cli.password, timeout);
}

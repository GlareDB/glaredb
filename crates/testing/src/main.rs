use std::sync::Arc;

use clap::Parser as _;
use testing::slt::{
    cli::Cli,
    hooks::{AllTestsHook, SshTunnelHook},
    runner::SltRunner,
    tests::{PgBinaryEncoding, SshKeysTest},
};

pub fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let test_dir = cli
        .test_dir
        .unwrap_or_else(|| std::env::current_dir().unwrap());

    SltRunner::new()
        .test_files_dir(test_dir)?
        // Rust tests
        .test("sqllogictests/ssh_keys", Box::new(SshKeysTest))?
        .test("pgproto/binary_encoding", Box::new(PgBinaryEncoding))?
        // Add hooks
        .hook("*", Arc::new(AllTestsHook))?
        // SSH Tunnels hook
        .hook("*/tunnels/ssh", Arc::new(SshTunnelHook))?
        .run()
}

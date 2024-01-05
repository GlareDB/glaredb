mod hooks;
mod tests;

use anyhow::Result;
use hooks::{AllTestsHook, SshTunnelHook};
use std::sync::Arc;
use testing::slt::runner::SltRunner;
use tests::{PgBinaryEncoding, SshKeysTest};

fn main() -> Result<()> {
    SltRunner::new()
    
        .test_files_dir("../../testdata")?
        // Rust tests
        .test("sqllogictests/ssh_keys", Box::new(SshKeysTest))?
        .test("pgproto/binary_encoding", Box::new(PgBinaryEncoding))?
        // Add hooks
        .hook("*", Arc::new(AllTestsHook))?
        // SSH Tunnels hook
        .hook("*/tunnels/ssh", Arc::new(SshTunnelHook))?
        .run()
}

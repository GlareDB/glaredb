mod hooks;
mod tests;

use anyhow::Result;
use hooks::{AllTestsHook, SshTunnelHook};
use std::sync::Arc;
use testing::slt::runner::SltRunner;
use tests::SshKeysTest;

fn main() -> Result<()> {
    SltRunner::new()
        .test_files_dir("../../testdata")?
        // Rust tests
        .test("sqllogictests/ssh_keys", Box::new(SshKeysTest))?
        // Add hooks
        .hook("*", Arc::new(AllTestsHook))?
        // SSH Tunnels hook
        .hook("*/tunnels/ssh", Arc::new(SshTunnelHook))?
        .run()
}

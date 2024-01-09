use std::sync::Arc;

use testing::slt::{
    hooks::{AllTestsHook, SshTunnelHook},
    runner::SltRunner,
    tests::{PgBinaryEncoding, SshKeysTest},
};

pub fn main() -> anyhow::Result<()> {
    SltRunner::new()
        .test_files_dir("testdata")?
        // Rust tests
        .test("sqllogictests/ssh_keys", Box::new(SshKeysTest))?
        .test("pgproto/binary_encoding", Box::new(PgBinaryEncoding))?
        // Add hooks
        .hook("*", Arc::new(AllTestsHook))?
        // SSH Tunnels hook
        .hook("*/tunnels/ssh", Arc::new(SshTunnelHook))?
        .run()
}

mod hooks;

use std::path::Path;

use anyhow::Result;
use hooks::{AllTestsHook, SshTunnelHook};
use testing::slt::runner::SltRunner;

fn main() -> Result<()> {
    SltRunner::new()
        .test_files_dir(Path::new("testdata"))?
        .hook("*", Box::new(AllTestsHook))?
        // SSH Tunnels setup
        .hook("*/tunnels/ssh", Box::new(SshTunnelHook))?
        .run()
}

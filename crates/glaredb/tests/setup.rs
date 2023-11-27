use assert_cmd::cmd::Command;

#[allow(dead_code)]
pub const DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

pub fn make_cli() -> Command {
    Command::cargo_bin(env!("CARGO_PKG_NAME")).expect("Failed to find binary")
}

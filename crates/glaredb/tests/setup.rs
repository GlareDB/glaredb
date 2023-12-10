use assert_cmd::cmd::Command;

pub fn make_cli() -> Command {
    Command::cargo_bin(env!("CARGO_PKG_NAME")).expect("Failed to find binary")
}

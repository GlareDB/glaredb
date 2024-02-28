mod setup;
use crate::setup::make_cli;

#[test]
fn test_version() {
    let mut cmd = make_cli();
    let assert = cmd.arg("version").assert();

    assert.failure().stderr(predicates::str::contains(
        "unrecognized subcommand \'version\'",
    ));
}

#[test]
fn test_version_flag() {
    let mut cmd = make_cli();
    let assert = cmd.arg("--version").assert();

    assert
        .success()
        .stdout(predicates::str::contains(env!("CARGO_PKG_VERSION")));
}

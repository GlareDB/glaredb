mod setup;

use crate::setup::{make_cli, DEFAULT_TIMEOUT};

#[test]
fn test_named_log_file() {
    let mut cmd = make_cli();
    let log_file_name = "test.log";
    cmd.timeout(DEFAULT_TIMEOUT)
        .arg("-q")
        .arg("select 1;")
        .arg("--log-file")
        .arg(log_file_name)
        .assert()
        .success();

    let mut log_file = std::env::current_dir().expect("failed to retrieve current dir");
    log_file.push(log_file_name);
    assert!(log_file.exists());
}

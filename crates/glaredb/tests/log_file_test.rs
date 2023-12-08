mod setup;

use std::{
    fs::{remove_file, OpenOptions},
    io::Read,
};

use crate::setup::{make_cli, DEFAULT_TIMEOUT};

#[test]
fn test_log_file() -> Result<(), Box<dyn std::error::Error>> {
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

    let mut file = OpenOptions::new().read(true).open(&log_file)?;
    assert!(file.metadata()?.len() > 0);

    let mut content = String::new();
    file.read_to_string(&mut content)?;

    assert!(content.contains("INFO"));
    assert!(!content.contains("DEBUG"));
    assert!(!content.contains("TRACE"));

    assert!(content.contains("Starting in-memory metastore"));

    remove_file(log_file)?;
    Ok(())
}

#[test]
fn test_log_file_verbosity_level() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = make_cli();
    let log_file_name = "test.log";
    cmd.timeout(DEFAULT_TIMEOUT)
        .arg("-v")
        .arg("-q")
        .arg("select 1;")
        .arg("--log-file")
        .arg(log_file_name)
        .assert()
        .success();

    let mut log_file = std::env::current_dir().expect("failed to retrieve current dir");
    log_file.push(log_file_name);
    assert!(log_file.exists());

    let mut file = OpenOptions::new().read(true).open(&log_file)?;
    assert!(file.metadata()?.len() > 0);

    let mut content = String::new();
    file.read_to_string(&mut content)?;

    assert!(content.contains("DEBUG"));
    assert!(!content.contains("TRACE"));

    remove_file(log_file)?;
    Ok(())
}

#[test]
fn test_skipping_logs() {
    let mut cmd = make_cli();
    let log_file_name = "/usr/bin/debug.log";
    cmd.timeout(DEFAULT_TIMEOUT)
        .arg("-q")
        .arg("select 1;")
        .arg("--log-file")
        .arg(log_file_name)
        .assert()
        .success();

    let mut log_file = std::env::current_dir().expect("failed to retrieve current dir");
    log_file.push(log_file_name);
    assert!(!log_file.exists());
}

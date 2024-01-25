mod setup;

use std::{
    fs::{remove_file, OpenOptions},
    io::Read,
};

use tempfile::NamedTempFile;

use crate::setup::make_cli;

#[test]
fn test_log_file() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = make_cli();
    let tmp_file = NamedTempFile::new().unwrap();
    let file_path = tmp_file.path();
    let file_path = file_path.canonicalize().unwrap();
    let file_path = file_path.as_os_str().to_str().unwrap();

    cmd.arg("--log-file")
        .arg(file_path)
        .arg("-q")
        .arg("select 1;")
        .assert()
        .success();

    let mut file = OpenOptions::new().read(true).open(&tmp_file)?;
    assert!(file.metadata()?.len() > 0);

    let mut content = String::new();
    file.read_to_string(&mut content)?;

    assert!(content.contains("INFO"));
    assert!(!content.contains("DEBUG"));
    assert!(!content.contains("TRACE"));

    assert!(content.contains("Starting in-memory metastore"));

    remove_file(tmp_file)?;
    Ok(())
}

#[test]
fn test_log_file_verbosity_level() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = make_cli();
    let tmp_file = NamedTempFile::new().unwrap();
    let file_path = tmp_file.path();
    let file_path = file_path.canonicalize().unwrap();
    let file_path = file_path.as_os_str().to_str().unwrap();

    cmd.arg("-v")
        .arg("--log-file")
        .arg(file_path)
        .arg("-q")
        .arg("select 1;")
        .assert()
        .success();

    let mut file = OpenOptions::new().read(true).open(&tmp_file)?;
    assert!(file.metadata()?.len() > 0);

    let mut content = String::new();
    file.read_to_string(&mut content)?;

    assert!(content.contains("DEBUG"));
    assert!(!content.contains("TRACE"));

    remove_file(tmp_file)?;
    Ok(())
}

mod setup;

use predicates::boolean::PredicateBooleanExt;
use setup::DEFAULT_TIMEOUT;

use crate::setup::make_cli;

#[test]
/// User shouldn't be able to specify both a query and a file.
/// Invalid: ./glaredb -q <QUERY> [FILE]
fn test_query_or_file() {
    let mut cmd = make_cli();

    let assert = cmd
        .timeout(DEFAULT_TIMEOUT)
        .args(["-q", "SELECT * FROM foo", "foo.sql"])
        .assert();
    assert.failure().stderr(predicates::str::contains(
        "the argument '--query <QUERY>' cannot be used with '[FILE]'",
    ));
}

#[test]
/// must provide '--location' if '--option' is provided.
/// Invalid: ./glaredb -o <KEY>=<VALUE>
fn test_storage_config_require_location() {
    let mut cmd = make_cli();

    let assert = cmd
        .timeout(DEFAULT_TIMEOUT)
        .args(["-o", "foo=bar"])
        .assert();
    assert.failure().stderr(
        predicates::str::contains("error: the following required arguments were not provided:")
            .and(predicates::str::contains("--location <LOCATION>")),
    );
}

#[test]
/// storage options must be key-value pairs. [key]=[value]
/// Invalid: ./glaredb -l <LOCATION> -o <key> <value>
fn test_parse_storage_options_not_ok() {
    let mut cmd = make_cli();

    let assert = cmd
        .timeout(DEFAULT_TIMEOUT)
        .args(["-l", "foo", "-o", "foobar"])
        .assert();
    assert.failure().stderr(predicates::str::contains(
        "Expected key-value pair delimited by an equals sign, got",
    ));
}

#[test]
/// storage options must be key-value pairs. [key]=[value]
/// ./glaredb -l <LOCATION> -o <KEY>=<VALUE>
fn test_parse_storage_options_ok() {
    let mut cmd = make_cli();
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_dir = temp_dir.path().to_str().unwrap();

    let assert = cmd
        .timeout(DEFAULT_TIMEOUT)
        .args(["-l", temp_dir, "-o", "foo=bar", "-q", "select 1"])
        .assert();
    assert.success();
}

#[test]
/// ./glaredb -f <PATH> -q <QUERY>
fn test_data_dir() {
    let mut cmd = make_cli();
    let temp_dir = tempfile::tempdir().unwrap();

    let data_dir = temp_dir.path();
    let path = data_dir.to_str().unwrap();

    let assert = cmd
        .timeout(DEFAULT_TIMEOUT)
        .args(["-f", path, "-q", "create table test as select 1"])
        .assert();
    assert.success();

    let db_dir = data_dir.join("databases/00000000-0000-0000-0000-000000000000");
    let visible_dir = db_dir.join("visible");
    let tmp_dir = db_dir.join("tmp");

    assert!(std::fs::read_dir(db_dir).is_ok());
    assert!(std::fs::read_dir(visible_dir).is_ok());
    assert!(std::fs::read_dir(tmp_dir).is_ok());
}

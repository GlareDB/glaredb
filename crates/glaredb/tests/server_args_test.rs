mod setup;

use predicates::{boolean::PredicateBooleanExt, str::contains};
use setup::DEFAULT_TIMEOUT;

use crate::setup::make_cli;

#[test]
fn test_server_bind_addr() {
    let mut cmd = make_cli();

    let assert = cmd
        .timeout(DEFAULT_TIMEOUT)
        .arg("server")
        .arg("--bind")
        .arg("0.0.0.0:0")
        .assert();

    assert.interrupted(/* We expect a timeout here */).stdout(contains(
        "Connect via Postgres: postgresql://",
    ));
}

#[test]
/// Can't use -b and --disable-postgres-api at the same time.
fn test_server_bind_addr_conflict() {
    let mut cmd = make_cli();

    let assert = cmd
        .timeout(DEFAULT_TIMEOUT)
        .arg("server")
        .arg("--bind")
        .arg("0.0.0.0:0")
        .arg("--disable-postgres-api")
        .assert();

    assert.failure(/* We expect a timeout here */).stderr(contains(
      "the argument '--bind <PORT>' cannot be used with '--disable-postgres-api'",
    ));
}

#[test]
/// Must provide a password if a user is provided.
fn test_user_requires_password() {
    let mut cmd = make_cli();

    let assert = cmd
        .timeout(DEFAULT_TIMEOUT)
        .arg("server")
        .arg("--bind")
        .arg("0.0.0.0:0")
        .args(&["-u", "test"])
        .assert();

    assert.failure(/* We expect a timeout here */).stderr(contains(
      "the following required arguments were not provided:",
    ).and(contains(
      "--password <PASSWORD>",
    )));
}

#[test]
/// ./glaredb server --enable-flight-api
fn test_enable_flight_api() {
    let mut cmd = make_cli();

    let assert = cmd
        .timeout(DEFAULT_TIMEOUT)
        .arg("server")
        .arg("--bind")
        .arg("0.0.0.0:0")
        .arg("--rpc-bind")
        .arg("0.0.0.0:0")
        .arg("--enable-flight-api")
        .assert();

    assert.interrupted(/* We expect a timeout here */).stdout(contains(
        "Connect via RPC: grpc://0.0.0.0"
    ).and(contains(
        "enabling flight sql service",
    )));
}

#[test]
/// ./glaredb server --enable-flight-api
fn test_flight_api_not_enabled_by_default() {
    let mut cmd = make_cli();

    let assert = cmd
        .timeout(DEFAULT_TIMEOUT)
        .arg("server")
        .arg("--bind")
        .arg("0.0.0.0:0")
        .assert();

    assert.interrupted(/* We expect a timeout here */).stdout(contains(
        "enabling flight sql service",
    ).not());
}

#[test]
/// ./glaredb server
fn test_pg_enabled_by_default() {
    let mut cmd = make_cli();

    let assert = cmd
        .timeout(DEFAULT_TIMEOUT)
        .arg("server")
        .arg("--bind")
        .arg("0.0.0.0:0")
        .assert();

    assert.interrupted(/* We expect a timeout here */).stdout(contains(
        "Connect via Postgres protocol: postgresql://"
    ));
}

#[test]
/// ./glaredb server --disable-postgres-api
fn test_pg_disable() {
    let mut cmd = make_cli();

    let assert = cmd
        .timeout(DEFAULT_TIMEOUT)
        .arg("server")
        .arg("--disable-postgres-api")
        .assert();

    assert.interrupted(/* We expect a timeout here */).stdout(contains(
        "Connect via Postgres protocol: postgresql"
    ).not());
}

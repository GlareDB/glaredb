mod setup;

use crate::setup::make_cli;

fn test_output_mode(mode: &str, expected: &str) {
    let mut cmd = make_cli();

    cmd.arg("--mode").arg(mode).arg("-q").arg("select 1;");
    let output = cmd.output().expect("Failed to run command");
    let stdout_str = String::from_utf8(output.stdout).expect("Failed to read stdout");

    assert_eq!(stdout_str, expected);
}

#[test]
/// Default output mode is table.
/// ./glaredb -q <QUERY>
fn test_output_mode_default() {
    let mut cmd = make_cli();

    cmd.arg("-q").arg("select 1;");
    let output = cmd.output().expect("Failed to run command");
    let stdout_str = String::from_utf8(output.stdout).expect("Failed to read stdout");
    let expected = r#"
┌──────────┐
│ Int64(1) │
│       ── │
│    Int64 │
╞══════════╡
│        1 │
└──────────┘
"#
    .trim_start();
    assert_eq!(stdout_str, expected);
}

#[test]
/// ./glaredb -q <QUERY> --mode json
fn test_output_mode_json() {
    let expected = r#"
[{"Int64(1)":1}]
"#
    .trim_start();
    test_output_mode("json", expected);
}

#[test]
/// ./glaredb -q <QUERY> --mode csv
fn test_output_mode_csv() {
    let expected = r#"
Int64(1)
1
"#
    .trim_start();
    test_output_mode("csv", expected);
}

#[test]
/// ./glaredb -q <QUERY> --mode ndjson
fn test_output_mode_ndjson() {
    let expected = r#"
{"Int64(1)":1}
"#
    .trim_start();
    test_output_mode("ndjson", expected);
}

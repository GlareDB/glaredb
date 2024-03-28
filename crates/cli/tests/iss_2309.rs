mod setup;

use crate::setup::make_cli;

#[test]
fn test_special_characters() {
    let mut cmd = make_cli();

    let assert = cmd.args(["-q", r#"select ';'"#]).assert();
    assert.success().stdout(predicates::str::contains(
        r#"
┌───────────┐
│ Utf8(";") │
│ ──        │
│ Utf8      │
╞═══════════╡
│ ;         │
└───────────┘"#
            .trim_start(),
    ));
}

#[test]
fn test_special_characters_2() {
    let mut cmd = make_cli();

    let assert = cmd.args(["-q", r#"select ";""#]).assert();
    assert.failure().stderr(predicates::str::contains(
        "Schema error: No field named \";\".",
    ));
}

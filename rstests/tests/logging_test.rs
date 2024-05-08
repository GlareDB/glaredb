mod setup;

use predicates::boolean::PredicateBooleanExt;
use setup::{make_cli, DEFAULT_TIMEOUT};

#[test]
fn test_verbose_logging_flag() {
    let mut cmd = make_cli();
    let assert = cmd.arg("-v").timeout(DEFAULT_TIMEOUT).assert();

    assert.stdout(predicates::str::contains("DEBUG").and(predicates::str::contains("TRACE").not()));
}

#[test]
fn test_very_verbose_logging_flag() {
    let mut cmd = make_cli();
    let assert = cmd.arg("-vv").timeout(DEFAULT_TIMEOUT).assert();

    assert.stdout(predicates::str::contains("DEBUG").and(predicates::str::contains("TRACE")));
}

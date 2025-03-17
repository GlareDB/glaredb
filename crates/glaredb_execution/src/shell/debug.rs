//! Debug module for printing to stderr for development purposes.
//!
//! ```text
//! cargo build --bin glaredb && ./target/debug/glaredb 2> lineedit.log
//! ```

use std::io::Write;

const ENABLE_LOGGING: bool = false;

pub fn log<S>(func: impl Fn() -> S)
where
    S: AsRef<str>,
{
    if ENABLE_LOGGING {
        log_inner(func().as_ref());
    }
}

fn log_inner(s: impl AsRef<str>) {
    let mut stderr = std::io::stderr();
    stderr.write_all(s.as_ref().as_bytes()).unwrap();
    stderr.write_all(b"\n").unwrap();
    stderr.flush().unwrap();
}

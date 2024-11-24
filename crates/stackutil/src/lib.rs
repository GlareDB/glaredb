use rayexec_error::{RayexecError, Result};

/// Stack redline to check usage against.
///
/// This is a pretty significant redline, but it's important that we error
/// instead of aborting.
const REDLINE: usize = 128 * 1024; // 128KB

/// Check if the remaining stack is below the redline and return an error if it
/// is.
///
/// `debug_str` can be anything to quickly find where hitting the redline
/// originated without resorting to backtraces or gdb.
#[inline]
pub fn check_stack_redline(debug_str: &str) -> Result<()> {
    if let Some(rem) = remaining_stack() {
        if rem <= REDLINE {
            return Err(RayexecError::new(format!(
                "Remaining stack below redline of {}, have: {}, debug str: {}",
                REDLINE, rem, debug_str
            )));
        }
    }
    Ok(())
}

pub fn remaining_stack() -> Option<usize> {
    stacker::remaining_stack()
}

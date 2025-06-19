/// Macros for inserting an early return.
///
/// This should be used for tests that either take a really long time to run
/// under Miri, or are incompatible.
///
/// Usage of this macros should occasionally be re-evaluated.
macro_rules! return_if_miri {
    () => {
        if cfg!(miri) {
            eprintln!("skipped under Miri");
            return;
        }
    };
    ($msg:expr) => {
        if cfg!(miri) {
            eprintln!("skipped under Miri: {}", $msg);
            return;
        }
    };
}

pub(crate) use return_if_miri;

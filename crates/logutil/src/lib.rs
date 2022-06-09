//! Utilities for logging.

#[cfg(test)]
pub fn init() {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Debug)
        .try_init();
}

#[cfg(not(test))]
pub fn init() {
    let _ = env_logger::builder().init();
}

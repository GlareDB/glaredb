//! Utilities for logging.

pub fn init_test() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::max())
        .is_test(true)
        .try_init();
}

pub fn init() {
    let _ = env_logger::builder().init();
}

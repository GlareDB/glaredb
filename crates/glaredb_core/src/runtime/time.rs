use std::time::Duration;

/// Instant provides an abstraction around getting the current time, and
/// computing a duration from two instances.
///
/// This should be a high-performance instant type as it's used to generating
/// timing for operator execution.
///
/// This trait is needed to allow for runtime-specific implementations since
/// WASM does not support fetching the current time using the std function.
pub trait RuntimeInstant {
    /// Gets an instant representing now.
    fn now() -> Self;

    /// Returns the elapsed duration between `earlier` and `self`.
    ///
    /// `earlier` is later than `self`, this should return a duration
    /// representing zero.
    fn duration_since(&self, earlier: Self) -> Duration;
}

#[derive(Debug)]
pub struct Timer<I: RuntimeInstant> {
    start: I,
}

impl<I: RuntimeInstant> Timer<I> {
    /// Starts a new timer.
    pub fn start() -> Self {
        Timer { start: I::now() }
    }

    /// Stop the timer, returning the duration.
    pub fn stop(self) -> Duration {
        let now = I::now();
        now.duration_since(self.start)
    }
}

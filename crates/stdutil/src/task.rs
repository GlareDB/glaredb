use std::task::{Context, RawWaker, RawWakerVTable, Waker};

/// Create s no-op context to use when polling.
///
/// This should only be used for tests.
pub const fn noop_context() -> Context<'static> {
    Context::from_waker(noop_waker())
}

// TODO: Remove when `Waker::noop` lands (1.85)
pub const fn noop_waker() -> &'static Waker {
    const WAKER: &Waker = &unsafe { Waker::from_raw(RAW_WAKER_NOOP) };
    WAKER
}

const RAW_WAKER_NOOP: RawWaker = {
    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        // Cloning just returns a new no-op raw waker
        |_| RAW_WAKER_NOOP,
        // `wake` does nothing
        |_| {},
        // `wake_by_ref` does nothing
        |_| {},
        // Dropping does nothing as we don't allocate anything
        |_| {},
    );

    RawWaker::new(std::ptr::null(), &VTABLE)
};

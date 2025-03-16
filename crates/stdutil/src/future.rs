use std::future::Future;
use std::pin::Pin;
use std::task::Poll;

use crate::task::noop_context;

/// Block on a future.
///
/// This will block the current thread and spin until the future produces a
/// result.
///
/// This should only be used in tests.
pub fn block_on<F>(mut fut: F) -> F::Output
where
    F: Future,
{
    // SAFETY: We own the future and it will not be dropped until after the
    // below loop.
    let mut fut = unsafe { Pin::new_unchecked(&mut fut) };

    let mut cx = noop_context();
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_on_simple() {
        // Entirely for UB check.
        let v = block_on(async { "hello world" });
        assert_eq!("hello world", v);
    }
}

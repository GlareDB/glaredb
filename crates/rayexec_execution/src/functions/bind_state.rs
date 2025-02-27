use std::sync::Arc;

use crate::arrays::datatype::DataType;
use crate::expr::Expression;

#[derive(Debug, Clone)]
pub struct RawTableFunctionBindState {
    pub state: RawBindStateInner,
}

/// Bind state for a table function.
#[derive(Debug)]
pub struct TableFunctionBindState<S> {
    pub state: S,
}

#[derive(Debug, Clone)]
pub struct RawBindState {
    pub state: RawBindStateInner,
    pub return_type: DataType,
    pub inputs: Vec<Expression>,
}

impl RawBindState {
    pub(crate) fn state_ptr(&self) -> *const () {
        self.state.0.ptr
    }
}

/// Bind state for a a scalar or aggregate function. Paramterized on the
/// function state.
#[derive(Debug)]
pub struct BindState<S> {
    pub state: S,
    pub return_type: DataType,
    pub inputs: Vec<Expression>,
}

/// State passed to functions during execute.
///
/// Inner state is wrapped in an arc since we allow cloning functions as we
/// allow cloning expressions.
///
/// The state passed during execute is not mutable, so if we end up with
/// multiple function calls using the same state, that's fine.
///
/// The Arc also ensure we call the drop function once.
#[derive(Debug, Clone)]
pub struct RawBindStateInner(Arc<StateInner>);

impl RawBindStateInner {
    pub(crate) fn from_state<S: Sync + Send>(state: S) -> Self {
        let state = Box::new(state);
        let ptr = Box::into_raw(state);

        let inner = StateInner {
            ptr: ptr.cast(),
            drop_fn: |drop_ptr: *const ()| {
                let drop_ptr = drop_ptr.cast::<S>().cast_mut();
                let state = unsafe { Box::from_raw(drop_ptr) };
                std::mem::drop(state);
            },
        };

        RawBindStateInner(Arc::new(inner))
    }
}

#[derive(Debug)]
struct StateInner {
    ptr: *const (),
    drop_fn: unsafe fn(ptr: *const ()),
}

// SAFETY: Aggregate and Scalar function traits should have Send + Sync bounds.
unsafe impl Send for StateInner {}
unsafe impl Sync for StateInner {}

impl Drop for StateInner {
    fn drop(&mut self) {
        unsafe { (self.drop_fn)(self.ptr) }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{self, AtomicUsize};

    use super::*;

    #[test]
    fn drop_once() {
        let drop_count = Arc::new(AtomicUsize::new(0));

        struct TestState {
            drop_count: Arc<AtomicUsize>,
        }

        impl Drop for TestState {
            fn drop(&mut self) {
                self.drop_count.fetch_add(1, atomic::Ordering::SeqCst);
            }
        }

        let state = TestState {
            drop_count: drop_count.clone(),
        };

        let raw1 = RawBindStateInner::from_state(state);
        let raw2 = raw1.clone();

        std::mem::drop(raw1);
        std::mem::drop(raw2);

        let count = drop_count.load(atomic::Ordering::SeqCst);
        assert_eq!(1, count);
    }
}

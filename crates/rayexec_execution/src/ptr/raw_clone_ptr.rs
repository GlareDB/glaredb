use std::ptr::NonNull;
use std::sync::atomic::{self, AtomicU32};

const MAX_REF_COUNT: u32 = u32::MAX / 2;

/// Type-erased Arc.
///
/// Holds a heap allocated pointer that can be cloned.
///
/// This will ensure the value's Drop implementation is called when all
/// references are dropped.
#[derive(Debug)]
pub struct RawClonePtr(NonNull<Inner>);

#[derive(Debug)]
struct Inner {
    ref_count: AtomicU32,
    ptr: *const (),
    drop_fn: unsafe fn(ptr: *const ()),
}

// SAFETY: Construction of this object requires Sync + Send types.
unsafe impl Send for RawClonePtr {}
unsafe impl Sync for RawClonePtr {}

impl RawClonePtr {
    pub fn new<V>(value: V) -> Self
    where
        V: Sync + Send,
    {
        let value = Box::new(value);
        let ptr = Box::into_raw(value);

        let inner = Inner {
            ref_count: AtomicU32::new(1),
            ptr: ptr.cast(),
            drop_fn: |drop_ptr: *const ()| {
                let drop_ptr = drop_ptr.cast::<V>().cast_mut();
                let value = unsafe { Box::from_raw(drop_ptr) };
                std::mem::drop(value);
            },
        };

        let inner = Box::new(inner);
        let inner_ptr = Box::into_raw(inner);

        RawClonePtr(NonNull::new(inner_ptr).unwrap())
    }

    /// Get the raw, untyped pointer to the underlying value.
    ///
    /// # Safety
    ///
    /// The caller must ensure this object outlives the returned pointer.
    pub const fn get(&self) -> *const () {
        let inner = unsafe { self.0.as_ref() };
        inner.ptr
    }

    const fn inner(&self) -> &Inner {
        unsafe { self.0.as_ref() }
    }
}

impl Clone for RawClonePtr {
    fn clone(&self) -> Self {
        let inner = unsafe { self.0.as_ref() };
        // From Arc comments:
        //
        // > As explained in the [Boost documentation][1], Increasing the
        // > reference counter can always be done with memory_order_relaxed: New
        // > references to an object can only be formed from an existing
        // > reference, and passing an existing reference from one thread to
        // > another must already provide any required synchronization.
        // >
        // > [1]: (www.boost.org/doc/libs/1_55_0/doc/html/atomic/usage_examples.html)
        let prev_count = inner.ref_count.fetch_add(1, atomic::Ordering::Relaxed);

        if prev_count == MAX_REF_COUNT {
            panic!("Ref count exceeded {MAX_REF_COUNT}");
        }

        RawClonePtr(self.0)
    }
}

impl Drop for RawClonePtr {
    fn drop(&mut self) {
        // Decrement reference count using Release to ensure prior writes are
        // visible.
        let prev_count = self
            .inner()
            .ref_count
            .fetch_sub(1, atomic::Ordering::Release);
        if prev_count != 1 {
            return;
        }

        {
            let inner = self.inner();
            // Ensure all previous writes are visible before proceeding.
            let _ = inner.ref_count.load(atomic::Ordering::Acquire);
            // Drop the stored value safely.
            unsafe { (inner.drop_fn)(inner.ptr) };
        }

        // Free the metadata itself.
        let value = unsafe { Box::from_raw(self.0.as_ptr()) };
        std::mem::drop(value);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{self, AtomicUsize};
    use std::sync::Arc;

    use super::*;

    #[test]
    fn get_ptr() {
        let cell = RawClonePtr::new("hello".to_string());
        let ptr = cell.get();
        let v = unsafe { ptr.cast::<String>().as_ref() }.unwrap();

        assert_eq!("hello", v)
    }

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

        let raw1 = RawClonePtr::new(state);
        let raw2 = raw1.clone();

        std::mem::drop(raw1);
        std::mem::drop(raw2);

        let count = drop_count.load(atomic::Ordering::SeqCst);
        assert_eq!(1, count);
    }
}

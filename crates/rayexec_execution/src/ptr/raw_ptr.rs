use std::ptr::NonNull;

/// Type-erased, non-cloneable Box.
#[derive(Debug)]
pub struct RawPtr(NonNull<Inner>);

#[derive(Debug)]
struct Inner {
    ptr: *mut (),
    drop_fn: unsafe fn(ptr: *const ()),
}

// SAFETY: Construction of this object requires Sync + Send types.
unsafe impl Send for RawPtr {}
unsafe impl Sync for RawPtr {}

impl RawPtr {
    pub fn new<V>(value: V) -> Self
    where
        V: Sync + Send,
    {
        let value = Box::new(value);
        let ptr = Box::into_raw(value);

        let inner = Inner {
            ptr: ptr.cast(),
            drop_fn: |drop_ptr: *const ()| {
                let drop_ptr = drop_ptr.cast::<V>().cast_mut();
                let value = unsafe { Box::from_raw(drop_ptr) };
                std::mem::drop(value);
            },
        };

        let inner = Box::new(inner);
        let inner_ptr = Box::into_raw(inner);

        RawPtr(NonNull::new(inner_ptr).unwrap())
    }

    /// Get the raw, untyped pointer to the underlying value.
    ///
    /// # Safety
    ///
    /// The caller must ensure this object outlives the returned pointer.
    pub const fn get(&self) -> *const () {
        let inner = unsafe { self.0.as_ref() };
        inner.ptr.cast_const()
    }

    pub const fn get_mut(&mut self) -> *mut () {
        let inner = unsafe { self.0.as_ref() };
        inner.ptr
    }
}

impl Drop for RawPtr {
    fn drop(&mut self) {
        let inner = unsafe { self.0.as_ref() };
        unsafe { (inner.drop_fn)(inner.ptr) }

        let value = unsafe { Box::from_raw(self.0.as_ptr()) };
        std::mem::drop(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_ptr() {
        let cell = RawPtr::new("hello".to_string());
        let ptr = cell.get();
        let v = unsafe { ptr.cast::<String>().as_ref() }.unwrap();

        assert_eq!("hello", v)
    }
}

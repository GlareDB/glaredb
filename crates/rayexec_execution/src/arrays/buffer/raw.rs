use rayexec_error::Result;

use super::buffer_manager::BufferManager;

#[derive(Debug)]
pub struct RawBufferParts<B: BufferManager> {
    /// Memory reservation for this buffer.
    pub(crate) reservation: B::Reservation,
    /// Raw pointer to start of vec.
    pub(crate) ptr: *mut u8,
    /// Number of elements `T` in the vec, not bytes.
    pub(crate) len: usize,
    /// Capacity of vec (`T` not bytes).
    pub(crate) cap: usize,
}

impl<B: BufferManager> RawBufferParts<B> {
    pub fn try_new<T: Default + Copy>(manager: &B, len: usize) -> Result<Self> {
        // Note that `vec!` may over-allocate, so we track that too.
        //
        // See <https://doc.rust-lang.org/std/vec/struct.Vec.html#guarantees>
        // > vec![x; n], vec![a, b, c, d], and Vec::with_capacity(n), will all
        // > produce a Vec with at least the requested capacity.
        let alloc_size = len * std::mem::size_of::<T>();
        let reservation = manager.reserve_external(alloc_size)?;

        let mut data: Vec<T> = vec![T::default(); len];

        let ptr = data.as_mut_ptr();
        let len = data.len();
        let cap = data.capacity();

        let additional = (cap * std::mem::size_of::<T>()) - alloc_size;
        if additional > 0 {
            // TODO: Combine
            // let additional = manager.reserve_external(additional)?;
            // reservation = reservation.combine(additional);
        }

        std::mem::forget(data);

        Ok(RawBufferParts {
            reservation,
            ptr: ptr.cast(),
            len,
            cap,
        })
    }

    pub unsafe fn as_slice<T>(&self) -> &[T] {
        std::slice::from_raw_parts(self.ptr.cast::<T>().cast_const(), self.len)
    }

    pub unsafe fn as_slice_mut<T>(&mut self) -> &mut [T] {
        std::slice::from_raw_parts_mut(self.ptr.cast::<T>(), self.len)
    }

    pub unsafe fn resize<T: Default + Copy>(&mut self, manager: &B, len: usize) -> Result<()> {
        if self.len == 0 {
            // Special case when length is zero.
            //
            // We want to enable the use case where we initialize the buffer to
            // nothing (null) and later append to it. However, the `T` that we
            // pass in here might have a different alignment which wouldn't be
            // safe.
            //
            // By just creating a new buffer, we can avoid that issue.
            let new_self = Self::try_new::<T>(manager, len)?;
            *self = new_self;
            return Ok(());
        }

        debug_assert_eq!(self.ptr as usize % std::mem::size_of::<T>(), 0);

        let mut data: Vec<T> = Vec::from_raw_parts(self.ptr.cast(), self.len, self.cap);

        // TODO: Reservation stuff.

        data.resize(len, T::default());

        self.ptr = data.as_mut_ptr().cast();
        self.len = data.len();
        self.cap = data.capacity();

        std::mem::forget(data);

        Ok(())
    }
}

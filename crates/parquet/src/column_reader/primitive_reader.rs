use rayexec_error::Result;
use rayexec_execution::arrays::array::buffer_manager::BufferManager;
use rayexec_execution::arrays::array::physical_type::{AsSliceMut, MutableScalarStorage};
use rayexec_execution::arrays::array::Array;

use super::buffer::ReadBuffer;
use crate::schema::types::ColumnDescPtr;

#[derive(Debug)]
pub struct PrimitiveColumnReader<B: BufferManager, S: MutableScalarStorage> {
    column: ColumnDescPtr,
    _b: B,
    _s: S,
}

impl<B, S> PrimitiveColumnReader<B, S>
where
    B: BufferManager,
    S: MutableScalarStorage,
    S::StorageType: Sized,
    for<'a> S::AddressableMut<'a, B>: AsSliceMut<S::StorageType>,
{
    /// Reads a plain encoding into the output array.
    ///
    /// Increments the internal read offset in the read buffer by the amount we
    /// write to the array.
    pub fn read_plain(
        &self,
        buffer: &mut ReadBuffer<B>,
        defs: &[i16],
        out: &mut Array<B>,
        count: usize,
    ) -> Result<()> {
        let (data, validity) = out.data_and_validity_mut();

        let mut out = S::get_addressable_mut(data)?;
        let out = out.as_slice_mut();

        if !self.has_def() {
            // No nulls to worry about, copy directly.
            let out = &mut out[..count];
            // SAFETY: TODO...
            unsafe {
                buffer.read_copy(out);
            }
        } else {
            // Otherwise need to handle nulls.
            for idx in 0..count {
                if defs[idx] != self.max_def() {
                    // Null value.
                    validity.set_invalid(idx);
                    continue;
                }
                // SAFETY: TODO...
                out[idx] = unsafe { buffer.read_next() };
            }
        }

        Ok(())
    }

    pub fn skip_plain(buffer: &mut ReadBuffer<B>, defs: &[u8], count: usize) -> Result<()> {
        unimplemented!()
    }

    fn has_def(&self) -> bool {
        self.column.max_def_level() > 0
    }

    fn max_def(&self) -> i16 {
        self.column.max_def_level()
    }
}

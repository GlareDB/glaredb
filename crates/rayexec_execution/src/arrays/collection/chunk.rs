use rayexec_error::Result;

use crate::arrays::array::array_buffer::ArrayBuffer;
use crate::arrays::array::validity::Validity;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::compute::copy::copy_rows_raw;
use crate::arrays::datatype::DataType;
use crate::buffer::buffer_manager::AsRawBufferManager;

#[derive(Debug)]
pub struct ColumnChunk {
    pub buffers: Vec<ColumnBuffer>,
    pub capacity: usize,
    pub filled: usize,
}

impl ColumnChunk {
    pub fn try_new(
        manager: &impl AsRawBufferManager,
        datatypes: &[DataType],
        capacity: usize,
    ) -> Result<Self> {
        let mut buffers = Vec::with_capacity(datatypes.len());
        for datatype in datatypes {
            let buffer = ArrayBuffer::try_new_for_datatype(manager, datatype, capacity)?;
            buffers.push(ColumnBuffer {
                validity: Validity::new_all_valid(capacity),
                buffer,
            });
        }

        Ok(ColumnChunk {
            buffers,
            capacity,
            filled: 0,
        })
    }

    /// Make all buffers in this chunk shared.
    pub fn make_all_shared(&mut self) {
        for buf in &mut self.buffers {
            buf.buffer.make_shared();
        }
    }

    /// Copy rows from the arrays into the chunk buffers.
    ///
    /// `src` must contain the same number of arrays of the buffers in this
    /// chunk.
    ///
    /// `count` must not exceed the remaining capacity of the chunk.
    pub fn copy_rows(&mut self, src: &[Array], src_offset: usize, count: usize) -> Result<()> {
        debug_assert_eq!(self.buffers.len(), src.len());
        debug_assert!(count <= self.capacity - self.filled);

        for (src, dest) in src.iter().zip(&mut self.buffers) {
            dest.copy_rows_from_array(self.filled, src, src_offset, count)?;
        }
        self.filled += count;

        Ok(())
    }

    pub fn scan(&self, output: &mut Batch) -> Result<usize> {
        debug_assert_eq!(self.buffers.len(), output.arrays.len());
        for (output, buffer) in output.arrays.iter_mut().zip(&self.buffers) {
            buffer.clone_to_array(output)?;
        }
        output.set_num_rows(self.filled)?;

        Ok(self.filled)
    }
}

#[derive(Debug)]
pub struct ColumnBuffer {
    pub validity: Validity,
    pub buffer: ArrayBuffer,
}

impl ColumnBuffer {
    pub fn clone_to_array(&self, array: &mut Array) -> Result<()> {
        array.validity = self.validity.clone();
        array.data = self.buffer.try_clone_shared()?;
        Ok(())
    }

    /// Copy rows from a source buffer and validity into this buffer.
    fn copy_rows_from_array(
        &mut self,
        dest_offset: usize,
        src: &Array,
        src_offset: usize,
        count: usize,
    ) -> Result<()> {
        let phys_type = src.datatype().physical_type();

        // src => dest mapping.
        let mapping = (src_offset..(src_offset + count)).zip(dest_offset..(dest_offset + count));

        if src.should_flatten_for_execution() {
            let src = src.flatten()?;
            copy_rows_raw(
                phys_type,
                src.array_buffer,
                src.validity,
                Some(src.selection),
                mapping,
                &mut self.buffer,
                &mut self.validity,
            )
        } else {
            copy_rows_raw(
                phys_type,
                &src.data,
                &src.validity,
                None,
                mapping,
                &mut self.buffer,
                &mut self.validity,
            )
        }
    }
}

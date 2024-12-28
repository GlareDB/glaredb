use rayexec_error::Result;

use super::array_data::ArrayData;
use super::flat::FlatArrayView;
use super::validity::Validity;
use crate::arrays::buffer::buffer_manager::{BufferManager, NopBufferManager};
use crate::arrays::buffer::physical_type::{
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalInterval,
    PhysicalType,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUtf8,
};
use crate::arrays::buffer::string_view::StringViewHeap;
use crate::arrays::buffer::{ArrayBuffer, SecondaryBuffer};
use crate::arrays::datatype::DataType;

#[derive(Debug)]
pub struct Array<B: BufferManager = NopBufferManager> {
    pub(crate) datatype: DataType,
    pub(crate) validity: Validity,
    pub(crate) data: ArrayData<B>,
}

impl Array<NopBufferManager> {
    /// Create a new array with the given capacity.
    ///
    /// This will take care of initalizing the primary and secondary data
    /// buffers depending on the type.
    pub fn new(datatype: DataType, capacity: usize) -> Result<Self> {
        let manager = NopBufferManager;

        let buffer = match datatype.physical_type() {
            PhysicalType::Int8 => {
                ArrayBuffer::with_primary_capacity::<PhysicalI8>(&manager, capacity)?
            }
            PhysicalType::Int16 => {
                ArrayBuffer::with_primary_capacity::<PhysicalI16>(&manager, capacity)?
            }
            PhysicalType::Int32 => {
                ArrayBuffer::with_primary_capacity::<PhysicalI32>(&manager, capacity)?
            }
            PhysicalType::Int64 => {
                ArrayBuffer::with_primary_capacity::<PhysicalI64>(&manager, capacity)?
            }
            PhysicalType::Int128 => {
                ArrayBuffer::with_primary_capacity::<PhysicalI128>(&manager, capacity)?
            }
            PhysicalType::UInt8 => {
                ArrayBuffer::with_primary_capacity::<PhysicalU8>(&manager, capacity)?
            }
            PhysicalType::UInt16 => {
                ArrayBuffer::with_primary_capacity::<PhysicalU16>(&manager, capacity)?
            }
            PhysicalType::UInt32 => {
                ArrayBuffer::with_primary_capacity::<PhysicalU32>(&manager, capacity)?
            }
            PhysicalType::UInt64 => {
                ArrayBuffer::with_primary_capacity::<PhysicalU64>(&manager, capacity)?
            }
            PhysicalType::UInt128 => {
                ArrayBuffer::with_primary_capacity::<PhysicalU128>(&manager, capacity)?
            }
            PhysicalType::Float16 => {
                ArrayBuffer::with_primary_capacity::<PhysicalF16>(&manager, capacity)?
            }
            PhysicalType::Float32 => {
                ArrayBuffer::with_primary_capacity::<PhysicalF32>(&manager, capacity)?
            }
            PhysicalType::Float64 => {
                ArrayBuffer::with_primary_capacity::<PhysicalF64>(&manager, capacity)?
            }
            PhysicalType::Interval => {
                ArrayBuffer::with_primary_capacity::<PhysicalInterval>(&manager, capacity)?
            }
            PhysicalType::Utf8 => {
                let mut buffer =
                    ArrayBuffer::with_primary_capacity::<PhysicalUtf8>(&manager, capacity)?;
                buffer.put_secondary_buffer(SecondaryBuffer::StringViewHeap(StringViewHeap::new()));
                buffer
            }
            _ => unimplemented!(),
        };

        let validity = Validity::new_all_valid(capacity);

        Ok(Array {
            datatype,
            validity,
            data: ArrayData::owned(buffer),
        })
    }
}

impl<B> Array<B>
where
    B: BufferManager,
{
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    /// If this array is a dictionary array.
    pub fn is_dictionary(&self) -> bool {
        self.data.physical_type() == PhysicalType::Dictionary
    }

    /// Return a flat array view for this array.
    pub fn flat_view(&self) -> Result<FlatArrayView<B>> {
        FlatArrayView::from_array(self)
    }
}

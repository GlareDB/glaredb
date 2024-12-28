use half::f16;
use iterutil::{IntoExactSizeIterator, TryFromExactSizeIterator};
use rayexec_error::{RayexecError, Result};

use super::array_data::ArrayData;
use super::flat::FlatArrayView;
use super::validity::Validity;
use crate::arrays::buffer::buffer_manager::{BufferManager, NopBufferManager};
use crate::arrays::buffer::physical_type::{
    Addressable,
    AddressableMut,
    MutablePhysicalStorage,
    PhysicalBool,
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
use crate::arrays::scalar::interval::Interval;

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
            PhysicalType::Boolean => {
                ArrayBuffer::with_primary_capacity::<PhysicalBool>(&manager, capacity)?
            }
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

    /// Copy rows from self to another array.
    ///
    /// `mapping` provides a mapping of source indices to destination indices in
    /// (source, dest) pairs.
    pub fn copy_rows(
        &self,
        mapping: impl IntoExactSizeIterator<Item = (usize, usize)>,
        dest: &mut Self,
    ) -> Result<()> {
        match self.datatype.physical_type() {
            PhysicalType::Boolean => copy_rows::<PhysicalBool, _>(self, mapping, dest)?,
            PhysicalType::Int8 => copy_rows::<PhysicalI8, _>(self, mapping, dest)?,
            PhysicalType::Int16 => copy_rows::<PhysicalI16, _>(self, mapping, dest)?,
            PhysicalType::Int32 => copy_rows::<PhysicalI32, _>(self, mapping, dest)?,
            PhysicalType::Int64 => copy_rows::<PhysicalI64, _>(self, mapping, dest)?,
            PhysicalType::Int128 => copy_rows::<PhysicalI128, _>(self, mapping, dest)?,
            PhysicalType::UInt8 => copy_rows::<PhysicalU8, _>(self, mapping, dest)?,
            PhysicalType::UInt16 => copy_rows::<PhysicalU16, _>(self, mapping, dest)?,
            PhysicalType::UInt32 => copy_rows::<PhysicalU32, _>(self, mapping, dest)?,
            PhysicalType::UInt64 => copy_rows::<PhysicalU64, _>(self, mapping, dest)?,
            PhysicalType::UInt128 => copy_rows::<PhysicalU128, _>(self, mapping, dest)?,
            PhysicalType::Float16 => copy_rows::<PhysicalF16, _>(self, mapping, dest)?,
            PhysicalType::Float32 => copy_rows::<PhysicalF32, _>(self, mapping, dest)?,
            PhysicalType::Float64 => copy_rows::<PhysicalF64, _>(self, mapping, dest)?,
            PhysicalType::Interval => copy_rows::<PhysicalInterval, _>(self, mapping, dest)?,
            PhysicalType::Utf8 => copy_rows::<PhysicalUtf8, _>(self, mapping, dest)?,
            _ => unimplemented!(),
        }

        Ok(())
    }
}

/// Helper for copying rows.
fn copy_rows<S, B>(
    from: &Array<B>,
    mapping: impl IntoExactSizeIterator<Item = (usize, usize)>,
    to: &mut Array<B>,
) -> Result<()>
where
    S: MutablePhysicalStorage,
    B: BufferManager,
{
    let from_flat = from.flat_view()?;
    let from_storage = S::get_addressable(from_flat.array_buffer)?;

    let to_data = to.data.try_as_mut()?;
    let mut to_storage = S::get_addressable_mut(to_data)?;

    if from_flat.validity.all_valid() && to.validity.all_valid() {
        for (from_idx, to_idx) in mapping.into_iter() {
            let from_idx = from_flat.selection.get(from_idx).unwrap();
            let v = from_storage.get(from_idx).unwrap();
            to_storage.put(to_idx, v);
        }
    } else {
        for (from_idx, to_idx) in mapping.into_iter() {
            let from_idx = from_flat.selection.get(from_idx).unwrap();
            if from_flat.validity.is_valid(from_idx) {
                let v = from_storage.get(from_idx).unwrap();
                to_storage.put(to_idx, v);
            } else {
                to.validity.set_invalid(to_idx);
            }
        }
    }

    Ok(())
}

/// Implements `try_from_iter` for primitive types.
///
/// Note these create arrays using Nop buffer manager and so really only
/// suitable for tests right now.
macro_rules! impl_primitive_from_iter {
    ($prim:ty, $phys:ty, $typ_variant:ident) => {
        impl TryFromExactSizeIterator<$prim> for Array<NopBufferManager> {
            type Error = RayexecError;

            fn try_from_iter<T: IntoExactSizeIterator<Item = $prim>>(
                iter: T,
            ) -> Result<Self, Self::Error> {
                let iter = iter.into_iter();

                let mut array = Array::new(DataType::$typ_variant, iter.len())?;
                let slice = array.data.try_as_mut()?.try_as_slice_mut::<$phys>()?;

                for (dest, v) in slice.iter_mut().zip(iter) {
                    *dest = v;
                }

                Ok(array)
            }
        }
    };
}

impl_primitive_from_iter!(bool, PhysicalBool, Boolean);

impl_primitive_from_iter!(i8, PhysicalI8, Int8);
impl_primitive_from_iter!(i16, PhysicalI16, Int16);
impl_primitive_from_iter!(i32, PhysicalI32, Int32);
impl_primitive_from_iter!(i64, PhysicalI64, Int64);
impl_primitive_from_iter!(i128, PhysicalI128, Int128);

impl_primitive_from_iter!(u8, PhysicalU8, UInt8);
impl_primitive_from_iter!(u16, PhysicalU16, UInt16);
impl_primitive_from_iter!(u32, PhysicalU32, UInt32);
impl_primitive_from_iter!(u64, PhysicalU64, UInt64);
impl_primitive_from_iter!(u128, PhysicalU128, UInt128);

impl_primitive_from_iter!(f16, PhysicalF16, Float16);
impl_primitive_from_iter!(f32, PhysicalF32, Float32);
impl_primitive_from_iter!(f64, PhysicalF64, Float64);

impl_primitive_from_iter!(Interval, PhysicalInterval, Interval);

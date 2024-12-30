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
    PhysicalDictionary,
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
use crate::arrays::buffer::{ArrayBuffer, DictionaryBuffer, SecondaryBuffer};
use crate::arrays::datatype::DataType;
use crate::arrays::scalar::interval::Interval;

#[derive(Debug)]
pub struct Array<B: BufferManager = NopBufferManager> {
    pub(crate) datatype: DataType,
    pub(crate) validity: Validity,
    pub(crate) data: ArrayData<B>,
}

impl<B> Array<B>
where
    B: BufferManager,
{
    /// Create a new array with the given capacity.
    ///
    /// This will take care of initalizing the primary and secondary data
    /// buffers depending on the type.
    pub fn new(manager: &B, datatype: DataType, capacity: usize) -> Result<Self> {
        let buffer = match datatype.physical_type() {
            PhysicalType::Boolean => {
                ArrayBuffer::with_primary_capacity::<PhysicalBool>(manager, capacity)?
            }
            PhysicalType::Int8 => {
                ArrayBuffer::with_primary_capacity::<PhysicalI8>(manager, capacity)?
            }
            PhysicalType::Int16 => {
                ArrayBuffer::with_primary_capacity::<PhysicalI16>(manager, capacity)?
            }
            PhysicalType::Int32 => {
                ArrayBuffer::with_primary_capacity::<PhysicalI32>(manager, capacity)?
            }
            PhysicalType::Int64 => {
                ArrayBuffer::with_primary_capacity::<PhysicalI64>(manager, capacity)?
            }
            PhysicalType::Int128 => {
                ArrayBuffer::with_primary_capacity::<PhysicalI128>(manager, capacity)?
            }
            PhysicalType::UInt8 => {
                ArrayBuffer::with_primary_capacity::<PhysicalU8>(manager, capacity)?
            }
            PhysicalType::UInt16 => {
                ArrayBuffer::with_primary_capacity::<PhysicalU16>(manager, capacity)?
            }
            PhysicalType::UInt32 => {
                ArrayBuffer::with_primary_capacity::<PhysicalU32>(manager, capacity)?
            }
            PhysicalType::UInt64 => {
                ArrayBuffer::with_primary_capacity::<PhysicalU64>(manager, capacity)?
            }
            PhysicalType::UInt128 => {
                ArrayBuffer::with_primary_capacity::<PhysicalU128>(manager, capacity)?
            }
            PhysicalType::Float16 => {
                ArrayBuffer::with_primary_capacity::<PhysicalF16>(manager, capacity)?
            }
            PhysicalType::Float32 => {
                ArrayBuffer::with_primary_capacity::<PhysicalF32>(manager, capacity)?
            }
            PhysicalType::Float64 => {
                ArrayBuffer::with_primary_capacity::<PhysicalF64>(manager, capacity)?
            }
            PhysicalType::Interval => {
                ArrayBuffer::with_primary_capacity::<PhysicalInterval>(manager, capacity)?
            }
            PhysicalType::Utf8 => {
                let mut buffer =
                    ArrayBuffer::with_primary_capacity::<PhysicalUtf8>(manager, capacity)?;
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

    pub fn datatype(&self) -> &DataType {
        &self.datatype
    }

    pub fn data(&self) -> &ArrayData<B> {
        &self.data
    }

    pub fn data_mut(&mut self) -> &mut ArrayData<B> {
        &mut self.data
    }

    pub fn validity(&self) -> &Validity {
        &self.validity
    }

    pub fn put_validity(&mut self, validity: Validity) -> Result<()> {
        if validity.len() != self.data().capacity() {
            return Err(RayexecError::new("Invalid validity length")
                .with_field("got", validity.len())
                .with_field("want", self.data.capacity()));
        }
        self.validity = validity;
        Ok(())
    }

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

    /// Selects indice from the array.
    ///
    /// This will convert the underlying array buffer into a dictionary buffer.
    pub fn select(
        &mut self,
        manager: &B,
        selection: impl IntoExactSizeIterator<Item = usize>,
    ) -> Result<()> {
        if self.is_dictionary() {
            // Already dictionary, select the selection.
            let sel = selection.into_iter();
            let mut new_buf =
                ArrayBuffer::with_primary_capacity::<PhysicalDictionary>(manager, sel.len())?;

            let old_sel = self.data.try_as_slice::<PhysicalDictionary>()?;
            let new_sel = new_buf.try_as_slice_mut::<PhysicalDictionary>()?;

            for (sel_idx, sel_buf) in sel.zip(new_sel) {
                let idx = old_sel[sel_idx];
                *sel_buf = idx;
            }

            // Now swap the secondary buffers, the dictionary buffer will now be
            // on `new_buf`.
            std::mem::swap(
                self.data.try_as_mut()?.get_secondary_mut(), // TODO: Should just clone the pointer if managed.
                new_buf.get_secondary_mut(),
            );

            // And set the new buf, old buf gets dropped.
            self.data = ArrayData::owned(new_buf);

            return Ok(());
        }

        let sel = selection.into_iter();
        let mut new_buf =
            ArrayBuffer::with_primary_capacity::<PhysicalDictionary>(manager, sel.len())?;

        let new_buf_slice = new_buf.try_as_slice_mut::<PhysicalDictionary>()?;

        // Set all selection indices in the new array buffer.
        for (sel_idx, sel_buf) in sel.zip(new_buf_slice) {
            *sel_buf = sel_idx
        }

        // TODO: Probably verify selection all in bounds.

        // Now replace the original buffer, and put the original buffer in the
        // secondary buffer.
        let orig_validity = std::mem::replace(
            &mut self.validity,
            Validity::new_all_valid(new_buf.capacity()),
        );
        let orig_buffer = std::mem::replace(&mut self.data, ArrayData::owned(new_buf));
        // TODO: Should just clone the pointer if managed.
        self.data
            .try_as_mut()?
            .put_secondary_buffer(SecondaryBuffer::Dictionary(DictionaryBuffer {
                validity: orig_validity,
                buffer: orig_buffer,
            }));

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

                let manager = NopBufferManager;

                let mut array = Array::new(&manager, DataType::$typ_variant, iter.len())?;
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

impl<'a> TryFromExactSizeIterator<&'a str> for Array<NopBufferManager> {
    type Error = RayexecError;

    fn try_from_iter<T: IntoExactSizeIterator<Item = &'a str>>(
        iter: T,
    ) -> Result<Self, Self::Error> {
        let iter = iter.into_iter();
        let len = iter.len();

        let mut buffer =
            ArrayBuffer::with_primary_capacity::<PhysicalUtf8>(&NopBufferManager, len)?;
        buffer.put_secondary_buffer(SecondaryBuffer::StringViewHeap(StringViewHeap::new()));

        let mut addressable = buffer.try_as_string_view_addressable_mut()?;

        for (idx, v) in iter.enumerate() {
            addressable.put(idx, v);
        }

        Ok(Array {
            datatype: DataType::Utf8,
            validity: Validity::new_all_valid(len),
            data: ArrayData::owned(buffer),
        })
    }
}

/// From iterator implementation that creates an array from optionally valid
/// values. Some is treated as valid, None as invalid.
impl<V> TryFromExactSizeIterator<Option<V>> for Array<NopBufferManager>
where
    V: Default,
    Array<NopBufferManager>: TryFromExactSizeIterator<V, Error = RayexecError>,
{
    type Error = RayexecError;

    fn try_from_iter<T: IntoExactSizeIterator<Item = Option<V>>>(
        iter: T,
    ) -> Result<Self, Self::Error> {
        let iter = iter.into_iter();
        let len = iter.len();

        let mut validity = Validity::new_all_valid(len);

        // New iterator that just uses the default value for missing values, and
        // sets the validity as appropriate.
        let iter = iter.enumerate().map(|(idx, v)| {
            if v.is_none() {
                validity.set_invalid(idx);
            }
            v.unwrap_or_default()
        });

        let mut array = Self::try_from_iter(iter)?;
        array.put_validity(validity)?;

        Ok(array)
    }
}

use glaredb_error::{DbError, Result, not_implemented};

use crate::arrays::array::Array;
use crate::arrays::array::array_buffer::{
    AnyArrayBuffer,
    ArrayBufferDowncast,
    ListBuffer,
    ListItemMetadata,
};
use crate::arrays::array::execution_format::ExecutionFormatMut;
use crate::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalBinary,
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI8,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI128,
    PhysicalInterval,
    PhysicalType,
    PhysicalU8,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU128,
    PhysicalUntypedNull,
    PhysicalUtf8,
};
use crate::arrays::array::validity::Validity;
use crate::arrays::datatype::DataType;
use crate::arrays::scalar::BorrowedScalarValue;
use crate::arrays::scalar::unwrap::{
    NullableValue,
    ScalarValueUnwrap,
    UnwrapBinary,
    UnwrapBool,
    UnwrapF16,
    UnwrapF32,
    UnwrapF64,
    UnwrapI8,
    UnwrapI16,
    UnwrapI32,
    UnwrapI64,
    UnwrapI128,
    UnwrapInterval,
    UnwrapU8,
    UnwrapU16,
    UnwrapU32,
    UnwrapU64,
    UnwrapU128,
    UnwrapUntypedNull,
    UnwrapUtf8,
};

/// Set the list value for a given row.
pub fn set_list_value(
    array: &mut Array,
    list_value: &[BorrowedScalarValue<'_>],
    row_idx: usize,
) -> Result<()> {
    set_list_value_raw(
        &array.datatype,
        &mut array.validity,
        &mut array.data,
        list_value,
        row_idx,
    )
}

pub fn set_list_value_raw(
    datatype: &DataType,
    _validity: &mut Validity,
    buffer: &mut AnyArrayBuffer,
    list_value: &[BorrowedScalarValue<'_>],
    row_idx: usize,
) -> Result<()> {
    let inner_type = datatype.try_get_list_type_meta()?.datatype.physical_type();

    // TODO: Do we need validate type metadata like decimal precision?

    match inner_type {
        PhysicalType::UntypedNull => {
            set_list_scalar::<PhysicalUntypedNull, UnwrapUntypedNull>(buffer, list_value, row_idx)
        }
        PhysicalType::Boolean => {
            set_list_scalar::<PhysicalBool, UnwrapBool>(buffer, list_value, row_idx)
        }
        PhysicalType::Int8 => set_list_scalar::<PhysicalI8, UnwrapI8>(buffer, list_value, row_idx),
        PhysicalType::Int16 => {
            set_list_scalar::<PhysicalI16, UnwrapI16>(buffer, list_value, row_idx)
        }
        PhysicalType::Int32 => {
            set_list_scalar::<PhysicalI32, UnwrapI32>(buffer, list_value, row_idx)
        }
        PhysicalType::Int64 => {
            set_list_scalar::<PhysicalI64, UnwrapI64>(buffer, list_value, row_idx)
        }
        PhysicalType::Int128 => {
            set_list_scalar::<PhysicalI128, UnwrapI128>(buffer, list_value, row_idx)
        }
        PhysicalType::UInt8 => set_list_scalar::<PhysicalU8, UnwrapU8>(buffer, list_value, row_idx),
        PhysicalType::UInt16 => {
            set_list_scalar::<PhysicalU16, UnwrapU16>(buffer, list_value, row_idx)
        }
        PhysicalType::UInt32 => {
            set_list_scalar::<PhysicalU32, UnwrapU32>(buffer, list_value, row_idx)
        }
        PhysicalType::UInt64 => {
            set_list_scalar::<PhysicalU64, UnwrapU64>(buffer, list_value, row_idx)
        }
        PhysicalType::UInt128 => {
            set_list_scalar::<PhysicalU128, UnwrapU128>(buffer, list_value, row_idx)
        }
        PhysicalType::Float16 => {
            set_list_scalar::<PhysicalF16, UnwrapF16>(buffer, list_value, row_idx)
        }
        PhysicalType::Float32 => {
            set_list_scalar::<PhysicalF32, UnwrapF32>(buffer, list_value, row_idx)
        }
        PhysicalType::Float64 => {
            set_list_scalar::<PhysicalF64, UnwrapF64>(buffer, list_value, row_idx)
        }
        PhysicalType::Interval => {
            set_list_scalar::<PhysicalInterval, UnwrapInterval>(buffer, list_value, row_idx)
        }
        PhysicalType::Utf8 => {
            set_list_scalar::<PhysicalUtf8, UnwrapUtf8>(buffer, list_value, row_idx)
        }
        PhysicalType::Binary => {
            set_list_scalar::<PhysicalBinary, UnwrapBinary>(buffer, list_value, row_idx)
        }
        other => not_implemented!("self list value for physical type {other}"),
    }
}

fn set_list_scalar<S, U>(
    buffer: &mut AnyArrayBuffer,
    list_value: &[BorrowedScalarValue<'_>],
    row_idx: usize,
) -> Result<()>
where
    S: MutableScalarStorage,
    U: ScalarValueUnwrap<StorageType = S::StorageType>,
{
    let list_buffer = match ListBuffer::downcast_execution_format_mut(buffer)? {
        ExecutionFormatMut::Flat(buf) => buf,
        ExecutionFormatMut::Selection(_) => {
            return Err(DbError::new(
                "Cannot set list value with selection on list array",
            ));
        }
    };

    // TODO: This always grows the child buffers.

    let grow = list_value.len();
    let child = &mut list_buffer.child;
    let child_len = child.validity.len();

    // Guard against child_len being zero, which may be the case if this list
    // array is actually all NULLs.
    let append_offset = child_len.saturating_sub(1);

    // TODO: I mean, ok. Definitely just trying to avoid working with the bitmap
    // here.
    child.validity.select(0..(child_len + grow));
    let rem = child.buffer.logical_len() - child_len;
    if rem < grow {
        child.buffer.resize(child_len + grow)?;
    }

    let child_buf = S::buffer_downcast_mut(&mut child.buffer)?;
    let mut child_addr = S::addressable_mut(child_buf);

    for (elem_idx, val) in list_value.iter().enumerate() {
        let child_idx = append_offset + elem_idx;
        match U::try_unwrap(val)? {
            NullableValue::Value(v) => {
                child.validity.set_valid(child_idx);
                child_addr.put(child_idx, v);
            }
            NullableValue::Null => {
                child.validity.set_invalid(child_idx);
            }
        }
    }

    // Update metadata.
    list_buffer.metadata.as_slice_mut()[row_idx] = ListItemMetadata {
        offset: append_offset as i32,
        len: list_value.len() as i32,
    };

    Ok(())
}

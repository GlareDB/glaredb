use rayexec_error::{not_implemented, RayexecError, Result};
use stdutil::convert::TryAsMut;
use stdutil::iter::IntoExactSizeIterator;

use crate::arrays::array::array_buffer::{ArrayBufferType, ListItemMetadata, SharedOrOwned};
use crate::arrays::array::physical_type::{
    Addressable,
    AddressableMut,
    MutableScalarStorage,
    PhysicalBinary,
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
    PhysicalUntypedNull,
    PhysicalUtf8,
};
use crate::arrays::array::validity::Validity;
use crate::arrays::array::Array;
use crate::arrays::datatype::DataType;

/// Writes lists values from `inputs` into `output`.
///
/// Each array in `inputs` represents a list element at that same ordinal. All
/// input arrays need to be the same length.
///
/// `sel` corresponds to the rows we should take from inputs.
pub fn make_list_from_values(
    inputs: &[Array],
    sel: impl IntoExactSizeIterator<Item = usize>,
    output: &mut Array,
) -> Result<()> {
    let inner_type = match output.datatype() {
        DataType::List(m) => m.datatype.physical_type(),
        other => {
            return Err(RayexecError::new(format!(
                "Expected output to be list datatype, got {other}",
            )))
        }
    };

    match inner_type {
        PhysicalType::UntypedNull => {
            make_list_from_values_inner::<PhysicalUntypedNull>(inputs, sel, output)
        }
        PhysicalType::Boolean => make_list_from_values_inner::<PhysicalBool>(inputs, sel, output),
        PhysicalType::Int8 => make_list_from_values_inner::<PhysicalI8>(inputs, sel, output),
        PhysicalType::Int16 => make_list_from_values_inner::<PhysicalI16>(inputs, sel, output),
        PhysicalType::Int32 => make_list_from_values_inner::<PhysicalI32>(inputs, sel, output),
        PhysicalType::Int64 => make_list_from_values_inner::<PhysicalI64>(inputs, sel, output),
        PhysicalType::Int128 => make_list_from_values_inner::<PhysicalI128>(inputs, sel, output),
        PhysicalType::UInt8 => make_list_from_values_inner::<PhysicalU8>(inputs, sel, output),
        PhysicalType::UInt16 => make_list_from_values_inner::<PhysicalU16>(inputs, sel, output),
        PhysicalType::UInt32 => make_list_from_values_inner::<PhysicalU32>(inputs, sel, output),
        PhysicalType::UInt64 => make_list_from_values_inner::<PhysicalU64>(inputs, sel, output),
        PhysicalType::UInt128 => make_list_from_values_inner::<PhysicalU128>(inputs, sel, output),
        PhysicalType::Float16 => make_list_from_values_inner::<PhysicalF16>(inputs, sel, output),
        PhysicalType::Float32 => make_list_from_values_inner::<PhysicalF32>(inputs, sel, output),
        PhysicalType::Float64 => make_list_from_values_inner::<PhysicalF64>(inputs, sel, output),
        PhysicalType::Interval => {
            make_list_from_values_inner::<PhysicalInterval>(inputs, sel, output)
        }
        PhysicalType::Utf8 => make_list_from_values_inner::<PhysicalUtf8>(inputs, sel, output),
        PhysicalType::Binary => make_list_from_values_inner::<PhysicalBinary>(inputs, sel, output),
        other => not_implemented!("list values for physical type {other}"),
    }
}

/// Helper for constructing the list values and writing them to `output`.
///
/// This will overwrite any existing data in `output`.
///
/// `S` should be the inner type.
fn make_list_from_values_inner<S: MutableScalarStorage>(
    inputs: &[Array],
    sel: impl IntoExactSizeIterator<Item = usize>,
    output: &mut Array,
) -> Result<()> {
    // TODO: Dictionary

    let sel = sel.into_exact_size_iter();
    let num_rows = sel.len();
    let total_capacity = sel.len() * inputs.len();

    let list_buf = match output.data.as_mut() {
        ArrayBufferType::List(list_buf) => list_buf,
        _ => return Err(RayexecError::new("Expected list buffer")),
    };

    let metadata = list_buf.metadata.try_as_mut()?;
    if metadata.capacity() < num_rows {
        metadata.reserve_additional(num_rows - metadata.capacity())?;
    }

    // Overwrite validity with new capacity.
    list_buf.child_validity = SharedOrOwned::owned(Validity::new_all_valid(total_capacity));
    let child_validity = list_buf.child_validity.try_as_mut()?;

    // Ensure we have appropriate capacity in the child buffer.
    if list_buf.child_buffer.logical_len() < total_capacity {
        let additional = total_capacity - list_buf.child_buffer.logical_len();
        S::try_reserve(&mut list_buf.child_buffer, additional)?;
    }

    // Update metadata on the list buffer itself. Note that this can be less
    // than the buffer's actual capacity. This only matters during writes to
    // know if we still have room to push to the child array.
    list_buf.entries = total_capacity;

    // (flat, addressable) pairs.
    let element_bufs = inputs
        .iter()
        .map(|arr| {
            let flat = arr.flatten()?;
            let addressable = S::get_addressable(flat.array_buffer)?;
            Ok((flat, addressable))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut output_buf = S::get_addressable_mut(&mut list_buf.child_buffer)?;

    let mut output_idx = 0;
    for row_idx in sel {
        for (flat, buffer) in &element_bufs {
            if flat.validity.is_valid(row_idx) {
                output_buf.put(output_idx, buffer.get(row_idx).unwrap());
            } else {
                child_validity.set_invalid(output_idx);
            }
            output_idx += 1;
        }
    }

    // Now generate and set the metadatas.
    let metadata = metadata.as_slice_mut();
    let len = inputs.len() as i32;

    for output_idx in 0..num_rows {
        metadata[output_idx] = ListItemMetadata {
            offset: (output_idx as i32) * len,
            len,
        };
    }

    Ok(())
}

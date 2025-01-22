use rayexec_error::{not_implemented, RayexecError, Result};
use stdutil::iter::IntoExactSizeIterator;

use crate::arrays::array::array_buffer::{ListItemMetadata, SecondaryBuffer};
use crate::arrays::array::physical_type::{
    Addressable,
    AddressableMut,
    MutablePhysicalStorage,
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
    PhysicalList,
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
/// `S` should be the inner type.
fn make_list_from_values_inner<S: MutablePhysicalStorage>(
    inputs: &[Array],
    sel: impl IntoExactSizeIterator<Item = usize>,
    output: &mut Array,
) -> Result<()> {
    // TODO: Dictionary

    let sel = sel.into_iter();
    let sel_len = sel.len();
    let capacity = sel.len() * inputs.len();

    let list_buf = match output.data.try_as_mut()?.get_secondary_mut() {
        SecondaryBuffer::List(list) => list,
        _ => return Err(RayexecError::new("Expected list buffer")),
    };

    // Resize secondary buffer (and validity) to hold everything.
    //
    // TODO: Need to store buffer manager somewhere else.
    list_buf
        .child
        .data
        .try_as_mut()?
        .reserve_primary::<S>(capacity)?;

    // Replace validity with properly sized one.
    list_buf
        .child
        .put_validity(Validity::new_all_valid(capacity))?;

    // Update metadata on the list buffer itself. Note that this can be less
    // than the buffer's actual capacity. This only matters during writes to
    // know if we still have room to push to the child array.
    list_buf.entries = capacity;

    let child_next = &mut list_buf.child;
    let mut child_outputs = S::get_addressable_mut(child_next.data.try_as_mut()?)?;
    let child_validity = &mut child_next.validity;

    // TODO: Possibly avoid allocating here?
    let col_bufs = inputs
        .iter()
        .map(|arr| S::get_addressable(&arr.data))
        .collect::<Result<Vec<_>>>()?;

    // Write the list values from the input batch.
    let mut output_idx = 0;
    for row_idx in sel {
        for (col, validity) in col_bufs.iter().zip(inputs.iter().map(|arr| &arr.validity)) {
            if validity.is_valid(row_idx) {
                child_outputs.put(output_idx, col.get(row_idx).unwrap());
            } else {
                child_validity.set_invalid(output_idx);
            }

            output_idx += 1;
        }
    }
    std::mem::drop(child_outputs);

    // Now generate and set the metadatas.
    let mut out = PhysicalList::get_addressable_mut(output.data.try_as_mut()?)?;

    let len = inputs.len() as i32;
    for output_idx in 0..sel_len {
        // Note top-level not possible if we're provided a batch.
        out.put(
            output_idx,
            &ListItemMetadata {
                offset: (output_idx as i32) * len,
                len,
            },
        );
    }

    Ok(())
}

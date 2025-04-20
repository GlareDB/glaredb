use glaredb_error::{DbError, Result, not_implemented};

use crate::arrays::array::Array;
use crate::arrays::array::array_buffer::{ArrayBufferDowncast, ListBuffer, ListItemMetadata};
use crate::arrays::array::physical_type::{
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
use crate::arrays::compute::copy::copy_rows_raw;
use crate::arrays::datatype::DataType;
use crate::util::iter::IntoExactSizeIterator;

/// Writes lists values from `inputs` into `output`.
///
/// `output` will be overwritten.
///
/// Each array in `inputs` represents a list element at that same ordinal. All
/// input arrays need to be the same length.
///
/// `sel` corresponds to the rows we should take from inputs.
pub fn make_list(
    inputs: &[Array],
    sel: impl IntoExactSizeIterator<Item = usize> + Clone,
    output: &mut Array,
) -> Result<()> {
    let inner_type = match output.datatype() {
        DataType::List(m) => m.datatype.physical_type(),
        other => {
            return Err(DbError::new(format!(
                "Expected output to be list datatype, got {other}",
            )));
        }
    };

    match inner_type {
        PhysicalType::UntypedNull => make_list_scalar::<PhysicalUntypedNull>(inputs, sel, output),
        PhysicalType::Boolean => make_list_scalar::<PhysicalBool>(inputs, sel, output),
        PhysicalType::Int8 => make_list_scalar::<PhysicalI8>(inputs, sel, output),
        PhysicalType::Int16 => make_list_scalar::<PhysicalI16>(inputs, sel, output),
        PhysicalType::Int32 => make_list_scalar::<PhysicalI32>(inputs, sel, output),
        PhysicalType::Int64 => make_list_scalar::<PhysicalI64>(inputs, sel, output),
        PhysicalType::Int128 => make_list_scalar::<PhysicalI128>(inputs, sel, output),
        PhysicalType::UInt8 => make_list_scalar::<PhysicalU8>(inputs, sel, output),
        PhysicalType::UInt16 => make_list_scalar::<PhysicalU16>(inputs, sel, output),
        PhysicalType::UInt32 => make_list_scalar::<PhysicalU32>(inputs, sel, output),
        PhysicalType::UInt64 => make_list_scalar::<PhysicalU64>(inputs, sel, output),
        PhysicalType::UInt128 => make_list_scalar::<PhysicalU128>(inputs, sel, output),
        PhysicalType::Float16 => make_list_scalar::<PhysicalF16>(inputs, sel, output),
        PhysicalType::Float32 => make_list_scalar::<PhysicalF32>(inputs, sel, output),
        PhysicalType::Float64 => make_list_scalar::<PhysicalF64>(inputs, sel, output),
        PhysicalType::Interval => make_list_scalar::<PhysicalInterval>(inputs, sel, output),
        PhysicalType::Utf8 => make_list_scalar::<PhysicalUtf8>(inputs, sel, output),
        PhysicalType::Binary => make_list_scalar::<PhysicalBinary>(inputs, sel, output),
        other => not_implemented!("list values for physical type {other}"),
    }
}

/// Helper for constructing the list values and writing them to `output`.
///
/// This will overwrite any existing data in `output`.
///
/// `S` should be the inner type.
fn make_list_scalar<S: MutableScalarStorage>(
    inputs: &[Array],
    sel: impl IntoExactSizeIterator<Item = usize> + Clone,
    output: &mut Array,
) -> Result<()> {
    let output_len = sel.clone().into_exact_size_iter().len();
    // A list elements exist. NULLs inside list elements is handled inside the
    // child buffer.
    output.validity = Validity::new_all_valid(output_len);

    let list = ListBuffer::downcast_mut(&mut output.data)?;

    let child = &mut list.child;
    child.validity = Validity::new_all_valid(inputs.len() * output_len);
    child.buffer.resize(inputs.len() * output_len)?;

    // Copy physical values first.
    let stride = inputs.len();
    for (elem_idx, input) in inputs.iter().enumerate() {
        let mapping = sel.clone().into_exact_size_iter().map(|input_idx| {
            let output_idx = input_idx * stride + elem_idx;
            (input_idx, output_idx)
        });

        copy_rows_raw(
            S::PHYSICAL_TYPE,
            &input.data,
            &input.validity,
            mapping,
            &mut child.buffer,
            &mut child.validity,
        )?;
    }

    // SAFETY: We're going to overwriting all values, never reading.
    unsafe { list.metadata.resize_uninit(output_len)? };
    let metadata = list.metadata.as_slice_mut();

    // Now generate metadatas.
    for (idx, m) in metadata.iter_mut().enumerate() {
        *m = ListItemMetadata {
            offset: (idx * stride) as i32,
            len: stride as i32,
        };
    }

    Ok(())
}

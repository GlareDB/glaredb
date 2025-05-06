use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::array_buffer::{ArrayBufferDowncast, ListBuffer, ListItemMetadata};
use crate::arrays::array::validity::Validity;
use crate::arrays::compute::copy::copy_rows_raw;
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
    make_list_scalar(inputs, sel, output)
}

/// Helper for constructing the list values and writing them to `output`.
///
/// This will overwrite any existing data in `output`.
// TODO: Why is this separate?
fn make_list_scalar(
    inputs: &[Array],
    sel: impl IntoExactSizeIterator<Item = usize> + Clone,
    output: &mut Array,
) -> Result<()> {
    let inner_type = &output.datatype.try_get_list_type_meta()?.datatype;

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
            inner_type,
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

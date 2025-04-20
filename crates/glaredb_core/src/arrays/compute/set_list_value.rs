use glaredb_error::{DbError, Result};

use crate::arrays::array::Array;
use crate::arrays::array::array_buffer::{ArrayBufferDowncast, ListBuffer};
use crate::arrays::array::execution_format::ExecutionFormatMut;
use crate::arrays::array::physical_type::MutableScalarStorage;
use crate::arrays::scalar::BorrowedScalarValue;

fn set_list_scalar<S: MutableScalarStorage>(
    array: &mut Array,
    list_value: &[BorrowedScalarValue<'_>],
    row_idx: usize,
) -> Result<()> {
    let list_buffer = match ListBuffer::downcast_execution_format_mut(&mut array.data)? {
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

    // TODO: I mean, ok. Definitely just trying to avoid working with the bitmap
    // here.
    child.validity.select(0..(child_len + grow));
    let rem = child.buffer.logical_len() - child_len;
    if rem < grow {
        child.buffer.resize(child_len + grow)?;
    }

    unimplemented!()
}

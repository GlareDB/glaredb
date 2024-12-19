mod binary;
pub use binary::*;

mod unary;
use rayexec_error::{RayexecError, Result};
pub use unary::*;

use crate::array::{Array, ArrayData};
use crate::bitmap::Bitmap;
use crate::executor::physical_type::PhysicalStorage;
use crate::selection::SelectionVector;

/// Gets the inner array storage. Checks to ensure the inner array does not
/// contain NULLs.
fn get_inner_array_storage<S>(array: &Array) -> Result<(S::Storage<'_>, Option<&Bitmap>)>
where
    S: PhysicalStorage,
{
    match array.array_data() {
        ArrayData::List(d) => {
            let storage = S::get_storage(d.array.array_data())?;
            let validity = d.array.validity();
            Ok((storage, validity))
        }
        _ => Err(RayexecError::new("Expected list array data")),
    }
}

fn get_inner_array_selection(array: &Array) -> Result<Option<&SelectionVector>> {
    match array.array_data() {
        ArrayData::List(d) => Ok(d.array.selection_vector()),
        _ => Err(RayexecError::new("Expected list array data")),
    }
}

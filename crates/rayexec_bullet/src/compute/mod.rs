//! Compute kernels.
pub mod cast;
pub mod concat;
pub mod date;
pub mod filter;
pub mod interleave;
pub mod slice;
pub mod take;

pub mod util;

mod macros {
    /// Helper macro for collecting all arrays of a concrete type into a single
    /// vector.
    ///
    /// Accepts an `$arrays` expression which should be able to be turned into
    /// an iterator over array refs. `$variant` is the Array enum variant, and
    /// `$datatype` is the data type of the array (currently used for providing
    /// an informational error message).
    ///
    /// Returns a `Result<Vec<typed_array>>` where `typed_array` is the concrete
    /// array type.
    macro_rules! collect_arrays_of_type {
        ($arrays:expr, $variant:ident, $datatype:expr) => {
            $arrays
                .iter()
                .map(|arr| match arr {
                    Array::$variant(arr) => Ok(arr),
                    other => {
                        return Err(rayexec_error::RayexecError::new(format!(
                            "Array is not of the expected type. Expected {}, got {}",
                            $datatype,
                            other.datatype()
                        )))
                    }
                })
                .collect::<rayexec_error::Result<Vec<_>>>()
        };
    }

    pub(super) use collect_arrays_of_type;

    #[cfg(test)]
    mod tests {
        use crate::{
            array::{Array, Int32Array, UInt32Array},
            datatype::DataType,
        };

        use super::*;

        #[test]
        fn collect_arrays_of_type_simple() {
            let arr1 = Array::Int32(Int32Array::from_iter([1, 2, 3]));
            let arr2 = Array::Int32(Int32Array::from_iter([3, 4, 5]));

            let arrs = [&arr1, &arr2];

            let typed_arrs = collect_arrays_of_type!(arrs, Int32, DataType::Int32).unwrap();

            let expected = [
                Int32Array::from_iter([1, 2, 3]),
                Int32Array::from_iter([3, 4, 5]),
            ];
            let expected_refs: Vec<&Int32Array> = expected.iter().collect();

            assert_eq!(expected_refs, typed_arrs);
        }

        #[test]
        fn collect_arrays_of_type_error() {
            let arr1 = Array::Int32(Int32Array::from_iter([1, 2, 3]));
            let arr2 = Array::UInt32(UInt32Array::from_iter([3, 4, 5]));

            let arrs = [&arr1, &arr2];

            let _ = collect_arrays_of_type!(arrs, Int32, DataType::Int32).unwrap_err();
        }
    }
}

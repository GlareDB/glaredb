pub mod to_decimal;
pub mod to_primitive;
pub mod to_string;

use to_decimal::{FUNCTION_SET_TO_DECIMAL64, FUNCTION_SET_TO_DECIMAL128};
use to_primitive::{
    FUNCTION_SET_TO_FLOAT16,
    FUNCTION_SET_TO_FLOAT32,
    FUNCTION_SET_TO_FLOAT64,
    FUNCTION_SET_TO_INT8,
    FUNCTION_SET_TO_INT16,
    FUNCTION_SET_TO_INT32,
    FUNCTION_SET_TO_INT64,
    FUNCTION_SET_TO_INT128,
    FUNCTION_SET_TO_UINT8,
    FUNCTION_SET_TO_UINT16,
    FUNCTION_SET_TO_UINT32,
    FUNCTION_SET_TO_UINT64,
    FUNCTION_SET_TO_UINT128,
};
use to_string::FUNCTION_SET_TO_STRING;

use super::CastFunctionSet;

pub const BUILTIN_CAST_FUNCTION_SETS: &[CastFunctionSet] = &[
    // String
    FUNCTION_SET_TO_STRING,
    // Numeric primitives
    FUNCTION_SET_TO_INT8,
    FUNCTION_SET_TO_INT16,
    FUNCTION_SET_TO_INT32,
    FUNCTION_SET_TO_INT64,
    FUNCTION_SET_TO_INT128,
    FUNCTION_SET_TO_UINT8,
    FUNCTION_SET_TO_UINT16,
    FUNCTION_SET_TO_UINT32,
    FUNCTION_SET_TO_UINT64,
    FUNCTION_SET_TO_UINT128,
    FUNCTION_SET_TO_FLOAT16,
    FUNCTION_SET_TO_FLOAT32,
    FUNCTION_SET_TO_FLOAT64,
    // Decimals
    FUNCTION_SET_TO_DECIMAL64,
    FUNCTION_SET_TO_DECIMAL128,
];

mod null {
    use glaredb_error::Result;

    use crate::arrays::array::Array;
    use crate::arrays::array::validity::Validity;
    use crate::arrays::datatype::DataType;
    use crate::functions::cast::CastFunction;
    use crate::functions::cast::behavior::CastErrorState;
    use crate::util::iter::IntoExactSizeIterator;

    #[derive(Debug, Clone, Copy)]
    pub struct NullToAnything;

    impl CastFunction for NullToAnything {
        type State = ();

        fn bind(_src: &DataType, _target: &DataType) -> Result<Self::State> {
            Ok(())
        }

        fn cast(
            _state: &Self::State,
            _error_state: CastErrorState,
            _src: &Array,
            _sel: impl IntoExactSizeIterator<Item = usize>,
            out: &mut Array,
        ) -> Result<()> {
            // Can cast NULL to anything else. Just set the valid mask to all
            // invalid.
            out.put_validity(Validity::new_all_invalid(out.logical_len()))?;
            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::buffer::buffer_manager::NopBufferManager;
        use crate::functions::cast::behavior::CastFailBehavior;
        use crate::testutil::arrays::assert_arrays_eq;
        use crate::util::iter::TryFromExactSizeIterator;

        #[test]
        fn array_cast_null_to_f32() {
            let arr = Array::new(&NopBufferManager, DataType::Null, 3).unwrap();
            let mut out = Array::new(&NopBufferManager, DataType::Float32, 3).unwrap();

            let error_state = CastFailBehavior::Error.new_state();
            NullToAnything::cast(&(), error_state, &arr, 0..3, &mut out).unwrap();

            let expected = Array::try_from_iter([None as Option<f32>, None, None]).unwrap();
            assert_arrays_eq(&expected, &out);
        }
    }
}

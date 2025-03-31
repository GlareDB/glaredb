use std::fmt::Display;
use std::marker::PhantomData;

use glaredb_error::{DbError, Result};
use num_traits::{NumCast, ToPrimitive};

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    MutableScalarStorage,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI8,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI128,
    PhysicalU8,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU128,
    ScalarStorage,
};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::functions::cast::behavior::CastErrorState;
use crate::functions::cast::{
    CastFunction,
    CastFunctionSet,
    CastRule,
    RawCastFunction,
    TO_INT16_CAST_RULE,
    TO_INT32_CAST_RULE,
    TO_INT64_CAST_RULE,
    TO_UINT16_CAST_RULE,
    TO_UINT32_CAST_RULE,
    TO_UINT64_CAST_RULE,
};
use crate::util::iter::IntoExactSizeIterator;

pub const FUNCTION_SET_TO_INT8: CastFunctionSet = CastFunctionSet {
    name: "to_int8",
    #[rustfmt::skip]
    functions: &[
        // Int_ -> Int8
        RawCastFunction::new(DataTypeId::Int8, &PrimToPrim::<PhysicalI8, PhysicalI8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int16, &PrimToPrim::<PhysicalI16, PhysicalI8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int32, &PrimToPrim::<PhysicalI32, PhysicalI8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int64, &PrimToPrim::<PhysicalI64, PhysicalI8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Int128, &PrimToPrim::<PhysicalI128, PhysicalI8>::new(), CastRule::Explicit),
        // UInt_ -> Int8
        RawCastFunction::new(DataTypeId::UInt8, &PrimToPrim::<PhysicalU8, PhysicalI8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt16, &PrimToPrim::<PhysicalU16, PhysicalI8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt32, &PrimToPrim::<PhysicalU32, PhysicalI8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt64, &PrimToPrim::<PhysicalU64, PhysicalI8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::UInt128, &PrimToPrim::<PhysicalU128, PhysicalI8>::new(), CastRule::Explicit),
        // Float_ -> Int8
        RawCastFunction::new(DataTypeId::Float16, &PrimToPrim::<PhysicalF16, PhysicalI8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float32, &PrimToPrim::<PhysicalF32, PhysicalI8>::new(), CastRule::Explicit),
        RawCastFunction::new(DataTypeId::Float64, &PrimToPrim::<PhysicalF64, PhysicalI8>::new(), CastRule::Explicit),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct PrimToPrim<S1, S2> {
    _s1: PhantomData<S1>,
    _s2: PhantomData<S2>,
}

impl<S1, S2> PrimToPrim<S1, S2> {
    pub const fn new() -> Self {
        PrimToPrim {
            _s1: PhantomData,
            _s2: PhantomData,
        }
    }
}

impl<S1, S2> CastFunction for PrimToPrim<S1, S2>
where
    S1: ScalarStorage,
    S1::StorageType: ToPrimitive + Display + Sized + Copy,
    S2: MutableScalarStorage,
    S2::StorageType: NumCast + Copy,
{
    type State = ();

    fn bind(_src: &DataType, _target: &DataType) -> Result<Self::State> {
        Ok(())
    }

    fn cast(
        _: &Self::State,
        mut error_state: CastErrorState,
        src: &Array,
        sel: impl IntoExactSizeIterator<Item = usize>,
        out: &mut Array,
    ) -> Result<()> {
        UnaryExecutor::execute::<S1, S2, _>(src, sel, OutBuffer::from_array(out)?, |&v, buf| {
            match NumCast::from(v) {
                Some(v) => buf.put(&v),
                None => {
                    error_state.set_error(|| {
                        DbError::new(format!(
                            "Failed to cast value '{}' from {} to {}",
                            v,
                            S1::PHYSICAL_TYPE,
                            S2::PHYSICAL_TYPE,
                        ))
                    });
                    buf.put_null();
                }
            }
        })?;

        error_state.into_result()
    }
}

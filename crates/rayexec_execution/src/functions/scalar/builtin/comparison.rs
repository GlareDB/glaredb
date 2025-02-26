use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::Result;

use crate::arrays::array::physical_type::{
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
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUtf8,
    ScalarStorage,
};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::BinaryExecutor;
use crate::arrays::executor::OutBuffer;
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::functions::Signature;

// TODO: Decimal casts.
// TODO: Nested comparisons.
// TODO: Null coerced functions. Operators are there, just need to wrap.

pub const FUNCTION_SET_EQ: ScalarFunctionSet = ScalarFunctionSet {
    name: "=",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::General,
        description: "Check if two values are equal. Returns NULL if either argument is NULL.",
        arguments: &["a", "b"],
        example: Some(Example {
            example: "a = b",
            output: "true",
        }),
    }),
    functions: &generate_functions::<EqOperation>(),
};

pub const FUNCTION_SET_NEQ: ScalarFunctionSet = ScalarFunctionSet {
    name: "!=",
    aliases: &["<>"],
    doc: Some(&Documentation {
        category: Category::General,
        description: "Check if two values are not equal. Returns NULL if either argument is NULL.",
        arguments: &["a", "b"],
        example: Some(Example {
            example: "a != b",
            output: "false",
        }),
    }),
    functions: &generate_functions::<NotEqOperation>(),
};

pub const FUNCTION_SET_LT: ScalarFunctionSet = ScalarFunctionSet {
    name: "<",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::General,
        description: "Check if the left value is less than the right. Returns NULL if either argument is NULL.",
        arguments: &["a", "b"],
        example: Some(Example {
            example: "a < b",
            output: "false",
        }),
    }),
    functions: &generate_functions::<LtOperation>(),
};

pub const FUNCTION_SET_LT_EQ: ScalarFunctionSet = ScalarFunctionSet {
    name: "<=",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::General,
        description: "Check if the left value is less than or equal to the right. Returns NULL if either argument is NULL.",
        arguments: &["a", "b"],
        example: Some(Example {
            example: "a <= b",
            output: "false",
        }),
    }),
    functions: &generate_functions::<LtEqOperation>(),
};

pub const FUNCTION_SET_GT: ScalarFunctionSet = ScalarFunctionSet {
    name: ">",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::General,
        description: "Check if the left value is greater than the right. Returns NULL if either argument is NULL.",
        arguments: &["a", "b"],
        example: Some(Example {
            example: "a > b",
            output: "false",
        }),
    }),
    functions: &generate_functions::<GtOperation>(),
};

pub const FUNCTION_SET_GT_EQ: ScalarFunctionSet = ScalarFunctionSet {
    name: ">=",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::General,
        description: "Check if the left value is greater than or equal to the right. Returns NULL if either argument is NULL.",
        arguments: &["a", "b"],
        example: Some(Example {
            example: "a >= b",
            output: "false",
        }),
    }),
    functions: &generate_functions::<GtEqOperation>(),
};

/// Describes a comparison operation between a left and right element and takes
/// into account if either value is valid.
pub trait NullableComparisonOperation: Debug + Sync + Send + Copy + 'static {
    fn compare_with_valid<T>(left: T, right: T, left_valid: bool, right_valid: bool) -> bool
    where
        T: PartialEq + PartialOrd;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsDistinctFromOperator;

impl NullableComparisonOperation for IsDistinctFromOperator {
    fn compare_with_valid<T>(left: T, right: T, left_valid: bool, right_valid: bool) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        if !left_valid || !right_valid {
            return left_valid != right_valid;
        }
        NotEqOperation::compare(left, right)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNotDistinctFromOperation;

impl NullableComparisonOperation for IsNotDistinctFromOperation {
    fn compare_with_valid<T>(left: T, right: T, left_valid: bool, right_valid: bool) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        if !left_valid || !right_valid {
            return left_valid == right_valid;
        }
        EqOperation::compare(left, right)
    }
}

/// Wrapper around a normal comparison operation (==, !=, etc) that coerces
/// output that should be NULL to instead be false.
///
/// E.g. `5 == NULL` outputs false instead of NULL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NullCoercedComparison<C: ComparisonOperation> {
    _c: PhantomData<C>,
}

impl<C> NullCoercedComparison<C>
where
    C: ComparisonOperation,
{
    pub const fn new() -> Self {
        NullCoercedComparison { _c: PhantomData }
    }
}

impl<C> NullableComparisonOperation for NullCoercedComparison<C>
where
    C: ComparisonOperation,
{
    fn compare_with_valid<T>(left: T, right: T, left_valid: bool, right_valid: bool) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        if !left_valid || !right_valid {
            return false;
        }
        C::compare(left, right)
    }
}

/// Describes a comparison betweeen a left and right element.
pub trait ComparisonOperation: Debug + Sync + Send + Copy + 'static {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EqOperation;

impl ComparisonOperation for EqOperation {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        left == right
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotEqOperation;

impl ComparisonOperation for NotEqOperation {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        left != right
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LtOperation;

impl ComparisonOperation for LtOperation {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        left < right
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LtEqOperation;

impl ComparisonOperation for LtEqOperation {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        left <= right
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GtOperation;

impl ComparisonOperation for GtOperation {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        left > right
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GtEqOperation;

impl ComparisonOperation for GtEqOperation {
    fn compare<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        left >= right
    }
}

struct Sigs {
    bool: Signature,
    i8: Signature,
    i16: Signature,
    i32: Signature,
    i64: Signature,
    i128: Signature,
    u8: Signature,
    u16: Signature,
    u32: Signature,
    u64: Signature,
    u128: Signature,
    f16: Signature,
    f32: Signature,
    f64: Signature,
    date32: Signature,
    date64: Signature,
    timestamp: Signature,
    interval: Signature,
    decimal64: Signature,
    decimal128: Signature,
    binary: Signature,
    utf8: Signature,
}

const SIGS: Sigs = Sigs {
    bool: Signature::new(
        &[DataTypeId::Boolean, DataTypeId::Boolean],
        DataTypeId::Boolean,
    ),
    i8: Signature::new(&[DataTypeId::Int8, DataTypeId::Int8], DataTypeId::Boolean),
    i16: Signature::new(&[DataTypeId::Int16, DataTypeId::Int16], DataTypeId::Boolean),
    i32: Signature::new(&[DataTypeId::Int32, DataTypeId::Int32], DataTypeId::Boolean),
    i64: Signature::new(&[DataTypeId::Int64, DataTypeId::Int64], DataTypeId::Boolean),
    i128: Signature::new(
        &[DataTypeId::Int128, DataTypeId::Int128],
        DataTypeId::Boolean,
    ),

    u8: Signature::new(&[DataTypeId::UInt8, DataTypeId::UInt8], DataTypeId::Boolean),
    u16: Signature::new(
        &[DataTypeId::UInt16, DataTypeId::UInt16],
        DataTypeId::Boolean,
    ),
    u32: Signature::new(
        &[DataTypeId::UInt32, DataTypeId::UInt32],
        DataTypeId::Boolean,
    ),
    u64: Signature::new(
        &[DataTypeId::UInt64, DataTypeId::UInt64],
        DataTypeId::Boolean,
    ),
    u128: Signature::new(
        &[DataTypeId::UInt128, DataTypeId::UInt128],
        DataTypeId::Boolean,
    ),
    f16: Signature::new(
        &[DataTypeId::Float16, DataTypeId::Float16],
        DataTypeId::Boolean,
    ),
    f32: Signature::new(
        &[DataTypeId::Float32, DataTypeId::Float32],
        DataTypeId::Boolean,
    ),
    f64: Signature::new(
        &[DataTypeId::Float64, DataTypeId::Float64],
        DataTypeId::Boolean,
    ),
    date32: Signature::new(
        &[DataTypeId::Date32, DataTypeId::Date32],
        DataTypeId::Boolean,
    ),
    date64: Signature::new(
        &[DataTypeId::Date64, DataTypeId::Date64],
        DataTypeId::Boolean,
    ),
    timestamp: Signature::new(
        &[DataTypeId::Timestamp, DataTypeId::Timestamp],
        DataTypeId::Boolean,
    ),
    interval: Signature::new(
        &[DataTypeId::Interval, DataTypeId::Interval],
        DataTypeId::Boolean,
    ),
    decimal64: Signature::new(
        &[DataTypeId::Decimal64, DataTypeId::Decimal64],
        DataTypeId::Boolean,
    ),
    decimal128: Signature::new(
        &[DataTypeId::Decimal128, DataTypeId::Decimal128],
        DataTypeId::Boolean,
    ),
    binary: Signature::new(
        &[DataTypeId::Binary, DataTypeId::Binary],
        DataTypeId::Boolean,
    ),
    utf8: Signature::new(&[DataTypeId::Utf8, DataTypeId::Utf8], DataTypeId::Boolean),
};

const fn generate_functions<O>() -> [RawScalarFunction; 22]
where
    O: ComparisonOperation,
{
    [
        RawScalarFunction::new(
            &SIGS.bool,
            UnnestedComparisonImpl::<O, PhysicalBool>::new_static(),
        ),
        // Ints
        RawScalarFunction::new(
            &SIGS.i8,
            UnnestedComparisonImpl::<O, PhysicalI8>::new_static(),
        ),
        RawScalarFunction::new(
            &SIGS.i16,
            UnnestedComparisonImpl::<O, PhysicalI16>::new_static(),
        ),
        RawScalarFunction::new(
            &SIGS.i32,
            UnnestedComparisonImpl::<O, PhysicalI32>::new_static(),
        ),
        RawScalarFunction::new(
            &SIGS.i64,
            UnnestedComparisonImpl::<O, PhysicalI64>::new_static(),
        ),
        RawScalarFunction::new(
            &SIGS.i128,
            UnnestedComparisonImpl::<O, PhysicalI128>::new_static(),
        ),
        RawScalarFunction::new(
            &SIGS.u8,
            UnnestedComparisonImpl::<O, PhysicalU8>::new_static(),
        ),
        RawScalarFunction::new(
            &SIGS.u16,
            UnnestedComparisonImpl::<O, PhysicalU16>::new_static(),
        ),
        RawScalarFunction::new(
            &SIGS.u32,
            UnnestedComparisonImpl::<O, PhysicalU32>::new_static(),
        ),
        RawScalarFunction::new(
            &SIGS.u64,
            UnnestedComparisonImpl::<O, PhysicalU64>::new_static(),
        ),
        RawScalarFunction::new(
            &SIGS.u128,
            UnnestedComparisonImpl::<O, PhysicalU128>::new_static(),
        ),
        // Floats
        RawScalarFunction::new(
            &SIGS.f16,
            UnnestedComparisonImpl::<O, PhysicalF16>::new_static(),
        ),
        RawScalarFunction::new(
            &SIGS.f32,
            UnnestedComparisonImpl::<O, PhysicalF32>::new_static(),
        ),
        RawScalarFunction::new(
            &SIGS.f64,
            UnnestedComparisonImpl::<O, PhysicalF64>::new_static(),
        ),
        // Date/times
        RawScalarFunction::new(
            &SIGS.date32,
            UnnestedComparisonImpl::<O, PhysicalI32>::new_static(),
        ),
        RawScalarFunction::new(
            &SIGS.date64,
            UnnestedComparisonImpl::<O, PhysicalI64>::new_static(),
        ),
        // TODO: Probably scale
        RawScalarFunction::new(
            &SIGS.timestamp,
            UnnestedComparisonImpl::<O, PhysicalI64>::new_static(),
        ),
        RawScalarFunction::new(
            &SIGS.interval,
            UnnestedComparisonImpl::<O, PhysicalInterval>::new_static(),
        ),
        // Decimals
        // TODO: Definitely scale
        RawScalarFunction::new(
            &SIGS.decimal64,
            UnnestedComparisonImpl::<O, PhysicalI64>::new_static(),
        ),
        RawScalarFunction::new(
            &SIGS.decimal128,
            UnnestedComparisonImpl::<O, PhysicalI128>::new_static(),
        ),
        // Varlen
        RawScalarFunction::new(
            &SIGS.binary,
            UnnestedComparisonImpl::<O, PhysicalBinary>::new_static(),
        ),
        RawScalarFunction::new(
            &SIGS.utf8,
            UnnestedComparisonImpl::<O, PhysicalUtf8>::new_static(),
        ),
    ]
}

#[derive(Debug, Clone)]
struct UnnestedComparisonImpl<O: ComparisonOperation, S: ScalarStorage> {
    _op: PhantomData<O>,
    _s: PhantomData<S>,
}

impl<O, S> UnnestedComparisonImpl<O, S>
where
    O: ComparisonOperation,
    S: ScalarStorage,
{
    pub const fn new_static() -> &'static Self {
        &UnnestedComparisonImpl {
            _op: PhantomData,
            _s: PhantomData,
        }
    }
}

impl<O, S> ScalarFunction for UnnestedComparisonImpl<O, S>
where
    O: ComparisonOperation,
    S: ScalarStorage,
    S::StorageType: PartialEq + PartialOrd,
{
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Boolean,
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let left = &input.arrays()[0];
        let right = &input.arrays()[1];

        BinaryExecutor::execute::<S, S, PhysicalBool, _>(
            left,
            sel,
            right,
            sel,
            OutBuffer::from_array(output)?,
            |left, right, buf| buf.put(&O::compare(left, right)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_coerced_eq() {
        // 4 = 4 => true
        let out = NullCoercedComparison::<EqOperation>::compare_with_valid(4, 4, true, true);
        assert!(out);

        // 4 = 5 => false
        let out = NullCoercedComparison::<EqOperation>::compare_with_valid(4, 5, true, true);
        assert!(!out);

        // 4 = NULL => false
        let out = NullCoercedComparison::<EqOperation>::compare_with_valid(4, 4, true, false);
        assert!(!out);
    }
}

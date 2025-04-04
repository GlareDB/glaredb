use std::fmt::Debug;
use std::marker::PhantomData;

use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    Addressable,
    AddressableMut,
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
    PhysicalU8,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU128,
    PhysicalUtf8,
    ScalarStorage,
};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId, DecimalTypeMeta};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::BinaryExecutor;
use crate::arrays::scalar::decimal::{Decimal64Type, Decimal128Type, DecimalType};
use crate::expr::{self, Expression};
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::util::iter::IntoExactSizeIterator;

// TODO: Nested comparisons.

pub const FUNCTION_SET_EQ: ScalarFunctionSet = ScalarFunctionSet {
    name: "=",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::General,
        description: "Check if two values are equal. Returns NULL if either argument is NULL.",
        arguments: &["a", "b"],
        example: Some(Example {
            example: "a = b",
            output: "true",
        }),
    }],
    functions: &generate_comparison_functions::<EqOperation>(),
};

pub const FUNCTION_SET_NEQ: ScalarFunctionSet = ScalarFunctionSet {
    name: "!=",
    aliases: &["<>"],
    doc: &[&Documentation {
        category: Category::General,
        description: "Check if two values are not equal. Returns NULL if either argument is NULL.",
        arguments: &["a", "b"],
        example: Some(Example {
            example: "a != b",
            output: "false",
        }),
    }],
    functions: &generate_comparison_functions::<NotEqOperation>(),
};

pub const FUNCTION_SET_LT: ScalarFunctionSet = ScalarFunctionSet {
    name: "<",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::General,
        description: "Check if the left value is less than the right. Returns NULL if either argument is NULL.",
        arguments: &["a", "b"],
        example: Some(Example {
            example: "a < b",
            output: "false",
        }),
    }],
    functions: &generate_comparison_functions::<LtOperation>(),
};

pub const FUNCTION_SET_LT_EQ: ScalarFunctionSet = ScalarFunctionSet {
    name: "<=",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::General,
        description: "Check if the left value is less than or equal to the right. Returns NULL if either argument is NULL.",
        arguments: &["a", "b"],
        example: Some(Example {
            example: "a <= b",
            output: "false",
        }),
    }],
    functions: &generate_comparison_functions::<LtEqOperation>(),
};

pub const FUNCTION_SET_GT: ScalarFunctionSet = ScalarFunctionSet {
    name: ">",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::General,
        description: "Check if the left value is greater than the right. Returns NULL if either argument is NULL.",
        arguments: &["a", "b"],
        example: Some(Example {
            example: "a > b",
            output: "false",
        }),
    }],
    functions: &generate_comparison_functions::<GtOperation>(),
};

pub const FUNCTION_SET_GT_EQ: ScalarFunctionSet = ScalarFunctionSet {
    name: ">=",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::General,
        description: "Check if the left value is greater than or equal to the right. Returns NULL if either argument is NULL.",
        arguments: &["a", "b"],
        example: Some(Example {
            example: "a >= b",
            output: "false",
        }),
    }],
    functions: &generate_comparison_functions::<GtEqOperation>(),
};

pub const FUNCTION_SET_IS_DISTINCT_FROM: ScalarFunctionSet = ScalarFunctionSet {
    name: "is_distinct_from",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::General,
        description: "Check if two values are not equal, treating NULLs as normal data values.",
        arguments: &["a", "b"],
        example: Some(Example {
            example: "'cat' IS DISTINCT FROM NULL",
            output: "true",
        }),
    }],
    functions: &generate_distinct_functions::<IsDistinctFromOperation>(),
};

pub const FUNCTION_SET_IS_NOT_DISTINCT_FROM: ScalarFunctionSet = ScalarFunctionSet {
    name: "is_not_distinct_from",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::General,
        description: "Check if two values are equal, treating NULLs as normal data values.",
        arguments: &["a", "b"],
        example: Some(Example {
            example: "'cat' IS NOT DISTINCT FROM NULL",
            output: "false",
        }),
    }],
    functions: &generate_distinct_functions::<IsNotDistinctFromOperation>(),
};

/// Describes a comparison operation between a left and right element and takes
/// into account if either value is valid.
///
/// For IS DISTINCT FROM and IS NOT DISTINCT FROM.
pub trait DistinctComparisonOperation: Debug + Sync + Send + Copy + 'static {
    /// Compare two values using the normal underlying comparison operator.
    fn compare_non_nullable<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd;

    /// Compare two values that may or may not be null.
    // TODO: These should probably accept option.
    fn compare_nullable<T>(left: Option<T>, right: Option<T>) -> bool
    where
        T: PartialEq + PartialOrd;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsDistinctFromOperation;

impl DistinctComparisonOperation for IsDistinctFromOperation {
    fn compare_non_nullable<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        NotEqOperation::compare(left, right)
    }

    fn compare_nullable<T>(left: Option<T>, right: Option<T>) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        match (left, right) {
            (Some(left), Some(right)) => NotEqOperation::compare(left, right),
            (Some(_), None) | (None, Some(_)) => true,
            (None, None) => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNotDistinctFromOperation;

impl DistinctComparisonOperation for IsNotDistinctFromOperation {
    fn compare_non_nullable<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        EqOperation::compare(left, right)
    }

    fn compare_nullable<T>(left: Option<T>, right: Option<T>) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        match (left, right) {
            (Some(left), Some(right)) => EqOperation::compare(left, right),
            (Some(_), None) | (None, Some(_)) => false,
            (None, None) => true,
        }
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

impl<C> DistinctComparisonOperation for NullCoercedComparison<C>
where
    C: ComparisonOperation,
{
    fn compare_non_nullable<T>(left: T, right: T) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        C::compare(left, right)
    }

    fn compare_nullable<T>(left: Option<T>, right: Option<T>) -> bool
    where
        T: PartialEq + PartialOrd,
    {
        match (left, right) {
            (Some(left), Some(right)) => C::compare(left, right),
            _ => false,
        }
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

#[rustfmt::skip]
const SIGS: Sigs = Sigs {
    bool: Signature::new(&[DataTypeId::Boolean, DataTypeId::Boolean], DataTypeId::Boolean),
    i8: Signature::new(&[DataTypeId::Int8, DataTypeId::Int8], DataTypeId::Boolean),
    i16: Signature::new(&[DataTypeId::Int16, DataTypeId::Int16], DataTypeId::Boolean),
    i32: Signature::new(&[DataTypeId::Int32, DataTypeId::Int32], DataTypeId::Boolean),
    i64: Signature::new(&[DataTypeId::Int64, DataTypeId::Int64], DataTypeId::Boolean),
    i128: Signature::new(&[DataTypeId::Int128, DataTypeId::Int128], DataTypeId::Boolean),

    u8: Signature::new(&[DataTypeId::UInt8, DataTypeId::UInt8], DataTypeId::Boolean),
    u16: Signature::new(&[DataTypeId::UInt16, DataTypeId::UInt16], DataTypeId::Boolean),
    u32: Signature::new(&[DataTypeId::UInt32, DataTypeId::UInt32], DataTypeId::Boolean),
    u64: Signature::new(&[DataTypeId::UInt64, DataTypeId::UInt64], DataTypeId::Boolean),
    u128: Signature::new(&[DataTypeId::UInt128, DataTypeId::UInt128], DataTypeId::Boolean),

    f16: Signature::new(&[DataTypeId::Float16, DataTypeId::Float16], DataTypeId::Boolean),
    f32: Signature::new(&[DataTypeId::Float32, DataTypeId::Float32], DataTypeId::Boolean),
    f64: Signature::new(&[DataTypeId::Float64, DataTypeId::Float64], DataTypeId::Boolean),

    date32: Signature::new(&[DataTypeId::Date32, DataTypeId::Date32], DataTypeId::Boolean),
    date64: Signature::new(&[DataTypeId::Date64, DataTypeId::Date64], DataTypeId::Boolean),
    timestamp: Signature::new(&[DataTypeId::Timestamp, DataTypeId::Timestamp], DataTypeId::Boolean),
    interval: Signature::new(&[DataTypeId::Interval, DataTypeId::Interval], DataTypeId::Boolean),
    decimal64: Signature::new(&[DataTypeId::Decimal64, DataTypeId::Decimal64], DataTypeId::Boolean),
    decimal128: Signature::new(&[DataTypeId::Decimal128, DataTypeId::Decimal128], DataTypeId::Boolean),
    binary: Signature::new(&[DataTypeId::Binary, DataTypeId::Binary], DataTypeId::Boolean),
    utf8: Signature::new(&[DataTypeId::Utf8, DataTypeId::Utf8], DataTypeId::Boolean),
};

/// Generate the "normal" comparison functions.
#[rustfmt::skip]
const fn generate_comparison_functions<O>() -> [RawScalarFunction; 22]
where
    O: ComparisonOperation,
{
    [
        RawScalarFunction::new(&SIGS.bool, FlatComparison::<O, PhysicalBool>::new_static()),
        // Ints
        RawScalarFunction::new(&SIGS.i8, FlatComparison::<O, PhysicalI8>::new_static()),
        RawScalarFunction::new(&SIGS.i16, FlatComparison::<O, PhysicalI16>::new_static()),
        RawScalarFunction::new(&SIGS.i32, FlatComparison::<O, PhysicalI32>::new_static()),
        RawScalarFunction::new(&SIGS.i64, FlatComparison::<O, PhysicalI64>::new_static()),
        RawScalarFunction::new(&SIGS.i128, FlatComparison::<O, PhysicalI128>::new_static()),
        RawScalarFunction::new(&SIGS.u8, FlatComparison::<O, PhysicalU8>::new_static()),
        RawScalarFunction::new(&SIGS.u16, FlatComparison::<O, PhysicalU16>::new_static()),
        RawScalarFunction::new(&SIGS.u32, FlatComparison::<O, PhysicalU32>::new_static()),
        RawScalarFunction::new(&SIGS.u64, FlatComparison::<O, PhysicalU64>::new_static()),
        RawScalarFunction::new(&SIGS.u128, FlatComparison::<O, PhysicalU128>::new_static()),
        // Floats
        RawScalarFunction::new(&SIGS.f16, FlatComparison::<O, PhysicalF16>::new_static()),
        RawScalarFunction::new(&SIGS.f32, FlatComparison::<O, PhysicalF32>::new_static()),
        RawScalarFunction::new(&SIGS.f64, FlatComparison::<O, PhysicalF64>::new_static()),
        // Date/times
        RawScalarFunction::new(&SIGS.date32, FlatComparison::<O, PhysicalI32>::new_static()),
        RawScalarFunction::new(&SIGS.date64, FlatComparison::<O, PhysicalI64>::new_static()),
        // TODO: Probably scale
        RawScalarFunction::new(&SIGS.timestamp, FlatComparison::<O, PhysicalI64>::new_static()),
        RawScalarFunction::new(&SIGS.interval, FlatComparison::<O, PhysicalInterval>::new_static()),
        // Decimals
        RawScalarFunction::new(&SIGS.decimal64, DecimalComparison::<O, Decimal64Type>::new_static()),
        RawScalarFunction::new(&SIGS.decimal128, DecimalComparison::<O, Decimal128Type>::new_static()),
        // Varlen
        RawScalarFunction::new(&SIGS.binary, FlatComparison::<O, PhysicalBinary>::new_static()),
        RawScalarFunction::new(&SIGS.utf8, FlatComparison::<O, PhysicalUtf8>::new_static()),
    ]
}

// Generate the distinct functions (IS DISTINCT FROM, IS NOT DISTINCT FROM).
#[rustfmt::skip]
const fn generate_distinct_functions<O>() -> [RawScalarFunction; 22]
where
    O: DistinctComparisonOperation,
{
    [
        RawScalarFunction::new(&SIGS.bool, FlatDistinctComparison::<O, PhysicalBool>::new_static()),
        // Ints
        RawScalarFunction::new(&SIGS.i8, FlatDistinctComparison::<O, PhysicalI8>::new_static()),
        RawScalarFunction::new(&SIGS.i16, FlatDistinctComparison::<O, PhysicalI16>::new_static()),
        RawScalarFunction::new(&SIGS.i32, FlatDistinctComparison::<O, PhysicalI32>::new_static()),
        RawScalarFunction::new(&SIGS.i64, FlatDistinctComparison::<O, PhysicalI64>::new_static()),
        RawScalarFunction::new(&SIGS.i128, FlatDistinctComparison::<O, PhysicalI128>::new_static()),
        RawScalarFunction::new(&SIGS.u8, FlatDistinctComparison::<O, PhysicalU8>::new_static()),
        RawScalarFunction::new(&SIGS.u16, FlatDistinctComparison::<O, PhysicalU16>::new_static()),
        RawScalarFunction::new(&SIGS.u32, FlatDistinctComparison::<O, PhysicalU32>::new_static()),
        RawScalarFunction::new(&SIGS.u64, FlatDistinctComparison::<O, PhysicalU64>::new_static()),
        RawScalarFunction::new(&SIGS.u128, FlatDistinctComparison::<O, PhysicalU128>::new_static()),
        // Floats
        RawScalarFunction::new(&SIGS.f16, FlatDistinctComparison::<O, PhysicalF16>::new_static()),
        RawScalarFunction::new(&SIGS.f32, FlatDistinctComparison::<O, PhysicalF32>::new_static()),
        RawScalarFunction::new(&SIGS.f64, FlatDistinctComparison::<O, PhysicalF64>::new_static()),
        // Date/times
        RawScalarFunction::new(&SIGS.date32, FlatDistinctComparison::<O, PhysicalI32>::new_static()),
        RawScalarFunction::new(&SIGS.date64, FlatDistinctComparison::<O, PhysicalI64>::new_static()),
        // TODO: Probably scale
        RawScalarFunction::new(&SIGS.timestamp, FlatDistinctComparison::<O, PhysicalI64>::new_static()),
        RawScalarFunction::new(&SIGS.interval, FlatDistinctComparison::<O, PhysicalInterval>::new_static()),
        // Decimals
        RawScalarFunction::new(&SIGS.decimal64, DecimalDistinctComparison::<O, Decimal64Type>::new_static()),
        RawScalarFunction::new(&SIGS.decimal128, DecimalDistinctComparison::<O, Decimal128Type>::new_static()),
        // Varlen
        RawScalarFunction::new(&SIGS.binary, FlatDistinctComparison::<O, PhysicalBinary>::new_static()),
        RawScalarFunction::new(&SIGS.utf8, FlatDistinctComparison::<O, PhysicalUtf8>::new_static()),
    ]
}

#[derive(Debug, Clone, Copy)]
pub struct FlatComparison<O: ComparisonOperation, S: ScalarStorage> {
    _op: PhantomData<O>,
    _s: PhantomData<S>,
}

impl<O, S> FlatComparison<O, S>
where
    O: ComparisonOperation,
    S: ScalarStorage,
{
    pub const fn new_static() -> &'static Self {
        &FlatComparison {
            _op: PhantomData,
            _s: PhantomData,
        }
    }
}

impl<O, S> ScalarFunction for FlatComparison<O, S>
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

/// Shared logic for binding decimal inputs for decimal comparisons.
///
/// This will ensure both inputs have the correct precision and scale for
/// comparison.
fn decimal_bind<D>(mut inputs: Vec<Expression>) -> Result<BindState<()>>
where
    D: DecimalType,
{
    let right = inputs.pop().unwrap();
    let left = inputs.pop().unwrap();

    let l_meta = D::decimal_meta(&left.datatype()?)?;
    let r_meta = D::decimal_meta(&right.datatype()?)?;

    if l_meta != r_meta {
        // Need to apply casts to get the decimals to the same prec/scale.
        let max_scale = i8::max(l_meta.scale, r_meta.scale);

        // TODO: Does this properly handle negative scale?
        let l_int_digits = (l_meta.precision as i8) - l_meta.scale;
        let r_int_digits = (r_meta.precision as i8) - r_meta.scale;

        let max_int_digits = i8::max(l_int_digits, r_int_digits);

        let mut new_prec = (max_int_digits + max_scale) as u8;
        if new_prec > D::MAX_PRECISION {
            // Truncate to max precision this decimal type can handle.
            // Casting may fail at runtime.
            new_prec = D::MAX_PRECISION;
        }

        let new_meta = DecimalTypeMeta {
            precision: new_prec,
            scale: max_scale,
        };
        let new_datatype = D::datatype_from_decimal_meta(new_meta);

        let left = if l_meta != new_meta {
            // Cast left.
            expr::cast(left, new_datatype.clone())?.into()
        } else {
            // Left is unchanged.
            left
        };

        let right = if r_meta != new_meta {
            // Cast right.
            expr::cast(right, new_datatype)?.into()
        } else {
            // Right is unchanged.
            right
        };

        Ok(BindState {
            state: (),
            return_type: DataType::Boolean,
            inputs: vec![left, right],
        })
    } else {
        // Left/right have the same precision and scale, no need to cast.
        Ok(BindState {
            state: (),
            return_type: DataType::Boolean,
            inputs: vec![left, right],
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DecimalComparison<O: ComparisonOperation, D: DecimalType> {
    _op: PhantomData<O>,
    _d: PhantomData<D>,
}

impl<O, D> DecimalComparison<O, D>
where
    O: ComparisonOperation,
    D: DecimalType,
{
    pub const fn new_static() -> &'static Self {
        &DecimalComparison {
            _op: PhantomData,
            _d: PhantomData,
        }
    }
}

impl<O, D> ScalarFunction for DecimalComparison<O, D>
where
    O: ComparisonOperation,
    D: DecimalType,
{
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        decimal_bind::<D>(inputs)
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let left = &input.arrays()[0];
        let right = &input.arrays()[1];

        BinaryExecutor::execute::<D::Storage, D::Storage, PhysicalBool, _>(
            left,
            sel,
            right,
            sel,
            OutBuffer::from_array(output)?,
            |left, right, buf| buf.put(&O::compare(left, right)),
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FlatDistinctComparison<O: DistinctComparisonOperation, S: ScalarStorage> {
    _op: PhantomData<O>,
    _s: PhantomData<S>,
}

impl<O, S> FlatDistinctComparison<O, S>
where
    O: DistinctComparisonOperation,
    S: ScalarStorage,
{
    pub const fn new_static() -> &'static Self {
        &FlatDistinctComparison {
            _op: PhantomData,
            _s: PhantomData,
        }
    }
}

impl<O, S> ScalarFunction for FlatDistinctComparison<O, S>
where
    O: DistinctComparisonOperation,
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

        binary_distinct_execute::<O, S>(left, right, sel, OutBuffer::from_array(output)?)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DecimalDistinctComparison<O: DistinctComparisonOperation, D: DecimalType> {
    _op: PhantomData<O>,
    _d: PhantomData<D>,
}

impl<O, D> DecimalDistinctComparison<O, D>
where
    O: DistinctComparisonOperation,
    D: DecimalType,
{
    pub const fn new_static() -> &'static Self {
        &DecimalDistinctComparison {
            _op: PhantomData,
            _d: PhantomData,
        }
    }
}

impl<O, D> ScalarFunction for DecimalDistinctComparison<O, D>
where
    O: DistinctComparisonOperation,
    D: DecimalType,
{
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        decimal_bind::<D>(inputs)
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let left = &input.arrays()[0];
        let right = &input.arrays()[1];

        binary_distinct_execute::<O, D::Storage>(left, right, sel, OutBuffer::from_array(output)?)
    }
}

/// Executes a distinct comparison operation on the inputs.
fn binary_distinct_execute<O, S>(
    array1: &Array,
    array2: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    out: OutBuffer,
) -> Result<()>
where
    O: DistinctComparisonOperation,
    S: ScalarStorage,
    S::StorageType: PartialEq + PartialOrd,
{
    let array1 = array1.flatten()?;
    let array2 = array2.flatten()?;

    let input1 = S::get_addressable(array1.array_buffer)?;
    let input2 = S::get_addressable(array2.array_buffer)?;

    let mut output = PhysicalBool::get_addressable_mut(out.buffer)?;

    let validity1 = &array1.validity;
    let validity2 = &array2.validity;

    if validity1.all_valid() && validity2.all_valid() {
        for (output_idx, sel_idx) in sel.into_exact_size_iter().enumerate() {
            let sel1 = array1.selection.get(sel_idx).unwrap();
            let sel2 = array2.selection.get(sel_idx).unwrap();

            let val1 = input1.get(sel1).unwrap();
            let val2 = input2.get(sel2).unwrap();

            let val = O::compare_non_nullable(val1, val2);
            output.put(output_idx, &val);
        }
    } else {
        for (output_idx, sel_idx) in sel.into_exact_size_iter().enumerate() {
            let val1 = if validity1.is_valid(sel_idx) {
                let sel1 = array1.selection.get(sel_idx).unwrap();
                Some(input1.get(sel1).unwrap())
            } else {
                None
            };

            let val2 = if validity2.is_valid(sel_idx) {
                let sel2 = array2.selection.get(sel_idx).unwrap();
                Some(input2.get(sel2).unwrap())
            } else {
                None
            };

            let val = O::compare_nullable(val1, val2);
            output.put(output_idx, &val);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_manager::NopBufferManager;
    use crate::generate_array;
    use crate::testutil::arrays::assert_arrays_eq;

    #[test]
    fn null_coerced_eq() {
        // 4 = 4 => true
        let out = NullCoercedComparison::<EqOperation>::compare_nullable(Some(4), Some(4));
        assert!(out);

        // 4 = 5 => false
        let out = NullCoercedComparison::<EqOperation>::compare_nullable(Some(4), Some(5));
        assert!(!out);

        // 4 = NULL => false
        let out = NullCoercedComparison::<EqOperation>::compare_nullable(Some(4), None);
        assert!(!out);
    }

    #[test]
    fn distinct_execute_is_distinct_from_non_null() {
        let arr1 = generate_array!([1, 2, 4]);
        let arr2 = generate_array!([4, 2, 1]);

        let mut output = Array::new(&NopBufferManager, DataType::Boolean, 3).unwrap();

        binary_distinct_execute::<IsDistinctFromOperation, PhysicalI32>(
            &arr1,
            &arr2,
            0..3,
            OutBuffer::from_array(&mut output).unwrap(),
        )
        .unwrap();

        let expected = generate_array!([true, false, true]);
        assert_arrays_eq(&expected, &output);
    }

    #[test]
    fn distinct_execute_is_distinct_from_null() {
        let arr1 = generate_array!([Some(1), Some(2), None, None]);
        let arr2 = generate_array!([Some(4), Some(2), None, Some(3)]);

        let mut output = Array::new(&NopBufferManager, DataType::Boolean, 4).unwrap();

        binary_distinct_execute::<IsDistinctFromOperation, PhysicalI32>(
            &arr1,
            &arr2,
            0..4,
            OutBuffer::from_array(&mut output).unwrap(),
        )
        .unwrap();

        let expected = generate_array!([true, false, false, true]);
        assert_arrays_eq(&expected, &output);
    }

    #[test]
    fn distinct_execute_is_not_distinct_from_non_null() {
        let arr1 = generate_array!([1, 2, 4]);
        let arr2 = generate_array!([4, 2, 1]);

        let mut output = Array::new(&NopBufferManager, DataType::Boolean, 3).unwrap();

        binary_distinct_execute::<IsNotDistinctFromOperation, PhysicalI32>(
            &arr1,
            &arr2,
            0..3,
            OutBuffer::from_array(&mut output).unwrap(),
        )
        .unwrap();

        let expected = generate_array!([false, true, false]);
        assert_arrays_eq(&expected, &output);
    }

    #[test]
    fn distinct_execute_is_not_distinct_from_null() {
        let arr1 = generate_array!([Some(1), Some(2), None, None]);
        let arr2 = generate_array!([Some(4), Some(2), None, Some(3)]);

        let mut output = Array::new(&NopBufferManager, DataType::Boolean, 4).unwrap();

        binary_distinct_execute::<IsNotDistinctFromOperation, PhysicalI32>(
            &arr1,
            &arr2,
            0..4,
            OutBuffer::from_array(&mut output).unwrap(),
        )
        .unwrap();

        let expected = generate_array!([false, true, true, false]);
        assert_arrays_eq(&expected, &output);
    }
}

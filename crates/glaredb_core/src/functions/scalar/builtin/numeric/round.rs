use glaredb_error::{Result, ResultExt};
use num_traits::Float;

use super::{UnaryInputNumericOperation, UnaryInputNumericScalar};
use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    MutableScalarStorage,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId, DecimalTypeMeta};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::scalar::decimal::{Decimal64Type, Decimal128Type, DecimalType};
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::bind_state::BindState;
use crate::functions::cast::CastFunction;
use crate::functions::cast::behavior::CastFailBehavior;
use crate::functions::cast::builtin::to_decimal::{DecimalToDecimal, DecimalToDecimalState};
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{RawScalarFunction, ScalarFunction};
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::util::iter::IntoExactSizeIterator;

pub const FUNCTION_SET_ROUND: ScalarFunctionSet = ScalarFunctionSet {
    name: "round",
    aliases: &[],
    doc: &[
        &Documentation {
            category: Category::Numeric,
            description: "Round a number to the nearest whole value.",
            arguments: &["number"],
            example: Some(Example {
                example: "round(3.14159)",
                output: "3",
            }),
        },
        &Documentation {
            category: Category::Numeric,
            description: "Round a number to a given scale.",
            arguments: &["number", "scale"],
            example: Some(Example {
                example: "round(3.14159, 2)",
                output: "3.14",
            }),
        },
    ],
    functions: &[
        // Floats
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float16], DataTypeId::Float16),
            &UnaryInputNumericScalar::<PhysicalF16, RoundFloat>::new(DataType::FLOAT16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float32], DataTypeId::Float32),
            &UnaryInputNumericScalar::<PhysicalF32, RoundFloat>::new(DataType::FLOAT32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Float64], DataTypeId::Float64),
            &UnaryInputNumericScalar::<PhysicalF64, RoundFloat>::new(DataType::FLOAT64),
        ),
        // Decimals
        // round(3.1415)
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Decimal64], DataTypeId::Decimal64),
            &RoundDecimal::<Decimal64Type>::new(),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Decimal128], DataTypeId::Decimal128),
            &RoundDecimal::<Decimal128Type>::new(),
        ),
        // round(3.1415, 2)
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Decimal64, DataTypeId::Int64],
                DataTypeId::Decimal64,
            ),
            &RoundDecimal::<Decimal64Type>::new(),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Decimal128, DataTypeId::Int64],
                DataTypeId::Decimal128,
            ),
            &RoundDecimal::<Decimal128Type>::new(),
        ),
    ],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoundFloat;

impl UnaryInputNumericOperation for RoundFloat {
    fn execute_float<S>(
        input: &Array,
        selection: impl IntoExactSizeIterator<Item = usize>,
        output: &mut Array,
    ) -> Result<()>
    where
        S: MutableScalarStorage,
        S::StorageType: Float,
    {
        UnaryExecutor::execute::<S, S, _>(
            input,
            selection,
            OutBuffer::from_array(output)?,
            |&v, buf| buf.put(&v.round()),
        )
    }
}

/// Rounds decimal inputs by casting them.
#[derive(Debug, Clone, Copy)]
pub struct RoundDecimal<D: DecimalType> {
    cast_fn: DecimalToDecimal<D, D>,
}

impl<D> RoundDecimal<D>
where
    D: DecimalType,
{
    pub const fn new() -> Self {
        RoundDecimal {
            cast_fn: DecimalToDecimal::new(),
        }
    }
}

impl<D> ScalarFunction for RoundDecimal<D>
where
    D: DecimalType,
{
    type State = DecimalToDecimalState<D::Primitive>;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        // TODO: This currently just does the easy thing and scales by a
        // constant.
        //
        // If we allowed the scale to be variable, then we wouldn't be able to
        // determine output decimal size.
        //
        // This _also_ limits the max scale to the currently decimal scale.
        // Postgres allows the scale to increase, and will return numerics with
        // the increased scale... but that would require we adjust precision,
        // and we can deal with that later.
        //
        // For example:
        // - DECIMAL(18, 4) rounded to scale of 1 => DECIMAL(18, 1)
        // - DECIMAL(18, 4) rounded to scale of 6 => DECIMAL(18, 4)

        let new_scale = match inputs.get(1) {
            Some(scale_expr) => {
                let scale = ConstFold::rewrite(scale_expr.clone())?
                    .try_into_scalar()?
                    .try_as_i64()?;
                i8::try_from(scale).context("Decimal scale too large")?
            }
            None => 0, // `round(3.14)` == `round(3.14, 0)`
        };

        let input_datatype = inputs[0].datatype()?;
        let input_meta = D::decimal_meta(&input_datatype)?;
        // TODO: Negative scale?
        let new_scale = i8::min(new_scale, input_meta.scale);
        let new_meta = DecimalTypeMeta::new(input_meta.precision, new_scale);

        let new_datatype = D::datatype_from_decimal_meta(new_meta);

        let state = self.cast_fn.bind(&input_datatype, &new_datatype)?;

        Ok(BindState {
            state,
            return_type: new_datatype,
            inputs,
        })
    }

    fn execute(state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let error_state = CastFailBehavior::Error.new_state();
        DecimalToDecimal::<D, D>::cast(
            state,
            error_state,
            &input.arrays[0],
            input.selection(),
            output,
        )
    }
}

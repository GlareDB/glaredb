use glaredb_error::Result;

use crate::arrays::array::physical_type::PhysicalI64;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId, TimeUnit, TimestampTypeMeta};
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::executor::OutBuffer;
use crate::expr::Expression;
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::functions::Signature;
use crate::util::iter::IntoExactSizeIterator;

pub const FUNCTION_SET_EPOCH: ScalarFunctionSet = ScalarFunctionSet {
    name: "epoch",
    aliases: &["epoch_s"],
    doc: None,
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Int64], DataTypeId::Timestamp),
        &EpochImpl::<1_000_000>,
    )],
};

pub const FUNCTION_SET_EPOCH_MS: ScalarFunctionSet = ScalarFunctionSet {
    name: "epoch_ms",
    aliases: &[],
    doc: None,
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Int64], DataTypeId::Timestamp),
        &EpochImpl::<1000>,
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochImpl<const S: i64>;

impl<const S: i64> ScalarFunction for EpochImpl<S> {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Timestamp(TimestampTypeMeta::new(TimeUnit::Microsecond)),
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];
        to_timestamp::<S>(input, sel, output)
    }
}

fn to_timestamp<const S: i64>(
    input: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    out: &mut Array,
) -> Result<()> {
    UnaryExecutor::execute::<PhysicalI64, PhysicalI64, _>(
        input,
        sel,
        OutBuffer::from_array(out)?,
        |&v, buf| buf.put(&(v * S)),
    )
}

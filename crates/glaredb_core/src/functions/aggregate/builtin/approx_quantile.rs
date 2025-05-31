use glaredb_error::{DbError, Result};

use crate::arrays::array::physical_type::{AddressableMut, PhysicalF64};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::PutBuffer;
use crate::arrays::executor::aggregate::AggregateState;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::aggregate::RawAggregateFunction;
use crate::functions::aggregate::simple::{BinaryAggregate, SimpleBinaryAggregate};
use crate::functions::bind_state::BindState;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::AggregateFunctionSet;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::statistics::tdigest::TDigest;

pub const FUNCTION_SET_APPROX_QUANTILE: AggregateFunctionSet = AggregateFunctionSet {
    name: "approx_quantile",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::GENERAL_PURPOSE_AGGREGATE,
        description: "Compute the approximate quantile using a T-Digest sketch.",
        arguments: &["input", "quantile"],
        example: None,
    }],
    functions: &[RawAggregateFunction::new(
        &Signature::new(
            &[DataTypeId::Float64, DataTypeId::Float64],
            DataTypeId::Float64,
        ),
        &SimpleBinaryAggregate::new(&ApproxQuantile),
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct ApproxQuantile;

#[derive(Debug)]
pub struct ApproxQuantileBindState {
    quantile: f64,
}

impl BinaryAggregate for ApproxQuantile {
    type Input1 = PhysicalF64;
    type Input2 = PhysicalF64;
    type Output = PhysicalF64;

    type BindState = ApproxQuantileBindState;
    type GroupState = ApproxQuantileState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        let quantile = ConstFold::rewrite(inputs[1].clone())?
            .try_into_scalar()?
            .try_as_f64()?;

        if quantile < 0.0 || quantile > 1.0 {
            return Err(DbError::new(
                "Quantile argument must be in the range [0, 1]",
            ));
        }

        Ok(BindState {
            state: ApproxQuantileBindState { quantile },
            return_type: DataType::float64(),
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        ApproxQuantileState {
            digest: TDigest::new(100),
        }
    }
}

#[derive(Debug)]
pub struct ApproxQuantileState {
    digest: TDigest,
}

impl AggregateState<(&f64, &f64), f64> for ApproxQuantileState {
    type BindState = ApproxQuantileBindState;

    fn merge(&mut self, _state: &Self::BindState, other: &mut Self) -> Result<()> {
        self.digest.merge(&other.digest);
        Ok(())
    }

    fn update(
        &mut self,
        _state: &Self::BindState,
        (&input, _quantile): (&f64, &f64),
    ) -> Result<()> {
        self.digest.add(input);
        Ok(())
    }

    fn finalize<M>(&mut self, state: &Self::BindState, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = f64>,
    {
        let v = self.digest.quantile(state.quantile);
        output.put(&v);
        Ok(())
    }
}

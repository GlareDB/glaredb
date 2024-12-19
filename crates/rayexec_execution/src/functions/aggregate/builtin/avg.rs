use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::AddAssign;

use num_traits::AsPrimitive;
use rayexec_bullet::array::Array;
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::aggregate::AggregateState;
use rayexec_bullet::executor::builder::{ArrayBuilder, ArrayDataBuffer, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::{PhysicalF64, PhysicalI64};
use rayexec_bullet::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType};
use rayexec_error::Result;
use serde::{Deserialize, Serialize};

use crate::expr::Expression;
use crate::functions::aggregate::states::{
    new_unary_aggregate_states,
    primitive_finalize,
    AggregateGroupStates,
};
use crate::functions::aggregate::{
    AggregateFunction,
    AggregateFunctionImpl,
    PlannedAggregateFunction,
};
use crate::functions::documentation::{Category, Documentation};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Avg;

impl FunctionInfo for Avg {
    fn name(&self) -> &'static str {
        "avg"
    }

    fn signatures(&self) -> &[Signature] {
        const DOC: &Documentation = &Documentation {
            category: Category::Aggregate,
            description: "Return the average value from the inputs.",
            arguments: &["input"],
            example: None,
        };

        &[
            Signature {
                positional_args: &[DataTypeId::Float64],
                variadic_arg: None,
                return_type: DataTypeId::Float64,
                doc: Some(DOC),
            },
            Signature {
                positional_args: &[DataTypeId::Int64],
                variadic_arg: None,
                return_type: DataTypeId::Float64, // TODO: Should be decimal // TODO: Should it though?
                doc: Some(DOC),
            },
            Signature {
                positional_args: &[DataTypeId::Decimal64],
                variadic_arg: None,
                return_type: DataTypeId::Float64,
                doc: Some(DOC),
            },
            Signature {
                positional_args: &[DataTypeId::Decimal128],
                variadic_arg: None,
                return_type: DataTypeId::Float64,
                doc: Some(DOC),
            },
        ]
    }
}

impl AggregateFunction for Avg {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        let (function_impl, return_type): (Box<dyn AggregateFunctionImpl>, _) =
            match inputs[0].datatype(table_list)? {
                DataType::Int64 => (Box::new(AvgInt64Impl), DataType::Float64),
                DataType::Float64 => (Box::new(AvgFloat64Impl), DataType::Float64),
                dt @ DataType::Decimal64(_) => {
                    // Datatype only used in order to convert decimal to float
                    // at the end. This always returns Float64.
                    (
                        Box::new(AvgDecimalImpl::<Decimal64Type>::new(dt)),
                        DataType::Float64,
                    )
                }
                dt @ DataType::Decimal128(_) => {
                    // See above
                    (
                        Box::new(AvgDecimalImpl::<Decimal128Type>::new(dt)),
                        DataType::Float64,
                    )
                }

                other => return Err(invalid_input_types_error(self, &[other])),
            };

        Ok(PlannedAggregateFunction {
            function: Box::new(*self),
            return_type,
            inputs,
            function_impl,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AvgDecimalImpl<D> {
    datatype: DataType,
    _d: PhantomData<D>,
}

impl<D> AvgDecimalImpl<D> {
    fn new(datatype: DataType) -> Self {
        AvgDecimalImpl {
            datatype,
            _d: PhantomData,
        }
    }
}

impl<D> AggregateFunctionImpl for AvgDecimalImpl<D>
where
    D: DecimalType,
    D::Primitive: Into<i128>,
{
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        let datatype = self.datatype.clone();

        let state_finalize = move |states: &mut [AvgStateDecimal<D::Primitive>]| {
            let mut builder = ArrayBuilder {
                datatype: DataType::Float64,
                buffer: PrimitiveBuffer::with_len(states.len()),
            };

            let mut validities = Bitmap::new_with_all_true(states.len());

            let m = datatype.clone().try_get_decimal_type_meta()?;
            let scale = f64::powi(10.0, m.scale.abs() as i32);

            for (idx, state) in states.iter_mut().enumerate() {
                let ((sum, count), valid) = state.finalize()?;

                if !valid {
                    validities.set(idx, false);
                    continue;
                }

                let val = (sum as f64) / (count as f64 * scale);
                builder.buffer.put(idx, &val);
            }

            Ok(Array::new_with_validity_and_array_data(
                builder.datatype,
                validities,
                builder.buffer.into_data(),
            ))
        };

        new_unary_aggregate_states::<D::Storage, _, _, _, _>(
            AvgStateDecimal::<D::Primitive>::default,
            state_finalize,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvgFloat64Impl;

impl AggregateFunctionImpl for AvgFloat64Impl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        new_unary_aggregate_states::<PhysicalF64, _, _, _, _>(
            AvgStateF64::<f64, f64>::default,
            move |states| primitive_finalize(DataType::Float64, states),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvgInt64Impl;

impl AggregateFunctionImpl for AvgInt64Impl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        new_unary_aggregate_states::<PhysicalI64, _, _, _, _>(
            AvgStateF64::<i64, i128>::default,
            move |states| primitive_finalize(DataType::Float64, states),
        )
    }
}

#[derive(Debug, Default)]
struct AvgStateDecimal<I> {
    sum: i128,
    count: i64,
    _input: PhantomData<I>,
}

impl<I: Into<i128> + Default + Debug> AggregateState<I, (i128, i64)> for AvgStateDecimal<I> {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.sum += other.sum;
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, input: I) -> Result<()> {
        self.sum += input.into();
        self.count += 1;
        Ok(())
    }

    fn finalize(&mut self) -> Result<((i128, i64), bool)> {
        if self.count == 0 {
            return Ok(((0, 0), false));
        }
        Ok(((self.sum, self.count), true))
    }
}

#[derive(Debug, Default)]
struct AvgStateF64<I, T> {
    sum: T,
    count: i64,
    _input: PhantomData<I>,
}

impl<I, T> AggregateState<I, f64> for AvgStateF64<I, T>
where
    I: Into<T> + Default + Debug,
    T: AsPrimitive<f64> + AddAssign + Debug + Default,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.sum += other.sum;
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, input: I) -> Result<()> {
        self.sum += input.into();
        self.count += 1;
        Ok(())
    }

    fn finalize(&mut self) -> Result<(f64, bool)> {
        if self.count == 0 {
            return Ok((0.0, false));
        }
        let sum: f64 = self.sum.as_();
        Ok((sum / self.count as f64, true))
    }
}

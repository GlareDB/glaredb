use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::AddAssign;

use num_traits::AsPrimitive;
use rayexec_error::Result;
use serde::{Deserialize, Serialize};

use crate::arrays::buffer::physical_type::{AddressableMut, PhysicalF64, PhysicalI64};
use crate::arrays::datatype::{DataType, DataTypeId, DecimalTypeMeta};
use crate::arrays::executor_exp::aggregate::AggregateState;
use crate::arrays::executor_exp::PutBuffer;
use crate::arrays::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType};
use crate::expr::Expression;
use crate::functions::aggregate::states::{
    drain,
    unary_update,
    AggregateGroupStates,
    TypedAggregateGroupStates,
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
        let m = self
            .datatype
            .try_get_decimal_type_meta()
            .unwrap_or(DecimalTypeMeta::new(D::MAX_PRECISION, D::DEFAULT_SCALE)); // TODO: Should rework to return the error instead.

        let scale = f64::powi(10.0, m.scale.abs() as i32);

        Box::new(TypedAggregateGroupStates::new(
            move || AvgStateDecimal::<D::Primitive> {
                scale,
                sum: 0,
                count: 0,
                _input: PhantomData,
            },
            unary_update::<D::Storage, PhysicalF64, _>,
            drain::<PhysicalF64, _, _>,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvgFloat64Impl;

impl AggregateFunctionImpl for AvgFloat64Impl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            AvgStateF64::<f64, f64>::default,
            unary_update::<PhysicalF64, PhysicalF64, _>,
            drain::<PhysicalF64, _, _>,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvgInt64Impl;

impl AggregateFunctionImpl for AvgInt64Impl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            AvgStateF64::<i64, i128>::default,
            unary_update::<PhysicalI64, PhysicalF64, _>,
            drain::<PhysicalF64, _, _>,
        ))
    }
}

#[derive(Debug)]
struct AvgStateDecimal<I> {
    /// Scale to use when finalizing the physical decimal value.
    scale: f64,
    sum: i128,
    count: i64,
    _input: PhantomData<I>,
}

impl<I> AggregateState<&I, f64> for AvgStateDecimal<I>
where
    I: Into<i128> + Copy + Debug,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.sum += other.sum;
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, &input: &I) -> Result<()> {
        self.sum += input.into();
        self.count += 1;
        Ok(())
    }

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = f64>,
    {
        if self.count == 0 {
            output.put_null();
            return Ok(());
        }

        let val = (self.sum as f64) / (self.count as f64 * self.scale);
        output.put(&val);

        Ok(())
    }
}

#[derive(Debug, Default)]
struct AvgStateF64<I, T> {
    sum: T,
    count: i64,
    _input: PhantomData<I>,
}

impl<I, T> AggregateState<&I, f64> for AvgStateF64<I, T>
where
    I: Into<T> + Copy + Default + Debug,
    T: AsPrimitive<f64> + AddAssign + Debug + Default,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.sum += other.sum;
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, &input: &I) -> Result<()> {
        self.sum += input.into();
        self.count += 1;
        Ok(())
    }

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = f64>,
    {
        if self.count == 0 {
            output.put_null();
            return Ok(());
        }
        let sum: f64 = self.sum.as_();
        output.put(&sum);

        Ok(())
    }
}

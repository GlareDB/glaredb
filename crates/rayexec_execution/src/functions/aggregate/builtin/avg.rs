use std::any::Any;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::AddAssign;
use std::sync::Arc;

use num_traits::AsPrimitive;
use rayexec_error::{RayexecError, Result};

use crate::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalF64,
    PhysicalI64,
};
use crate::arrays::array::Array;
use crate::arrays::datatype::{DataType, DataTypeId, DecimalTypeMeta};
use crate::arrays::executor::aggregate::{AggregateState, UnaryNonNullUpdater};
use crate::arrays::executor::PutBuffer;
use crate::arrays::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType};
use crate::buffer::buffer_manager::BufferManager;
use crate::expr::Expression;
use crate::functions::aggregate::states::{
    AggregateFunctionImpl,
    AggregateStateLogic,
    UnaryStateLogic,
};
use crate::functions::aggregate::{AggregateFunction, PlannedAggregateFunction};
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

        let (function_impl, return_type) = match inputs[0].datatype()? {
            DataType::Int64 => {
                let function_impl = AggregateFunctionImpl::new::<
                    UnaryStateLogic<AvgStateF64<i64, i128>, PhysicalI64, PhysicalF64>,
                >(None);

                (function_impl, DataType::Float64)
            }
            DataType::Float64 => {
                let function_impl = AggregateFunctionImpl::new::<
                    UnaryStateLogic<AvgStateF64<f64, f64>, PhysicalF64, PhysicalF64>,
                >(None);

                (function_impl, DataType::Float64)
            }
            dt @ DataType::Decimal64(_) => {
                // Datatype only used in order to convert decimal to float
                // at the end. This always returns Float64.
                let datatype = Arc::new(dt) as Arc<_>;
                let function_impl =
                    AggregateFunctionImpl::new::<AvgDecimalImpl<Decimal64Type>>(Some(datatype));

                (function_impl, DataType::Float64)
            }
            dt @ DataType::Decimal128(_) => {
                // See above
                let datatype = Arc::new(dt) as Arc<_>;
                let function_impl =
                    AggregateFunctionImpl::new::<AvgDecimalImpl<Decimal128Type>>(Some(datatype));

                (function_impl, DataType::Float64)
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
    _d: PhantomData<D>,
}

impl<D> AvgDecimalImpl<D> {
    fn new() -> Self {
        AvgDecimalImpl { _d: PhantomData }
    }
}

impl<D> AggregateStateLogic for AvgDecimalImpl<D>
where
    D: DecimalType,
    D::Primitive: Into<i128>,
{
    type State = AvgStateDecimal<D::Primitive>;

    fn init_state(extra: Option<&dyn Any>) -> Self::State {
        let datatype = extra.unwrap().downcast_ref::<DataType>().unwrap();
        let m = datatype
            .try_get_decimal_type_meta()
            .unwrap_or(DecimalTypeMeta::new(D::MAX_PRECISION, D::DEFAULT_SCALE)); // TODO: Should rework to return the error instead.

        let scale = f64::powi(10.0, m.scale.abs() as i32);

        AvgStateDecimal::<D::Primitive> {
            scale,
            sum: 0,
            count: 0,
            _input: PhantomData,
        }
    }

    fn update(
        _extra: Option<&dyn Any>,
        inputs: &[Array],
        num_rows: usize,
        states: &mut [*mut Self::State],
    ) -> Result<()> {
        UnaryNonNullUpdater::update::<D::Storage, _, _>(&inputs[0], 0..num_rows, states)
    }

    fn combine(
        _extra: Option<&dyn Any>,
        src: &mut [&mut Self::State],
        dest: &mut [&mut Self::State],
    ) -> Result<()> {
        // TODO: Reduce duplications with `UnaryStateLogic`
        if src.len() != dest.len() {
            return Err(RayexecError::new(
                "Source and destination have different number of states",
            )
            .with_field("source", src.len())
            .with_field("dest", dest.len()));
        }

        for (src, dest) in src.iter_mut().zip(dest) {
            dest.merge(src)?;
        }

        Ok(())
    }

    fn finalize(
        _extra: Option<&dyn Any>,
        states: &mut [&mut Self::State],
        output: &mut Array,
    ) -> Result<()> {
        // TODO: Reduce duplications with `UnaryStateLogic`
        let buffer = &mut PhysicalF64::get_addressable_mut(&mut output.data)?;
        let validity = &mut output.validity;

        for (idx, state) in states.iter_mut().enumerate() {
            state.finalize(PutBuffer::new(idx, buffer, validity))?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct AvgStateDecimal<I> {
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

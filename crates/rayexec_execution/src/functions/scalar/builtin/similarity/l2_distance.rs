use std::marker::PhantomData;
use std::ops::AddAssign;

use num_traits::{AsPrimitive, Float};
use rayexec_error::Result;

use crate::arrays::array::physical_type::{
    MutableScalarStorage,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    ScalarStorage,
};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::{BinaryListReducer, BinaryReducer};
use crate::arrays::executor::OutBuffer;
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

/// Euclidean distance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L2Distance;

impl FunctionInfo for L2Distance {
    fn name(&self) -> &'static str {
        "l2_distance"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["array_distance"]
    }

    fn signatures(&self) -> &[Signature] {
        // TODO: Ideally return type would depend on the primitive type in the
        // list.
        &[Signature {
            positional_args: &[DataTypeId::List, DataTypeId::List],
            variadic_arg: None,
            return_type: DataTypeId::Float64,
            doc: Some(&Documentation{
                category: Category::List,
                description: "Compute the Euclidean distance between two lists. Both lists must be the same length and cannot contain NULLs.",
                arguments: &["list1", "list2"],
                example: Some(Example{
                    example: "l2_distance([1.0, 1.0], [2.0, 4.0])",
                    output: "3.1622776601683795",
                }),
            }),
        }]
    }
}

impl ScalarFunction for L2Distance {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 2)?;

        let function_impl: Box<dyn ScalarFunctionImpl> = match (
            inputs[0].datatype(table_list)?,
            inputs[1].datatype(table_list)?,
        ) {
            (DataType::List(a), DataType::List(b)) => {
                match (a.datatype.as_ref(), b.datatype.as_ref()) {
                    (DataType::Float16, DataType::Float16) => {
                        Box::new(L2DistanceImpl::<PhysicalF16>::new())
                    }
                    (DataType::Float32, DataType::Float32) => {
                        Box::new(L2DistanceImpl::<PhysicalF32>::new())
                    }
                    (DataType::Float64, DataType::Float64) => {
                        Box::new(L2DistanceImpl::<PhysicalF64>::new())
                    }
                    (a, b) => return Err(invalid_input_types_error(self, &[a, b])),
                }
            }
            (a, b) => return Err(invalid_input_types_error(self, &[a, b])),
        };

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Float64,
            inputs,
            function_impl,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L2DistanceImpl<S: ScalarStorage> {
    _s: PhantomData<S>,
}

impl<S> L2DistanceImpl<S>
where
    S: ScalarStorage,
{
    fn new() -> Self {
        L2DistanceImpl { _s: PhantomData }
    }
}

impl<S> ScalarFunctionImpl for L2DistanceImpl<S>
where
    S: MutableScalarStorage,
    S::StorageType: Float + AddAssign + AsPrimitive<f64> + Default + Copy,
{
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let a = &input.arrays()[0];
        let b = &input.arrays()[1];

        BinaryListReducer::reduce::<S, S, L2DistanceReducer<_>, PhysicalF64>(
            a,
            sel,
            b,
            sel,
            OutBuffer::from_array(output)?,
        )
    }
}

#[derive(Debug, Default)]
pub(crate) struct L2DistanceReducer<F> {
    pub distance: F,
}

impl<F> BinaryReducer<&F, &F, f64> for L2DistanceReducer<F>
where
    F: Float + AddAssign + AsPrimitive<f64> + Default + Copy,
{
    fn put_values(&mut self, &v1: &F, &v2: &F) {
        let diff = v1 - v2;
        self.distance += diff * diff;
    }

    fn finish(self) -> f64 {
        self.distance.as_().sqrt()
    }
}

#[cfg(test)]
mod tests {

    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::buffer_manager::NopBufferManager;
    use crate::arrays::compute::make_list::make_list_from_values;
    use crate::arrays::datatype::ListTypeMeta;
    use crate::expr;
    use crate::testutil::arrays::assert_arrays_eq;

    #[test]
    fn l2_distance_ok() {
        let mut a = Array::new(
            &NopBufferManager,
            DataType::List(ListTypeMeta::new(DataType::Float64)),
            1,
        )
        .unwrap();
        make_list_from_values(
            &[
                Array::try_from_iter([1.0]).unwrap(),
                Array::try_from_iter([2.0]).unwrap(),
                Array::try_from_iter([3.0]).unwrap(),
            ],
            0..1,
            &mut a,
        )
        .unwrap();

        let mut b = Array::new(
            &NopBufferManager,
            DataType::List(ListTypeMeta::new(DataType::Float64)),
            1,
        )
        .unwrap();
        make_list_from_values(
            &[
                Array::try_from_iter([1.0]).unwrap(),
                Array::try_from_iter([2.0]).unwrap(),
                Array::try_from_iter([4.0]).unwrap(),
            ],
            0..1,
            &mut b,
        )
        .unwrap();

        let batch = Batch::from_arrays([a, b]).unwrap();

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![
                    DataType::List(ListTypeMeta::new(DataType::Float64)),
                    DataType::List(ListTypeMeta::new(DataType::Float64)),
                ],
                vec!["a".to_string(), "b".to_string()],
            )
            .unwrap();

        let planned = L2Distance
            .plan(
                &table_list,
                vec![expr::col_ref(table_ref, 0), expr::col_ref(table_ref, 1)],
            )
            .unwrap();

        let mut out = Array::new(&NopBufferManager, DataType::Float64, 1).unwrap();
        planned.function_impl.execute(&batch, &mut out).unwrap();

        let expected = Array::try_from_iter([1.0]).unwrap();
        assert_arrays_eq(&expected, &out);
    }
}

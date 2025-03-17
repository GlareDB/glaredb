use std::marker::PhantomData;
use std::ops::AddAssign;

use glaredb_error::Result;
use num_traits::{AsPrimitive, Float};

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    MutableScalarStorage,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    ScalarStorage,
};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::{BinaryListReducer, BinaryReducer};
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_L2_DISTANCE: ScalarFunctionSet = ScalarFunctionSet {
    name: "l2_distance",
    aliases: &["array_distance"],
    doc: Some(&Documentation {
        category: Category::List,
        description: "Compute the Euclidean distance between two lists. Both lists must be the same length and cannot contain NULLs.",
        arguments: &["list1", "list2"],
        example: Some(Example {
            example: "l2_distance([1.0, 1.0], [2.0, 4.0])",
            output: "3.1622776601683795",
        }),
    }),
    functions: &[
        RawScalarFunction::new(
            &Signature::new(
                &[
                    DataTypeId::List(&DataTypeId::Float16),
                    DataTypeId::List(&DataTypeId::Float16),
                ],
                DataTypeId::Float64,
            ),
            &L2Distance::<PhysicalF16>::new(),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[
                    DataTypeId::List(&DataTypeId::Float32),
                    DataTypeId::List(&DataTypeId::Float32),
                ],
                DataTypeId::Float64,
            ),
            &L2Distance::<PhysicalF32>::new(),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[
                    DataTypeId::List(&DataTypeId::Float64),
                    DataTypeId::List(&DataTypeId::Float64),
                ],
                DataTypeId::Float64,
            ),
            &L2Distance::<PhysicalF64>::new(),
        ),
    ],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L2Distance<S: ScalarStorage> {
    _s: PhantomData<S>,
}

impl<S> L2Distance<S>
where
    S: ScalarStorage,
{
    pub const fn new() -> Self {
        L2Distance { _s: PhantomData }
    }
}

impl<S> ScalarFunction for L2Distance<S>
where
    S: MutableScalarStorage,
    S::StorageType: Float + AddAssign + AsPrimitive<f64> + Default + Copy,
{
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Float64,
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
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

// #[cfg(test)]
// mod tests {

//     use crate::util::iter::TryFromExactSizeIterator;

//     use super::*;
//     use crate::arrays::compute::make_list::make_list_from_values;
//     use crate::arrays::datatype::ListTypeMeta;
//     use crate::buffer::buffer_manager::NopBufferManager;
//     use crate::expr;
//     use crate::testutil::arrays::assert_arrays_eq;

//     #[test]
//     fn l2_distance_ok() {
//         let mut a = Array::new(
//             &NopBufferManager,
//             DataType::List(ListTypeMeta::new(DataType::Float64)),
//             1,
//         )
//         .unwrap();
//         make_list_from_values(
//             &[
//                 Array::try_from_iter([1.0]).unwrap(),
//                 Array::try_from_iter([2.0]).unwrap(),
//                 Array::try_from_iter([3.0]).unwrap(),
//             ],
//             0..1,
//             &mut a,
//         )
//         .unwrap();

//         let mut b = Array::new(
//             &NopBufferManager,
//             DataType::List(ListTypeMeta::new(DataType::Float64)),
//             1,
//         )
//         .unwrap();
//         make_list_from_values(
//             &[
//                 Array::try_from_iter([1.0]).unwrap(),
//                 Array::try_from_iter([2.0]).unwrap(),
//                 Array::try_from_iter([4.0]).unwrap(),
//             ],
//             0..1,
//             &mut b,
//         )
//         .unwrap();

//         let batch = Batch::from_arrays([a, b]).unwrap();

//         let mut table_list = TableList::empty();
//         let table_ref = table_list
//             .push_table(
//                 None,
//                 vec![
//                     DataType::List(ListTypeMeta::new(DataType::Float64)),
//                     DataType::List(ListTypeMeta::new(DataType::Float64)),
//                 ],
//                 vec!["a".to_string(), "b".to_string()],
//             )
//             .unwrap();

//         let planned = L2Distance
//             .plan(
//                 &table_list,
//                 vec![expr::col_ref(table_ref, 0), expr::col_ref(table_ref, 1)],
//             )
//             .unwrap();

//         let mut out = Array::new(&NopBufferManager, DataType::Float64, 1).unwrap();
//         planned.function_impl.execute(&batch, &mut out).unwrap();

//         let expected = Array::try_from_iter([1.0]).unwrap();
//         assert_arrays_eq(&expected, &out);
//     }
// }

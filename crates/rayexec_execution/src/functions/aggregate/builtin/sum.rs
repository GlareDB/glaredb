use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::AddAssign;

use num_traits::CheckedAdd;
use rayexec_error::Result;

use crate::arrays::array::physical_type::{AddressableMut, PhysicalF64, PhysicalI64};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::executor::PutBuffer;
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
pub struct Sum;

impl FunctionInfo for Sum {
    fn name(&self) -> &'static str {
        "sum"
    }

    fn signatures(&self) -> &[Signature] {
        const DOC: &Documentation = &Documentation {
            category: Category::Aggregate,
            description: "Compute the sum of all non-NULL inputs.",
            arguments: &["inputs"],
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
                return_type: DataTypeId::Int64, // TODO: Should be big num
                doc: Some(DOC),
            },
            Signature {
                positional_args: &[DataTypeId::Decimal64],
                variadic_arg: None,
                return_type: DataTypeId::Decimal64,
                doc: Some(DOC),
            },
            Signature {
                positional_args: &[DataTypeId::Decimal128],
                variadic_arg: None,
                return_type: DataTypeId::Decimal128,
                doc: Some(DOC),
            },
        ]
    }
}

impl AggregateFunction for Sum {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        let (function_impl, return_type): (Box<dyn AggregateFunctionImpl>, _) =
            match inputs[0].datatype(table_list)? {
                DataType::Int64 => (Box::new(SumInt64Impl), DataType::Int64),
                DataType::Float64 => (Box::new(SumFloat64Impl), DataType::Float64),
                DataType::Decimal64(m) => {
                    let datatype = DataType::Decimal64(m);
                    (Box::new(SumDecimalImpl::<Decimal64Type>::new()), datatype)
                }
                DataType::Decimal128(m) => {
                    let datatype = DataType::Decimal128(m);
                    (Box::new(SumDecimalImpl::<Decimal128Type>::new()), datatype)
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
pub struct SumInt64Impl;

impl AggregateFunctionImpl for SumInt64Impl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            SumStateCheckedAdd::<i64>::default,
            unary_update::<PhysicalI64, PhysicalI64, _>,
            drain::<PhysicalI64, _, _>,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct SumFloat64Impl;

impl AggregateFunctionImpl for SumFloat64Impl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            SumStateAdd::<f64>::default,
            unary_update::<PhysicalF64, PhysicalF64, _>,
            drain::<PhysicalF64, _, _>,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct SumDecimalImpl<D> {
    _d: PhantomData<D>,
}

impl<D> SumDecimalImpl<D> {
    const fn new() -> Self {
        SumDecimalImpl { _d: PhantomData }
    }
}

impl<D> AggregateFunctionImpl for SumDecimalImpl<D>
where
    D: DecimalType,
{
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        Box::new(TypedAggregateGroupStates::new(
            SumStateCheckedAdd::<D::Primitive>::default,
            unary_update::<D::Storage, D::Storage, _>,
            drain::<D::Storage, _, _>,
        ))
    }
}

#[derive(Debug, Default)]
pub struct SumStateCheckedAdd<T> {
    sum: T,
    valid: bool,
}

impl<T> AggregateState<&T, T> for SumStateCheckedAdd<T>
where
    T: CheckedAdd + Default + Debug + Copy,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.sum = self.sum.checked_add(&other.sum).unwrap_or_default(); // TODO
        self.valid = self.valid || other.valid;
        Ok(())
    }

    fn update(&mut self, input: &T) -> Result<()> {
        self.sum = self.sum.checked_add(input).unwrap_or_default(); // TODO
        self.valid = true;
        Ok(())
    }

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = T>,
    {
        if self.valid {
            output.put(&self.sum);
        } else {
            output.put_null();
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct SumStateAdd<T> {
    sum: T,
    valid: bool,
}

impl<T> AggregateState<&T, T> for SumStateAdd<T>
where
    T: AddAssign + Default + Debug + Copy,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.sum += other.sum;
        self.valid = self.valid || other.valid;
        Ok(())
    }

    fn update(&mut self, &input: &T) -> Result<()> {
        self.sum += input;
        self.valid = true;
        Ok(())
    }

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = T>,
    {
        if self.valid {
            output.put(&self.sum);
        } else {
            output.put_null();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::buffer_manager::NopBufferManager;
    use crate::arrays::array::selection::Selection;
    use crate::arrays::array::Array;
    use crate::arrays::testutil::{assert_arrays_eq, assert_arrays_eq_sel};
    use crate::expr;

    #[test]
    fn sum_i64_single_group_two_partitions() {
        // Single group, two partitions, 'SELECT SUM(a) FROM table'

        let partition_1_vals = Array::try_from_iter::<[i64; 3]>([1, 2, 3]).unwrap();
        let partition_2_vals = Array::try_from_iter::<[i64; 3]>([4, 5, 6]).unwrap();

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(None, vec![DataType::Int64], vec!["c0".to_string()])
            .unwrap();

        let specialized = Sum
            .plan(&table_list, vec![expr::col_ref(table_ref, 0)])
            .unwrap();

        let mut states_1 = specialized.function_impl.new_states();
        let mut states_2 = specialized.function_impl.new_states();

        states_1.new_groups(1);
        states_2.new_groups(1);

        // All inputs map to the same group (no GROUP BY clause)
        states_1
            .update_group_states(&[&partition_1_vals], Selection::linear(0, 3), &[0, 0, 0])
            .unwrap();
        states_2
            .update_group_states(&[&partition_2_vals], Selection::linear(0, 3), &[0, 0, 0])
            .unwrap();

        // Combine states.
        //
        // Both partitions hold a single state (representing a single group),
        // and those states map to each other.
        states_1
            .combine(&mut states_2, Selection::slice(&[0]), &[0])
            .unwrap();

        // Get final output.
        let mut out = Array::try_new(&NopBufferManager, DataType::Int64, 1).unwrap();
        states_1.drain(&mut out).unwrap();

        let expected = Array::try_from_iter([21_i64]).unwrap();
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn sum_i64_two_groups_two_partitions() {
        // Two groups, two partitions, 'SELECT SUM(col2) FROM table GROUP BY col1'
        //
        // | col1 | col2 |
        // |------|------|
        // | 'a'  | 1    |
        // | 'a'  | 2    |
        // | 'b'  | 3    |
        // | 'b'  | 4    |
        // | 'b'  | 5    |
        // | 'a'  | 6    |
        //
        // Partition values and mappings represent the positions of the above
        // table. The actual grouping values are stored in the operator, and
        // operator is what computes the mappings.
        let partition_1_vals = Array::try_from_iter::<[i64; 3]>([1, 2, 3]).unwrap();
        let partition_2_vals = Array::try_from_iter::<[i64; 3]>([4, 5, 6]).unwrap();

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataType::Utf8, DataType::Int64],
                vec!["col1".to_string(), "col2".to_string()],
            )
            .unwrap();

        let specialized = Sum
            .plan(&table_list, vec![expr::col_ref(table_ref, 1)])
            .unwrap();

        let mut states_1 = specialized.function_impl.new_states();
        let mut states_2 = specialized.function_impl.new_states();

        // Both partitions are operating on two groups ('a' and 'b').
        states_1.new_groups(1);
        states_1.new_groups(1);

        states_2.new_groups(1);
        states_2.new_groups(1);

        // Mapping corresponding to the above table. Group 'a' == 0 and group
        // 'b' == 1.
        states_1
            .update_group_states(&[&partition_1_vals], Selection::linear(0, 3), &[0, 0, 1])
            .unwrap();
        states_2
            .update_group_states(&[&partition_2_vals], Selection::linear(0, 3), &[1, 1, 0])
            .unwrap();

        // Combine states.
        //
        // The above `mapping_1` and `mapping_2` vectors indices that the state
        // for group 'a' is state 0 in each partition, and group 'b' is state 1
        // in each.
        //
        // The mapping here indicates the the 0th state for both partitions
        // should be combined, and the 1st state for both partitions should be
        // combined.
        states_1
            .combine(
                &mut states_2,
                Selection::linear(0, 2), // States 0 ('a') and 1 ('b')
                &[0, 1],
            )
            .unwrap();

        // Get final output.
        let mut out = Array::try_new(&NopBufferManager, DataType::Int64, 2).unwrap();
        states_1.drain(&mut out).unwrap();

        let expected = Array::try_from_iter([9_i64, 12_i64]).unwrap();
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn sum_i64_three_groups_two_partitions_with_unseen_group() {
        // Three groups, two partitions, 'SELECT SUM(col2) FROM table GROUP BY col1'
        //
        // This test represents a case where we're merging two aggregate hash
        // maps, where the map we're merging into has seen more groups than the
        // one that's being consumed. The implementation of the hash aggregate
        // operator ensures either this is the case, or that both hash maps have
        // seen the same number of groups.
        //
        // | col1 | col2 |
        // |------|------|
        // | 'x'  | 1    |
        // | 'x'  | 2    |
        // | 'y'  | 3    |
        // | 'z'  | 4    |
        // | 'x'  | 5    |
        // | 'z'  | 6    |
        // | 'z'  | 7    |
        // | 'z'  | 8    |
        //
        // Partition values and mappings represent the positions of the above
        // table. The actual grouping values are stored in the operator, and
        // operator is what computes the mappings.
        let partition_1_vals = Array::try_from_iter::<[i64; 4]>([1, 2, 3, 4]).unwrap();
        let partition_2_vals = Array::try_from_iter::<[i64; 4]>([5, 6, 7, 8]).unwrap();

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataType::Utf8, DataType::Int64],
                vec!["col1".to_string(), "col2".to_string()],
            )
            .unwrap();

        let specialized = Sum
            .plan(&table_list, vec![expr::col_ref(table_ref, 1)])
            .unwrap();

        let mut states_1 = specialized.function_impl.new_states();
        let mut states_2 = specialized.function_impl.new_states();

        // Partition 1 sees groups 'x', 'y', and 'z'.
        states_1.new_groups(1);
        states_1.new_groups(1);
        states_1.new_groups(1);

        // Partition 2 see groups 'x' and 'z' (no 'y').
        states_2.new_groups(1);
        states_2.new_groups(1);

        // For partition 1: 'x' == 0, 'y' == 1, 'z' == 2
        states_1
            .update_group_states(&[&partition_1_vals], Selection::linear(0, 4), &[0, 0, 1, 2])
            .unwrap();
        // For partition 2: 'x' == 0, 'z' == 1
        states_2
            .update_group_states(&[&partition_2_vals], Selection::linear(0, 4), &[0, 1, 1, 1])
            .unwrap();

        // Combine states.
        //
        // States for 'x' both at the same position.
        //
        // States for 'y' at different positions, partition_2_state[1] => partition_1_state[2]
        states_1
            .combine(&mut states_2, Selection::slice(&[0, 1]), &[0, 2])
            .unwrap();

        // Get final output.
        let mut out = Array::try_new(&NopBufferManager, DataType::Int64, 3).unwrap();
        states_1.drain(&mut out).unwrap();

        let expected = Array::try_from_iter([8_i64, 3_i64, 25_i64]).unwrap();
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn sum_i64_drain_multiple() {
        // Three groups, single partition, test that drain can be called
        // multiple times until states are exhausted.
        let vals = Array::try_from_iter::<[i64; 6]>([1, 2, 3, 4, 5, 6]).unwrap();

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataType::Utf8, DataType::Int64],
                vec!["col1".to_string(), "col2".to_string()],
            )
            .unwrap();

        let specialized = Sum
            .plan(&table_list, vec![expr::col_ref(table_ref, 1)])
            .unwrap();
        let mut states = specialized.function_impl.new_states();

        states.new_groups(3);

        states
            .update_group_states(&[&vals], Selection::linear(0, 6), &[0, 0, 1, 1, 2, 2])
            .unwrap();

        let mut out = Array::try_new(&NopBufferManager, DataType::Int64, 2).unwrap();

        let n = states.drain(&mut out).unwrap();
        assert_eq!(2, n);

        let expected = Array::try_from_iter([3_i64, 7]).unwrap();
        assert_arrays_eq(&expected, &out);

        let n = states.drain(&mut out).unwrap();
        assert_eq!(1, n);

        let expected = Array::try_from_iter([11_i64]).unwrap();
        assert_arrays_eq_sel(&expected, 0..1, &out, 0..1);

        let n = states.drain(&mut out).unwrap();
        assert_eq!(0, n);
    }
}

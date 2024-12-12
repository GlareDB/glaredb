use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::AddAssign;

use num_traits::CheckedAdd;
use rayexec_bullet::array::ArrayData;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::aggregate::AggregateState;
use rayexec_bullet::executor::physical_type::{PhysicalF64, PhysicalI64};
use rayexec_bullet::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType};
use rayexec_bullet::storage::PrimitiveStorage;
use rayexec_error::Result;

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
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sum;

impl FunctionInfo for Sum {
    fn name(&self) -> &'static str {
        "sum"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataTypeId::Float64],
                variadic: None,
                return_type: DataTypeId::Float64,
            },
            Signature {
                input: &[DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::Int64, // TODO: Should be big num
            },
            Signature {
                input: &[DataTypeId::Decimal64],
                variadic: None,
                return_type: DataTypeId::Decimal64,
            },
            Signature {
                input: &[DataTypeId::Decimal128],
                variadic: None,
                return_type: DataTypeId::Decimal128,
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
                    (
                        Box::new(SumDecimalImpl::<Decimal64Type>::new(datatype.clone())),
                        datatype,
                    )
                }
                DataType::Decimal128(m) => {
                    let datatype = DataType::Decimal128(m);
                    (
                        Box::new(SumDecimalImpl::<Decimal128Type>::new(datatype.clone())),
                        datatype,
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
pub struct SumInt64Impl;

impl AggregateFunctionImpl for SumInt64Impl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        new_unary_aggregate_states::<PhysicalI64, _, _, _, _>(
            SumStateCheckedAdd::<i64>::default,
            move |states| primitive_finalize(DataType::Int64, states),
        )
    }
}

#[derive(Debug, Clone)]
pub struct SumFloat64Impl;

impl AggregateFunctionImpl for SumFloat64Impl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        new_unary_aggregate_states::<PhysicalF64, _, _, _, _>(
            SumStateAdd::<f64>::default,
            move |states| primitive_finalize(DataType::Float64, states),
        )
    }
}

#[derive(Debug, Clone)]
pub struct SumDecimalImpl<D> {
    datatype: DataType,
    _d: PhantomData<D>,
}

impl<D> SumDecimalImpl<D> {
    fn new(datatype: DataType) -> Self {
        SumDecimalImpl {
            datatype,
            _d: PhantomData,
        }
    }
}

impl<D> AggregateFunctionImpl for SumDecimalImpl<D>
where
    D: DecimalType,
    ArrayData: From<PrimitiveStorage<D::Primitive>>,
{
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        let datatype = self.datatype.clone();

        new_unary_aggregate_states::<D::Storage, _, _, _, _>(
            SumStateCheckedAdd::<D::Primitive>::default,
            move |states| primitive_finalize(datatype.clone(), states),
        )
    }
}

#[derive(Debug, Default)]
pub struct SumStateCheckedAdd<T> {
    sum: T,
    set: bool,
}

impl<T: CheckedAdd + Default + Debug + Copy> AggregateState<T, T> for SumStateCheckedAdd<T> {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.sum = self.sum.checked_add(&other.sum).unwrap_or_default(); // TODO
        self.set = self.set || other.set;
        Ok(())
    }

    fn update(&mut self, input: T) -> Result<()> {
        self.sum = self.sum.checked_add(&input).unwrap_or_default(); // TODO
        self.set = true;
        Ok(())
    }

    fn finalize(&mut self) -> Result<(T, bool)> {
        if self.set {
            Ok((self.sum, true))
        } else {
            Ok((T::default(), false))
        }
    }
}

#[derive(Debug, Default)]
pub struct SumStateAdd<T> {
    sum: T,
    valid: bool,
}

impl<T: AddAssign + Default + Debug + Copy> AggregateState<T, T> for SumStateAdd<T> {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.sum += other.sum;
        self.valid = self.valid || other.valid;
        Ok(())
    }

    fn update(&mut self, input: T) -> Result<()> {
        self.sum += input;
        self.valid = true;
        Ok(())
    }

    fn finalize(&mut self) -> Result<(T, bool)> {
        if self.valid {
            Ok((self.sum, true))
        } else {
            Ok((T::default(), false))
        }
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::array::Array;
    use rayexec_bullet::scalar::ScalarValue;

    use super::*;
    use crate::execution::operators::hash_aggregate::hash_table::GroupAddress;
    use crate::expr;
    use crate::functions::aggregate::ChunkGroupAddressIter;

    #[test]
    fn sum_i64_single_group_two_partitions() {
        // Single group, two partitions, 'SELECT SUM(a) FROM table'

        let partition_1_vals = &Array::from_iter::<[i64; 3]>([1, 2, 3]);
        let partition_2_vals = &Array::from_iter::<[i64; 3]>([4, 5, 6]);

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(None, vec![DataType::Int64], vec!["c0".to_string()])
            .unwrap();

        let specialized = Sum
            .plan(&table_list, vec![expr::col_ref(table_ref, 0)])
            .unwrap();

        let mut states_1 = specialized.function_impl.new_states();
        let mut states_2 = specialized.function_impl.new_states();

        states_1.new_states(1);
        states_2.new_states(1);

        // All inputs map to the same group (no GROUP BY clause)
        let addrs_1: Vec<_> = (0..partition_1_vals.logical_len())
            .map(|_| GroupAddress {
                chunk_idx: 0,
                row_idx: 0,
            })
            .collect();
        let addrs_2: Vec<_> = (0..partition_2_vals.logical_len())
            .map(|_| GroupAddress {
                chunk_idx: 0,
                row_idx: 0,
            })
            .collect();

        states_1
            .update_states(&[partition_1_vals], ChunkGroupAddressIter::new(0, &addrs_1))
            .unwrap();
        states_2
            .update_states(&[partition_2_vals], ChunkGroupAddressIter::new(0, &addrs_2))
            .unwrap();

        // Combine states.
        //
        // Both partitions hold a single state (representing a single group),
        // and those states map to each other.
        let combine_mapping = vec![GroupAddress {
            chunk_idx: 0,
            row_idx: 0,
        }];
        states_1
            .combine(
                &mut states_2,
                ChunkGroupAddressIter::new(0, &combine_mapping),
            )
            .unwrap();

        // Get final output.
        let out = states_1.finalize().unwrap();

        assert_eq!(1, out.logical_len());
        assert_eq!(ScalarValue::Int64(21), out.logical_value(0).unwrap());
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
        let partition_1_vals = &Array::from_iter::<[i64; 3]>([1, 2, 3]);
        let partition_2_vals = &Array::from_iter::<[i64; 3]>([4, 5, 6]);

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
        states_1.new_states(1);
        states_1.new_states(1);

        states_2.new_states(1);
        states_2.new_states(1);

        // Mapping corresponding to the above table. Group 'a' == 0 and group
        // 'b' == 1.
        let addrs_1 = vec![
            GroupAddress {
                chunk_idx: 0,
                row_idx: 0,
            },
            GroupAddress {
                chunk_idx: 0,
                row_idx: 0,
            },
            GroupAddress {
                chunk_idx: 0,
                row_idx: 1,
            },
        ];
        let addrs_2 = vec![
            GroupAddress {
                chunk_idx: 0,
                row_idx: 1,
            },
            GroupAddress {
                chunk_idx: 0,
                row_idx: 1,
            },
            GroupAddress {
                chunk_idx: 0,
                row_idx: 0,
            },
        ];

        states_1
            .update_states(&[partition_1_vals], ChunkGroupAddressIter::new(0, &addrs_1))
            .unwrap();
        states_2
            .update_states(&[partition_2_vals], ChunkGroupAddressIter::new(0, &addrs_2))
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
        let combine_mapping = vec![
            GroupAddress {
                chunk_idx: 0,
                row_idx: 0,
            },
            GroupAddress {
                chunk_idx: 0,
                row_idx: 1,
            },
        ];
        states_1
            .combine(
                &mut states_2,
                ChunkGroupAddressIter::new(0, &combine_mapping),
            )
            .unwrap();

        // Get final output.
        let out = states_1.finalize().unwrap();

        assert_eq!(2, out.logical_len());
        assert_eq!(ScalarValue::Int64(9), out.logical_value(0).unwrap());
        assert_eq!(ScalarValue::Int64(12), out.logical_value(1).unwrap());
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
        let partition_1_vals = &Array::from_iter::<[i64; 4]>([1, 2, 3, 4]);
        let partition_2_vals = &Array::from_iter::<[i64; 4]>([5, 6, 7, 8]);

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
        states_1.new_states(1);
        states_1.new_states(1);
        states_1.new_states(1);

        // Partition 2 see groups 'x' and 'z' (no 'y').
        states_2.new_states(1);
        states_2.new_states(1);

        // For partition 1: 'x' == 0, 'y' == 1, 'z' == 2
        let addrs_1 = vec![
            GroupAddress {
                chunk_idx: 0,
                row_idx: 0,
            },
            GroupAddress {
                chunk_idx: 0,
                row_idx: 0,
            },
            GroupAddress {
                chunk_idx: 0,
                row_idx: 1,
            },
            GroupAddress {
                chunk_idx: 0,
                row_idx: 2,
            },
        ];
        // For partition 2: 'x' == 0, 'z' == 1
        let addrs_2 = vec![
            GroupAddress {
                chunk_idx: 0,
                row_idx: 0,
            },
            GroupAddress {
                chunk_idx: 0,
                row_idx: 1,
            },
            GroupAddress {
                chunk_idx: 0,
                row_idx: 1,
            },
            GroupAddress {
                chunk_idx: 0,
                row_idx: 1,
            },
        ];

        states_1
            .update_states(&[partition_1_vals], ChunkGroupAddressIter::new(0, &addrs_1))
            .unwrap();
        states_2
            .update_states(&[partition_2_vals], ChunkGroupAddressIter::new(0, &addrs_2))
            .unwrap();

        // Combine states.
        //
        // States for 'x' both at the same position.
        //
        // States for 'y' at different positions, partition_2_state[1] => partition_1_state[2]
        let combine_mapping = vec![
            GroupAddress {
                chunk_idx: 0,
                row_idx: 0,
            },
            GroupAddress {
                chunk_idx: 0,
                row_idx: 2,
            },
        ];
        states_1
            .combine(
                &mut states_2,
                ChunkGroupAddressIter::new(0, &combine_mapping),
            )
            .unwrap();

        // Get final output.
        let out = states_1.finalize().unwrap();

        assert_eq!(3, out.logical_len());
        assert_eq!(ScalarValue::Int64(8), out.logical_value(0).unwrap());
        assert_eq!(ScalarValue::Int64(3), out.logical_value(1).unwrap());
        assert_eq!(ScalarValue::Int64(25), out.logical_value(2).unwrap());
    }

    // #[test]
    // fn sum_i64_drain_multiple() {
    //     // Three groups, single partition, test that drain can be called
    //     // multiple times until states are exhausted.
    //     let vals = &Array::from_iter::<[i64; 6]>([1, 2, 3, 4, 5, 6]);

    //     let specialized = Sum.plan_from_datatypes(&[DataType::Int64]).unwrap();
    //     let mut states = specialized.new_grouped_state();

    //     states.new_group();
    //     states.new_group();
    //     states.new_group();

    //     let addrs = vec![
    //         GroupAddress {
    //             chunk_idx: 0,
    //             row_idx: 0,
    //         },
    //         GroupAddress {
    //             chunk_idx: 0,
    //             row_idx: 0,
    //         },
    //         GroupAddress {
    //             chunk_idx: 0,
    //             row_idx: 1,
    //         },
    //         GroupAddress {
    //             chunk_idx: 0,
    //             row_idx: 1,
    //         },
    //         GroupAddress {
    //             chunk_idx: 0,
    //             row_idx: 2,
    //         },
    //         GroupAddress {
    //             chunk_idx: 0,
    //             row_idx: 2,
    //         },
    //     ];

    //     states
    //         .update_states(&[vals], ChunkGroupAddressIter::new(0, &addrs))
    //         .unwrap();

    //     let out_1 = states.drain_next(2).unwrap().unwrap();
    //     assert_eq!(2, out_1.logical_len());
    //     assert_eq!(ScalarValue::Int64(3), out_1.logical_value(0).unwrap());
    //     assert_eq!(ScalarValue::Int64(7), out_1.logical_value(1).unwrap());

    //     let out_2 = states.drain_next(2).unwrap().unwrap();
    //     assert_eq!(1, out_2.logical_len());
    //     assert_eq!(ScalarValue::Int64(11), out_2.logical_value(0).unwrap());

    //     let out_3 = states.drain_next(2).unwrap();
    //     assert_eq!(None, out_3);
    // }
}

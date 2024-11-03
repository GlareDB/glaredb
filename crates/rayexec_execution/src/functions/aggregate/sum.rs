use std::fmt::Debug;
use std::ops::AddAssign;

use num_traits::CheckedAdd;
use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId, DecimalTypeMeta};
use rayexec_bullet::executor::aggregate::{AggregateState, StateFinalizer, UnaryNonNullUpdater};
use rayexec_bullet::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::{PhysicalF64, PhysicalI128, PhysicalI64};
use rayexec_error::{RayexecError, Result};
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use serde::{Deserialize, Serialize};

use super::{
    primitive_finalize,
    unary_update,
    AggregateFunction,
    ChunkGroupAddressIter,
    DefaultGroupedStates,
    GroupedStates,
    PlannedAggregateFunction,
};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};

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
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedAggregateFunction>> {
        let mut packed = PackedDecoder::new(state);
        let variant: String = packed.decode_next()?;
        match variant.as_str() {
            "decimal_64" => {
                let precision: i32 = packed.decode_next()?;
                let scale: i32 = packed.decode_next()?;
                Ok(Box::new(SumImpl::Decimal64(SumDecimal64Impl {
                    precision: precision as u8,
                    scale: scale as i8,
                })))
            }
            "decimal_128" => {
                let precision: i32 = packed.decode_next()?;
                let scale: i32 = packed.decode_next()?;
                Ok(Box::new(SumImpl::Decimal128(SumDecimal128Impl {
                    precision: precision as u8,
                    scale: scale as i8,
                })))
            }
            "float_64" => Ok(Box::new(SumImpl::Float64(SumFloat64Impl))),
            "int_64" => Ok(Box::new(SumImpl::Int64(SumInt64Impl))),

            other => Err(RayexecError::new(format!("Invalid avg variant: {other}"))),
        }
    }

    fn plan_from_datatypes(
        &self,
        inputs: &[DataType],
    ) -> Result<Box<dyn PlannedAggregateFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Int64 => Ok(Box::new(SumImpl::Int64(SumInt64Impl))),
            DataType::Float64 => Ok(Box::new(SumImpl::Float64(SumFloat64Impl))),
            DataType::Decimal64(meta) => Ok(Box::new(SumImpl::Decimal64(SumDecimal64Impl {
                precision: meta.precision,
                scale: meta.scale,
            }))),
            DataType::Decimal128(meta) => Ok(Box::new(SumImpl::Decimal128(SumDecimal128Impl {
                precision: meta.precision,
                scale: meta.scale,
            }))),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SumImpl {
    Int64(SumInt64Impl),
    Float64(SumFloat64Impl),
    Decimal64(SumDecimal64Impl),
    Decimal128(SumDecimal128Impl),
}

impl PlannedAggregateFunction for SumImpl {
    fn aggregate_function(&self) -> &dyn AggregateFunction {
        &Sum
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        let mut packed = PackedEncoder::new(state);
        match self {
            Self::Decimal64(v) => {
                packed.encode_next(&"decimal_64".to_string())?;
                packed.encode_next(&(v.precision as i32))?;
                packed.encode_next(&(v.scale as i32))?;
            }
            Self::Decimal128(v) => {
                packed.encode_next(&"decimal_128".to_string())?;
                packed.encode_next(&(v.precision as i32))?;
                packed.encode_next(&(v.scale as i32))?;
            }
            Self::Float64(_) => {
                packed.encode_next(&"float_64".to_string())?;
            }
            Self::Int64(_) => {
                packed.encode_next(&"int_64".to_string())?;
            }
        }
        Ok(())
    }

    fn return_type(&self) -> DataType {
        match self {
            Self::Decimal64(s) => DataType::Decimal64(DecimalTypeMeta::new(s.precision, s.scale)),
            Self::Decimal128(s) => DataType::Decimal128(DecimalTypeMeta::new(s.precision, s.scale)),
            Self::Float64(_) => DataType::Float64,
            Self::Int64(_) => DataType::Int64,
        }
    }

    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        match self {
            Self::Decimal64(s) => s.new_grouped_state(),
            Self::Decimal128(s) => s.new_grouped_state(),
            Self::Float64(s) => s.new_grouped_state(),
            Self::Int64(s) => s.new_grouped_state(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SumInt64Impl;

impl SumInt64Impl {
    fn update(
        arrays: &[&Array],
        mapping: ChunkGroupAddressIter,
        states: &mut [SumStateCheckedAdd<i64>],
    ) -> Result<()> {
        UnaryNonNullUpdater::update::<PhysicalI64, _, _, _>(arrays[0], mapping, states)
    }

    fn finalize(states: &mut [SumStateCheckedAdd<i64>]) -> Result<Array> {
        let builder = ArrayBuilder {
            datatype: DataType::Int64,
            buffer: PrimitiveBuffer::<i64>::with_len(states.len()),
        };
        StateFinalizer::finalize(states, builder)
    }

    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        Box::new(DefaultGroupedStates::new(Self::update, Self::finalize))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SumFloat64Impl;

impl SumFloat64Impl {
    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        Box::new(DefaultGroupedStates::new(
            unary_update::<SumStateAdd<f64>, PhysicalF64, f64>,
            move |states| primitive_finalize(DataType::Float64, states),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SumDecimal64Impl {
    precision: u8,
    scale: i8,
}

impl SumDecimal64Impl {
    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        let datatype = DataType::Decimal64(DecimalTypeMeta::new(self.precision, self.scale));
        Box::new(DefaultGroupedStates::new(
            unary_update::<SumStateCheckedAdd<i64>, PhysicalI64, i64>,
            move |states| primitive_finalize(datatype.clone(), states),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SumDecimal128Impl {
    precision: u8,
    scale: i8,
}

impl SumDecimal128Impl {
    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        let datatype = DataType::Decimal128(DecimalTypeMeta::new(self.precision, self.scale));
        Box::new(DefaultGroupedStates::new(
            unary_update::<SumStateCheckedAdd<i128>, PhysicalI128, i128>,
            move |states| primitive_finalize(datatype.clone(), states),
        ))
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

    #[test]
    fn sum_i64_single_group_two_partitions() {
        // Single group, two partitions, 'SELECT SUM(a) FROM table'

        let partition_1_vals = &Array::from_iter::<[i64; 3]>([1, 2, 3]);
        let partition_2_vals = &Array::from_iter::<[i64; 3]>([4, 5, 6]);

        let specialized = Sum.plan_from_datatypes(&[DataType::Int64]).unwrap();

        let mut states_1 = specialized.new_grouped_state();
        let mut states_2 = specialized.new_grouped_state();

        states_1.new_groups(1);
        states_2.new_groups(1);

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
        let out = states_1.drain().unwrap();

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

        let specialized = Sum.plan_from_datatypes(&[DataType::Int64]).unwrap();

        let mut states_1 = specialized.new_grouped_state();
        let mut states_2 = specialized.new_grouped_state();

        // Both partitions are operating on two groups ('a' and 'b').
        states_1.new_groups(1);
        states_1.new_groups(1);

        states_2.new_groups(1);
        states_2.new_groups(1);

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
        let out = states_1.drain().unwrap();

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

        let specialized = Sum.plan_from_datatypes(&[DataType::Int64]).unwrap();

        let mut states_1 = specialized.new_grouped_state();
        let mut states_2 = specialized.new_grouped_state();

        // Partition 1 sees groups 'x', 'y', and 'z'.
        states_1.new_groups(1);
        states_1.new_groups(1);
        states_1.new_groups(1);

        // Partition 2 see groups 'x' and 'z' (no 'y').
        states_2.new_groups(1);
        states_2.new_groups(1);

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
        let out = states_1.drain().unwrap();

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

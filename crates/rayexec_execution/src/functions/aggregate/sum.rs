use super::{AggregateFunction, DefaultGroupedStates, GroupedStates, PlannedAggregateFunction};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use num_traits::CheckedAdd;
use rayexec_bullet::{
    array::{Array, Decimal128Array, Decimal64Array, PrimitiveArray},
    bitmap::Bitmap,
    datatype::{DataType, DataTypeId, DecimalTypeMeta},
    executor::aggregate::{AggregateState, StateFinalizer, UnaryNonNullUpdater},
};
use rayexec_error::{RayexecError, Result};
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, ops::AddAssign, vec};

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
        row_selection: &Bitmap,
        arrays: &[&Array],
        mapping: &[usize],
        states: &mut [SumStateCheckedAdd<i64>],
    ) -> Result<()> {
        match &arrays[0] {
            Array::Int64(arr) => UnaryNonNullUpdater::update(row_selection, arr, mapping, states),
            other => panic!("unexpected array type: {other:?}"),
        }
    }

    fn finalize(states: vec::Drain<SumStateCheckedAdd<i64>>) -> Result<Array> {
        let mut buffer = Vec::with_capacity(states.len());
        let mut bitmap = Bitmap::with_capacity(states.len());
        StateFinalizer::finalize(states, &mut buffer, &mut bitmap)?;
        Ok(Array::Int64(PrimitiveArray::new(buffer, Some(bitmap))))
    }

    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        Box::new(DefaultGroupedStates::new(Self::update, Self::finalize))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SumFloat64Impl;

impl SumFloat64Impl {
    fn update(
        row_selection: &Bitmap,
        arrays: &[&Array],
        mapping: &[usize],
        states: &mut [SumStateAdd<f64>],
    ) -> Result<()> {
        match &arrays[0] {
            Array::Float64(arr) => UnaryNonNullUpdater::update(row_selection, arr, mapping, states),
            other => panic!("unexpected array type: {other:?}"),
        }
    }

    fn finalize(states: vec::Drain<SumStateAdd<f64>>) -> Result<Array> {
        let mut buffer = Vec::with_capacity(states.len());
        let mut bitmap = Bitmap::with_capacity(states.len());
        StateFinalizer::finalize(states, &mut buffer, &mut bitmap)?;
        Ok(Array::Float64(PrimitiveArray::new(buffer, Some(bitmap))))
    }

    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        Box::new(DefaultGroupedStates::new(Self::update, Self::finalize))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SumDecimal64Impl {
    precision: u8,
    scale: i8,
}

impl SumDecimal64Impl {
    fn update(
        row_selection: &Bitmap,
        arrays: &[&Array],
        mapping: &[usize],
        states: &mut [SumStateCheckedAdd<i64>],
    ) -> Result<()> {
        match &arrays[0] {
            Array::Decimal64(arr) => {
                UnaryNonNullUpdater::update(row_selection, arr.get_primitive(), mapping, states)
            }
            other => panic!("unexpected array type: {other:?}"),
        }
    }

    fn finalize(states: vec::Drain<SumStateCheckedAdd<i64>>) -> Result<Array> {
        let mut buffer = Vec::with_capacity(states.len());
        let mut bitmap = Bitmap::with_capacity(states.len());
        StateFinalizer::finalize(states, &mut buffer, &mut bitmap)?;
        Ok(Array::Int64(PrimitiveArray::new(buffer, Some(bitmap))))
    }

    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        let precision = self.precision;
        let scale = self.scale;
        let finalize = move |states: vec::Drain<_>| match Self::finalize(states)? {
            Array::Int64(arr) => Ok(Array::Decimal64(Decimal64Array::new(precision, scale, arr))),
            other => panic!("unexpected array type: {}", other.datatype()),
        };
        Box::new(DefaultGroupedStates::new(Self::update, finalize))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SumDecimal128Impl {
    precision: u8,
    scale: i8,
}

impl SumDecimal128Impl {
    fn update(
        row_selection: &Bitmap,
        arrays: &[&Array],
        mapping: &[usize],
        states: &mut [SumStateCheckedAdd<i128>],
    ) -> Result<()> {
        match &arrays[0] {
            Array::Decimal128(arr) => {
                UnaryNonNullUpdater::update(row_selection, arr.get_primitive(), mapping, states)
            }
            other => panic!("unexpected array type: {other:?}"),
        }
    }

    fn finalize(states: vec::Drain<SumStateCheckedAdd<i128>>) -> Result<Array> {
        let mut buffer = Vec::with_capacity(states.len());
        let mut bitmap = Bitmap::with_capacity(states.len());
        StateFinalizer::finalize(states, &mut buffer, &mut bitmap)?;
        Ok(Array::Int128(PrimitiveArray::new(buffer, Some(bitmap))))
    }

    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        let precision = self.precision;
        let scale = self.scale;
        let finalize = move |states: vec::Drain<_>| match Self::finalize(states)? {
            Array::Int128(arr) => Ok(Array::Decimal128(Decimal128Array::new(
                precision, scale, arr,
            ))),
            other => panic!("unexpected array type: {}", other.datatype()),
        };
        Box::new(DefaultGroupedStates::new(Self::update, finalize))
    }
}

#[derive(Debug, Default)]
pub struct SumStateCheckedAdd<T> {
    sum: T,
    set: bool,
}

impl<T: CheckedAdd + Default + Debug> AggregateState<T, T> for SumStateCheckedAdd<T> {
    fn merge(&mut self, other: Self) -> Result<()> {
        self.sum = self.sum.checked_add(&other.sum).unwrap_or_default(); // TODO
        self.set = self.set || other.set;
        Ok(())
    }

    fn update(&mut self, input: T) -> Result<()> {
        self.sum = self.sum.checked_add(&input).unwrap_or_default(); // TODO
        self.set = true;
        Ok(())
    }

    fn finalize(self) -> Result<(T, bool)> {
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

impl<T: AddAssign + Default + Debug> AggregateState<T, T> for SumStateAdd<T> {
    fn merge(&mut self, other: Self) -> Result<()> {
        self.sum += other.sum;
        self.valid = self.valid || other.valid;
        Ok(())
    }

    fn update(&mut self, input: T) -> Result<()> {
        self.sum += input;
        self.valid = true;
        Ok(())
    }

    fn finalize(self) -> Result<(T, bool)> {
        if self.valid {
            Ok((self.sum, true))
        } else {
            Ok((T::default(), false))
        }
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::array::{Array, Int64Array};

    use super::*;

    #[test]
    fn sum_i64_single_group_two_partitions() {
        // Single group, two partitions, 'SELECT SUM(a) FROM table'

        let partition_1_vals = &Array::Int64(Int64Array::from_iter([1, 2, 3]));
        let partition_2_vals = &Array::Int64(Int64Array::from_iter([4, 5, 6]));

        let specialized = Sum.plan_from_datatypes(&[DataType::Int64]).unwrap();

        let mut states_1 = specialized.new_grouped_state();
        let mut states_2 = specialized.new_grouped_state();

        let idx_1 = states_1.new_group();
        assert_eq!(0, idx_1);

        let idx_2 = states_2.new_group();
        assert_eq!(0, idx_2);

        // All inputs map to the same group (no GROUP BY clause)
        let mapping_1 = vec![0; partition_1_vals.len()];
        let mapping_2 = vec![0; partition_2_vals.len()];

        states_1
            .update_states(&Bitmap::all_true(3), &[partition_1_vals], &mapping_1)
            .unwrap();
        states_2
            .update_states(&Bitmap::all_true(3), &[partition_2_vals], &mapping_2)
            .unwrap();

        // Combine states.
        //
        // Both partitions hold a single state (representing a single group),
        // and those states map to each other.
        let combine_mapping = vec![0];
        states_1.try_combine(states_2, &combine_mapping).unwrap();

        // Get final output.
        let out = states_1.drain_finalize_n(100).unwrap();
        let expected = Array::Int64(Int64Array::from_iter([Some(21)]));
        assert_eq!(expected, out.unwrap());
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
        let partition_1_vals = &Array::Int64(Int64Array::from_iter([1, 2, 3]));
        let partition_2_vals = &Array::Int64(Int64Array::from_iter([4, 5, 6]));

        let specialized = Sum.plan_from_datatypes(&[DataType::Int64]).unwrap();

        let mut states_1 = specialized.new_grouped_state();
        let mut states_2 = specialized.new_grouped_state();

        // Both partitions are operating on two groups ('a' and 'b').
        states_1.new_group();
        states_1.new_group();

        states_2.new_group();
        states_2.new_group();

        // Mapping corresponding to the above table. Group 'a' == 0 and group
        // 'b' == 1.
        let mapping_1 = vec![0, 0, 1];
        let mapping_2 = vec![1, 1, 0];

        states_1
            .update_states(&Bitmap::all_true(3), &[partition_1_vals], &mapping_1)
            .unwrap();
        states_2
            .update_states(&Bitmap::all_true(3), &[partition_2_vals], &mapping_2)
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
        let combine_mapping = vec![0, 1];
        states_1.try_combine(states_2, &combine_mapping).unwrap();

        // Get final output.
        let out = states_1.drain_finalize_n(100).unwrap();
        let expected = Array::Int64(Int64Array::from_iter([Some(9), Some(12)]));
        assert_eq!(expected, out.unwrap());
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
        let partition_1_vals = &Array::Int64(Int64Array::from_iter([1, 2, 3, 4]));
        let partition_2_vals = &Array::Int64(Int64Array::from_iter([5, 6, 7, 8]));

        let specialized = Sum.plan_from_datatypes(&[DataType::Int64]).unwrap();

        let mut states_1 = specialized.new_grouped_state();
        let mut states_2 = specialized.new_grouped_state();

        // Partition 1 sees groups 'x', 'y', and 'z'.
        states_1.new_group();
        states_1.new_group();
        states_1.new_group();

        // Partition 2 see groups 'x' and 'z' (no 'y').
        states_2.new_group();
        states_2.new_group();

        // For partitions 1: 'x' == 0, 'y' == 1, 'z' == 2
        let mapping_1 = vec![0, 0, 1, 2];
        // For partitions 2: 'x' == 0, 'z' == 1
        let mapping_2 = vec![0, 1, 1, 1];

        states_1
            .update_states(&Bitmap::all_true(4), &[partition_1_vals], &mapping_1)
            .unwrap();
        states_2
            .update_states(&Bitmap::all_true(4), &[partition_2_vals], &mapping_2)
            .unwrap();

        // Combine states.
        //
        // States for 'x' both at the same position.
        //
        // States for 'y' at different positions, partition_2_state[1] => partition_1_state[2]
        let combine_mapping = vec![0, 2];
        states_1.try_combine(states_2, &combine_mapping).unwrap();

        // Get final output.
        let out = states_1.drain_finalize_n(100).unwrap();
        let expected = Array::Int64(Int64Array::from_iter([Some(8), Some(3), Some(25)]));
        assert_eq!(expected, out.unwrap());
    }

    #[test]
    fn sum_i64_drain_multiple() {
        // Three groups, single partition, test that drain can be called
        // multiple times until states are exhausted.
        let vals = &Array::Int64(Int64Array::from_iter([1, 2, 3, 4, 5, 6]));

        let specialized = Sum.plan_from_datatypes(&[DataType::Int64]).unwrap();
        let mut states = specialized.new_grouped_state();

        states.new_group();
        states.new_group();
        states.new_group();

        let mapping = vec![0, 0, 1, 1, 2, 2];
        states
            .update_states(&Bitmap::all_true(6), &[vals], &mapping)
            .unwrap();

        let expected_1 = Array::Int64(Int64Array::from_iter([Some(3), Some(7)]));
        let out_1 = states.drain_finalize_n(2).unwrap();
        assert_eq!(Some(expected_1), out_1);

        let expected_2 = Array::Int64(Int64Array::from_iter([Some(11)]));
        let out_2 = states.drain_finalize_n(2).unwrap();
        assert_eq!(Some(expected_2), out_2);

        let out_3 = states.drain_finalize_n(2).unwrap();
        assert_eq!(None, out_3);
    }
}

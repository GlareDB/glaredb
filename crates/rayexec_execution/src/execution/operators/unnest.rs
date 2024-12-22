use std::borrow::Borrow;
use std::sync::Arc;
use std::task::{Context, Waker};

use half::f16;
use rayexec_bullet::array::{ArrayData, ArrayOld};
use rayexec_bullet::batch::BatchOld;
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::executor::builder::{
    ArrayBuilder,
    ArrayDataBuffer,
    BooleanBuffer,
    GermanVarlenBuffer,
    PrimitiveBuffer,
};
use rayexec_bullet::executor::physical_type::{
    PhysicalBinaryOld,
    PhysicalBoolOld,
    PhysicalF16Old,
    PhysicalF32Old,
    PhysicalF64Old,
    PhysicalI128Old,
    PhysicalI16Old,
    PhysicalI32Old,
    PhysicalI64Old,
    PhysicalI8Old,
    PhysicalList,
    PhysicalStorageOld,
    PhysicalType,
    PhysicalU128Old,
    PhysicalU16Old,
    PhysicalU32Old,
    PhysicalU64Old,
    PhysicalU8Old,
    PhysicalUtf8Old,
};
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_bullet::selection::{self, SelectionVector};
use rayexec_bullet::storage::{AddressableStorage, ListItemMetadata};
use rayexec_error::{not_implemented, RayexecError, Result};

use super::{
    ExecutableOperatorOld,
    ExecutionStates,
    InputOutputStates,
    OperatorStateOld,
    PartitionStateOld,
    PollFinalizeOld,
    PollPullOld,
    PollPushOld,
};
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::PhysicalScalarExpression;

#[derive(Debug)]
pub struct UnnestPartitionState {
    /// Projections that need to extended to match the unnest outputs.
    project_inputs: Vec<ArrayOld>,
    /// Inputs we're processing.
    unnest_inputs: Vec<ArrayOld>,
    /// Number of rows in the input.
    input_num_rows: usize,
    /// Row we're currently unnesting.
    current_row: usize,
    /// If inputs are finished.
    finished: bool,
    /// Push side waker.
    ///
    /// Set if we still have rows to process.
    push_waker: Option<Waker>,
    /// Pull side waker.
    ///
    /// Set if we've processed all rows and need more input.
    pull_waker: Option<Waker>,
}

#[derive(Debug)]
pub struct PhysicalUnnest {
    pub project_expressions: Vec<PhysicalScalarExpression>,
    pub unnest_expressions: Vec<PhysicalScalarExpression>,
}

impl ExecutableOperatorOld for PhysicalUnnest {
    fn create_states_old(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        let partitions = partitions[0];

        let states: Vec<_> = (0..partitions)
            .map(|_| {
                PartitionStateOld::Unnest(UnnestPartitionState {
                    project_inputs: vec![
                        ArrayOld::new_untyped_null_array(0);
                        self.project_expressions.len()
                    ],
                    unnest_inputs: vec![
                        ArrayOld::new_untyped_null_array(0);
                        self.unnest_expressions.len()
                    ],
                    input_num_rows: 0,
                    current_row: 0,
                    finished: false,
                    push_waker: None,
                    pull_waker: None,
                })
            })
            .collect();

        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorStateOld::None),
            partition_states: InputOutputStates::OneToOne {
                partition_states: states,
            },
        })
    }

    fn poll_push_old(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionStateOld,
        _operator_state: &OperatorStateOld,
        batch: BatchOld,
    ) -> Result<PollPushOld> {
        let state = match partition_state {
            PartitionStateOld::Unnest(state) => state,
            other => panic!("invalid state: {other:?}"),
        };

        if state.current_row < state.input_num_rows {
            // Still processing inputs, come back later.
            state.push_waker = Some(cx.waker().clone());
            if let Some(waker) = state.pull_waker.take() {
                waker.wake();
            }

            return Ok(PollPushOld::Pending(batch));
        }

        // Compute inputs. These will be stored until we've processed all rows.
        for (col_idx, expr) in self.project_expressions.iter().enumerate() {
            state.project_inputs[col_idx] = expr.eval2(&batch)?.into_owned();
        }

        for (col_idx, expr) in self.unnest_expressions.iter().enumerate() {
            state.unnest_inputs[col_idx] = expr.eval2(&batch)?.into_owned();
        }

        state.input_num_rows = batch.num_rows();
        state.current_row = 0;

        if let Some(waker) = state.pull_waker.take() {
            waker.wake();
        }

        Ok(PollPushOld::Pushed)
    }

    fn poll_finalize_push_old(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionStateOld,
        _operator_state: &OperatorStateOld,
    ) -> Result<PollFinalizeOld> {
        let state = match partition_state {
            PartitionStateOld::Unnest(state) => state,
            other => panic!("invalid state: {other:?}"),
        };

        state.finished = true;

        if let Some(waker) = state.pull_waker.take() {
            waker.wake();
        }

        Ok(PollFinalizeOld::Finalized)
    }

    fn poll_pull_old(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionStateOld,
        _operator_state: &OperatorStateOld,
    ) -> Result<PollPullOld> {
        let state = match partition_state {
            PartitionStateOld::Unnest(state) => state,
            other => panic!("invalid state: {other:?}"),
        };

        if state.current_row >= state.input_num_rows {
            if state.finished {
                return Ok(PollPullOld::Exhausted);
            }

            // We're done with these inputs. Come back later.
            state.pull_waker = Some(cx.waker().clone());
            if let Some(waker) = state.push_waker.take() {
                waker.wake();
            }

            return Ok(PollPullOld::Pending);
        }

        // We have input ready, get the longest list for the current row.
        let mut longest = 0;
        for input_idx in 0..state.unnest_inputs.len() {
            if state.unnest_inputs[input_idx].physical_type() == PhysicalType::UntypedNull {
                // Just let other unnest expressions determine the number of
                // rows.
                continue;
            }

            if let Some(list_meta) = UnaryExecutor::value_at::<PhysicalList>(
                &state.unnest_inputs[input_idx],
                state.current_row,
            )? {
                if list_meta.len > longest {
                    longest = list_meta.len;
                }
            }
        }

        let mut outputs =
            Vec::with_capacity(state.project_inputs.len() + state.unnest_inputs.len());

        // Process plain project inputs.
        //
        // Create a selection vector that points to the current row to extend
        // out the values as needed.
        let selection = Arc::new(SelectionVector::from(vec![
            state.current_row;
            longest as usize
        ]));
        for projected in &state.project_inputs {
            let mut out = projected.clone();
            out.select_mut(selection.clone());
            outputs.push(out);
        }

        // Now process unnests.
        for input_idx in 0..state.unnest_inputs.len() {
            let arr = &state.unnest_inputs[input_idx];

            match arr.physical_type() {
                PhysicalType::List => {
                    let child = match arr.array_data() {
                        ArrayData::List(list) => list.inner_array(),
                        _other => return Err(RayexecError::new("Unexpected storage type")),
                    };

                    match UnaryExecutor::value_at::<PhysicalList>(arr, state.current_row)? {
                        Some(meta) => {
                            // Row is a list, unnest.
                            let out = unnest(child, longest as usize, meta)?;
                            outputs.push(out);
                        }
                        None => {
                            // Row is null, produce nulls according to longest
                            // length.
                            let out = ArrayOld::new_typed_null_array(
                                child.datatype().clone(),
                                longest as usize,
                            )?;
                            outputs.push(out);
                        }
                    }
                }
                PhysicalType::UntypedNull => {
                    // Just produce null array according to longest length.
                    let out = ArrayOld::new_untyped_null_array(longest as usize);
                    outputs.push(out);
                }
                other => {
                    return Err(RayexecError::new(format!(
                        "Unexpected physical type in unnest: {other:?}"
                    )))
                }
            }
        }

        // Next pull works on the next row.
        state.current_row += 1;

        // If these inputs are done, go ahead and let the push side know.
        if state.current_row >= state.input_num_rows {
            if let Some(waker) = state.push_waker.take() {
                waker.wake()
            }
        }

        let batch = BatchOld::try_new(outputs)?;

        Ok(PollPullOld::Computed(batch.into()))
    }
}

impl Explainable for PhysicalUnnest {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Unnest")
            .with_values("project_expressions", &self.project_expressions)
            .with_values("unnest_expressions", &self.unnest_expressions)
    }
}

pub(crate) fn unnest(
    child: &ArrayOld,
    longest_len: usize,
    meta: ListItemMetadata,
) -> Result<ArrayOld> {
    let datatype = child.datatype().clone();

    match child.physical_type() {
        PhysicalType::UntypedNull => Ok(ArrayOld::new_untyped_null_array(longest_len)),
        PhysicalType::Boolean => {
            let builder = ArrayBuilder {
                datatype,
                buffer: BooleanBuffer::with_len(longest_len),
            };
            unnest_inner::<PhysicalBoolOld, _>(builder, child, meta)
        }
        PhysicalType::Int8 => {
            let builder = ArrayBuilder {
                datatype,
                buffer: PrimitiveBuffer::<i8>::with_len(longest_len),
            };
            unnest_inner::<PhysicalI8Old, _>(builder, child, meta)
        }
        PhysicalType::Int16 => {
            let builder = ArrayBuilder {
                datatype,
                buffer: PrimitiveBuffer::<i16>::with_len(longest_len),
            };
            unnest_inner::<PhysicalI16Old, _>(builder, child, meta)
        }
        PhysicalType::Int32 => {
            let builder = ArrayBuilder {
                datatype,
                buffer: PrimitiveBuffer::<i32>::with_len(longest_len),
            };
            unnest_inner::<PhysicalI32Old, _>(builder, child, meta)
        }
        PhysicalType::Int64 => {
            let builder = ArrayBuilder {
                datatype,
                buffer: PrimitiveBuffer::<i64>::with_len(longest_len),
            };
            unnest_inner::<PhysicalI64Old, _>(builder, child, meta)
        }
        PhysicalType::Int128 => {
            let builder = ArrayBuilder {
                datatype,
                buffer: PrimitiveBuffer::<i128>::with_len(longest_len),
            };
            unnest_inner::<PhysicalI128Old, _>(builder, child, meta)
        }
        PhysicalType::UInt8 => {
            let builder = ArrayBuilder {
                datatype,
                buffer: PrimitiveBuffer::<u8>::with_len(longest_len),
            };
            unnest_inner::<PhysicalU8Old, _>(builder, child, meta)
        }
        PhysicalType::UInt16 => {
            let builder = ArrayBuilder {
                datatype,
                buffer: PrimitiveBuffer::<u16>::with_len(longest_len),
            };
            unnest_inner::<PhysicalU16Old, _>(builder, child, meta)
        }
        PhysicalType::UInt32 => {
            let builder = ArrayBuilder {
                datatype,
                buffer: PrimitiveBuffer::<u32>::with_len(longest_len),
            };
            unnest_inner::<PhysicalU32Old, _>(builder, child, meta)
        }
        PhysicalType::UInt64 => {
            let builder = ArrayBuilder {
                datatype,
                buffer: PrimitiveBuffer::<u64>::with_len(longest_len),
            };
            unnest_inner::<PhysicalU64Old, _>(builder, child, meta)
        }
        PhysicalType::UInt128 => {
            let builder = ArrayBuilder {
                datatype,
                buffer: PrimitiveBuffer::<u128>::with_len(longest_len),
            };
            unnest_inner::<PhysicalU128Old, _>(builder, child, meta)
        }
        PhysicalType::Float16 => {
            let builder = ArrayBuilder {
                datatype,
                buffer: PrimitiveBuffer::<f16>::with_len(longest_len),
            };
            unnest_inner::<PhysicalF16Old, _>(builder, child, meta)
        }
        PhysicalType::Float32 => {
            let builder = ArrayBuilder {
                datatype,
                buffer: PrimitiveBuffer::<f32>::with_len(longest_len),
            };
            unnest_inner::<PhysicalF32Old, _>(builder, child, meta)
        }
        PhysicalType::Float64 => {
            let builder = ArrayBuilder {
                datatype,
                buffer: PrimitiveBuffer::<f64>::with_len(longest_len),
            };
            unnest_inner::<PhysicalF64Old, _>(builder, child, meta)
        }
        PhysicalType::Utf8 => {
            let builder = ArrayBuilder {
                datatype,
                buffer: GermanVarlenBuffer::<str>::with_len(longest_len),
            };
            unnest_inner::<PhysicalUtf8Old, _>(builder, child, meta)
        }
        PhysicalType::Binary => {
            let builder = ArrayBuilder {
                datatype,
                buffer: GermanVarlenBuffer::<[u8]>::with_len(longest_len),
            };
            unnest_inner::<PhysicalBinaryOld, _>(builder, child, meta)
        }
        other => not_implemented!("Unnest for physical type {other:?}"),
    }
}

fn unnest_inner<'a, S, B>(
    mut builder: ArrayBuilder<B>,
    child: &'a ArrayOld,
    meta: ListItemMetadata,
) -> Result<ArrayOld>
where
    S: PhysicalStorageOld,
    B: ArrayDataBuffer,
    S::Type<'a>: Borrow<B::Type>,
{
    let selection = child.selection_vector();
    // Note out len may differ from the length indicated by the list item
    // metadata. Just means we need to ensure the trailing values are marked
    // NULL.
    let out_len = builder.buffer.len();

    match child.validity() {
        Some(validity) => {
            let values = S::get_storage(child.array_data())?;
            let mut out_validity = Bitmap::new_with_all_false(out_len);

            for (out_idx, child_idx) in (meta.offset..meta.offset + meta.len).enumerate() {
                let child_idx = child_idx as usize;
                let sel = selection::get(selection, child_idx);

                if !validity.value(sel) {
                    continue;
                }

                let val = unsafe { values.get_unchecked(sel) };
                out_validity.set(out_idx, true);
                builder.buffer.put(out_idx, val.borrow());
            }

            Ok(ArrayOld::new_with_validity_and_array_data(
                builder.datatype,
                out_validity,
                builder.buffer.into_data(),
            ))
        }
        None => {
            let values = S::get_storage(child.array_data())?;

            // Note we always have an output validity since we may be producing
            // an array from a list that contains fewer items than the number of
            // rows we're producing.
            let mut out_validity = Bitmap::new_with_all_false(out_len);

            for (out_idx, child_idx) in (meta.offset..meta.offset + meta.len).enumerate() {
                let child_idx = child_idx as usize;
                let sel = selection::get(selection, child_idx);

                let val = unsafe { values.get_unchecked(sel) };
                out_validity.set(out_idx, true);
                builder.buffer.put(out_idx, val.borrow());
            }

            Ok(ArrayOld::new_with_validity_and_array_data(
                builder.datatype,
                out_validity,
                builder.buffer.into_data(),
            ))
        }
    }
}

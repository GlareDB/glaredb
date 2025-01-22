use std::collections::HashMap;
use std::task::{Context, Waker};

use rayexec_error::{RayexecError, Result};

use crate::arrays::array::physical_type::{PhysicalList, PhysicalType};
use crate::arrays::array::{Array, ArrayData2};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::field::{Field, Schema};
use crate::arrays::scalar::OwnedScalarValue;
use crate::execution::operators::{ExecuteInOutState, PollExecute, PollFinalize, PollPush};
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::table::inout::{InOutPollPull, TableInOutFunction, TableInOutPartitionState};
use crate::functions::table::{
    InOutPlanner,
    PlannedTableFunction,
    TableFunction,
    TableFunctionImpl,
    TableFunctionPlanner,
};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;
use crate::logical::statistics::StatisticsValue;

#[derive(Debug, Clone, Copy)]
pub struct Unnest;

impl FunctionInfo for Unnest {
    fn name(&self) -> &'static str {
        "unnest"
    }

    fn signatures(&self) -> &[Signature] {
        const DOC: &Documentation = &Documentation {
            category: Category::Table,
            description: "Unnest a list, producing a table of unnested values.",
            arguments: &["list"],
            example: None,
        };

        &[
            Signature {
                positional_args: &[DataTypeId::List],
                variadic_arg: None,
                return_type: DataTypeId::Any,
                doc: Some(DOC),
            },
            Signature {
                positional_args: &[DataTypeId::Null],
                variadic_arg: None,
                return_type: DataTypeId::Null,
                doc: Some(DOC),
            },
        ]
    }
}

impl TableFunction for Unnest {
    fn planner(&self) -> TableFunctionPlanner {
        TableFunctionPlanner::InOut(self)
    }
}

impl InOutPlanner for Unnest {
    fn plan(
        &self,
        table_list: &TableList,
        positional_inputs: Vec<Expression>,
        named_inputs: HashMap<String, OwnedScalarValue>,
    ) -> Result<PlannedTableFunction> {
        plan_check_num_args(self, &positional_inputs, 1)?;
        if !named_inputs.is_empty() {
            return Err(RayexecError::new(
                "UNNEST does not yet accept named arguments",
            ));
        }

        let datatype = positional_inputs[0].datatype(table_list)?;

        let return_type = match datatype {
            DataType::List(m) => *m.datatype,
            DataType::Null => DataType::Null,
            other => return Err(invalid_input_types_error(self, &[other])),
        };

        let schema = Schema::new([Field::new("unnest", return_type, true)]);

        Ok(PlannedTableFunction {
            function: Box::new(*self),
            positional_inputs,
            named_inputs,
            function_impl: TableFunctionImpl::InOut(Box::new(UnnestInOutImpl)),
            cardinality: StatisticsValue::Unknown,
            schema,
        })
    }
}

#[derive(Debug, Clone)]
pub struct UnnestInOutImpl;

impl TableInOutFunction for UnnestInOutImpl {
    fn create_states(
        &self,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn TableInOutPartitionState>>> {
        let states: Vec<_> = (0..num_partitions)
            .map(|_| {
                Box::new(UnnestInOutPartitionState {
                    input: None,
                    input_num_rows: 0,
                    current_row: 0,
                    finished: false,
                    push_waker: None,
                    pull_waker: None,
                }) as _
            })
            .collect();

        Ok(states)
    }
}

// TODO: A lot of this is duplicated with the Unnest operator.
//
// Ideally we'd have a more generic operator for handling table funcs in the
// select list at some point.
//
// Nearer term we should look at combining the logic a bit more.
#[derive(Debug)]
pub struct UnnestInOutPartitionState {
    /// The array we're unnesting.
    input: Option<Array>,
    /// Number of rows in the input batch.
    input_num_rows: usize,
    /// Current row we're processing.
    current_row: usize,
    /// If we're finished receiving inputs.
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

impl TableInOutPartitionState for UnnestInOutPartitionState {
    fn poll_execute(&mut self, cx: &mut Context, inout: ExecuteInOutState) -> Result<PollExecute> {
        unimplemented!()
    }

    fn poll_finalize(&mut self, cx: &mut Context) -> Result<PollFinalize> {
        unimplemented!()
    }

    // fn poll_push(&mut self, cx: &mut Context, inputs: Batch) -> Result<PollPush> {
    //     if self.current_row < self.input_num_rows {
    //         // Still processing inputs, come back later.
    //         self.push_waker = Some(cx.waker().clone());
    //         if let Some(waker) = self.pull_waker.take() {
    //             waker.wake();
    //         }

    //         return Ok(PollPush::Pending(inputs));
    //     }

    //     self.input_num_rows = inputs.num_rows();
    //     self.current_row = 0;

    //     match inputs.arrays().len() {
    //         1 => self.input = inputs.into_arrays().pop(),
    //         other => {
    //             return Err(RayexecError::new("Invalid number of arrays").with_field("len", other))
    //         }
    //     }

    //     if let Some(waker) = self.pull_waker.take() {
    //         waker.wake();
    //     }

    //     Ok(PollPush::Pushed)
    // }

    // fn poll_finalize_push(&mut self, _cx: &mut Context) -> Result<PollFinalize> {
    //     self.finished = true;

    //     if let Some(waker) = self.pull_waker.take() {
    //         waker.wake();
    //     }

    //     Ok(PollFinalize::Finalized)
    // }

    // fn poll_pull(&mut self, cx: &mut Context) -> Result<InOutPollPull> {
    //     if self.current_row >= self.input_num_rows {
    //         if self.finished {
    //             return Ok(InOutPollPull::Exhausted);
    //         }

    //         // We're done with these inputs. Come back later.
    //         self.pull_waker = Some(cx.waker().clone());
    //         if let Some(waker) = self.push_waker.take() {
    //             waker.wake();
    //         }

    //         return Ok(InOutPollPull::Pending);
    //     }

    //     let input = self.input.as_ref().unwrap();
    //     let output = match input.physical_type() {
    //         PhysicalType::List => {
    //             let child = match input.array_data() {
    //                 ArrayData2::List(list) => list.inner_array(),
    //                 _other => return Err(RayexecError::new("Unexpected storage type")),
    //             };

    //             match UnaryExecutor::value_at2::<PhysicalList>(input, self.current_row)? {
    //                 Some(meta) => {
    //                     // Row is a list, unnest.
    //                     unnest(child, meta.len as usize, meta)?
    //                 }
    //                 None => {
    //                     // Row is null, produce as single null
    //                     Array::new_typed_null_array(child.datatype().clone(), 1)?
    //                 }
    //             }
    //         }
    //         PhysicalType::UntypedNull => {
    //             // Just produce null array of length 1.
    //             Array::new_untyped_null_array(1)
    //         }
    //         other => {
    //             return Err(RayexecError::new(format!(
    //                 "Unexpected physical type in unnest: {other:?}"
    //             )))
    //         }
    //     };

    //     let row_nums = vec![self.current_row; output.logical_len()];

    //     // Next pull works on the next row.
    //     self.current_row += 1;

    //     // If these inputs are done, go ahead and let the push side know.
    //     if self.current_row >= self.input_num_rows {
    //         if let Some(waker) = self.push_waker.take() {
    //             waker.wake()
    //         }
    //     }

    //     let batch = Batch::try_from_arrays([output])?;

    //     Ok(InOutPollPull::Batch { batch, row_nums })
    // }
}

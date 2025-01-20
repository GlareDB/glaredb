use std::fmt::Debug;

use rayexec_error::{RayexecError, Result};

use crate::execution::operators::{PollExecute, PollFinalize};

/// Handle effects for the stack.
pub trait StackEffectHandler {
    /// Handle execution for the operator at the given index.
    fn handle_execute(&mut self, op_idx: usize) -> Result<PollExecute>;

    /// Handle finalize for the operator at the given index.
    fn handle_finalize(&mut self, op_idx: usize) -> Result<PollFinalize>;
}

/// Control flow returned from the stack to notify the pipeline on how to
/// proceed with execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StackControlFlow {
    /// Stack has more instructions, keep going.
    Continue,
    /// No more instructions in the stack, execution complete.
    Finished,
    /// Operator returned a pending poll, bubble up pending.
    Pending,
}

/// Instructions for driving execution of a pipeline.
#[derive(Debug, Clone, Copy)]
enum Instruction {
    /// Execute an operator.
    ExecuteOperator {
        /// Operator to execute.
        operator_idx: usize,
        /// If this operator is the start of the pipeline.
        is_pipeline_start: bool,
    },
    /// Finalize operator at the given index.
    FinalizeOperator { operator_idx: usize },
}

/// Simple instruction stack for operator execution.
#[derive(Debug)]
pub struct ExecutionStack {
    /// Number of operators in this pipeline.
    num_operators: usize,
    /// Instruction stack.
    instructions: Vec<Instruction>,
}

impl ExecutionStack {
    pub fn try_new(num_operators: usize) -> Result<Self> {
        if num_operators == 0 {
            return Err(RayexecError::new(
                "Cannot create execution stack for zero operators",
            ));
        }

        // Initialize stack with single instruction to execute the first operator.
        let mut instructions = Vec::with_capacity(num_operators);
        instructions.push(Instruction::ExecuteOperator {
            operator_idx: 0,
            is_pipeline_start: true,
        });

        Ok(ExecutionStack {
            num_operators,
            instructions,
        })
    }

    /// Pops the next instruction in the stack, calling the appropriate method
    /// on `effects` depending on the instruction.
    ///
    /// The returned control flow enum tells the pipeline how to proceed.
    ///
    /// This will call `effects` based on the instruction we're currently
    /// working on.
    pub fn pop_next<H>(&mut self, effects: &mut H) -> Result<StackControlFlow>
    where
        H: StackEffectHandler,
    {
        let instr = match self.instructions.pop() {
            Some(instr) => instr,
            None => return Ok(StackControlFlow::Finished),
        };

        match instr {
            Instruction::ExecuteOperator {
                operator_idx,
                is_pipeline_start,
            } => {
                let poll = effects.handle_execute(operator_idx)?;
                match poll {
                    PollExecute::Ready => {
                        if is_pipeline_start {
                            // Keep instruction in stack, we'll be executing it
                            // again.
                            self.instructions.push(instr);
                        }

                        // Push instruction to execute next operator if there is one.
                        if operator_idx != self.num_operators - 1 {
                            self.instructions.push(Instruction::ExecuteOperator {
                                operator_idx: operator_idx + 1,
                                is_pipeline_start: false,
                            });
                        }

                        Ok(StackControlFlow::Continue)
                    }
                    PollExecute::Pending => {
                        // Push current instruction, we'll need to re-execute it
                        // again once woken.
                        self.instructions.push(instr);

                        Ok(StackControlFlow::Pending)
                    }
                    PollExecute::NeedsMore => {
                        // Do nothing, we'll want to pop previous instructions
                        // in order to get more batches.
                        Ok(StackControlFlow::Continue)
                    }
                    PollExecute::HasMore => {
                        // Push instruction to execute this operator again.
                        self.instructions.push(Instruction::ExecuteOperator {
                            operator_idx,
                            is_pipeline_start,
                        });

                        // And push instruction to execute next operator first.
                        if operator_idx != self.num_operators - 1 {
                            self.instructions.push(Instruction::ExecuteOperator {
                                operator_idx: operator_idx + 1,
                                is_pipeline_start: false,
                            });
                        } else {
                            return Err(RayexecError::new("Last operator returned HasMore"));
                        }

                        Ok(StackControlFlow::Continue)
                    }
                    PollExecute::Exhausted => {
                        // Clear all existing instructions.
                        self.instructions.clear();

                        if operator_idx == self.num_operators - 1 {
                            return Err(RayexecError::new("Last operator returned Exhausted"));
                        }

                        // Finalize next operator.
                        self.instructions.push(Instruction::FinalizeOperator {
                            operator_idx: operator_idx + 1,
                        });

                        // Execute next operator first.
                        self.instructions.push(Instruction::ExecuteOperator {
                            operator_idx: operator_idx + 1,
                            is_pipeline_start: false,
                        });

                        Ok(StackControlFlow::Continue)
                    }
                }
            }
            Instruction::FinalizeOperator { operator_idx } => {
                let poll = effects.handle_finalize(operator_idx)?;
                match poll {
                    PollFinalize::Finalized => {
                        if operator_idx == self.num_operators - 1 {
                            // We're done.
                            Ok(StackControlFlow::Finished)
                        } else {
                            // Finalize next operator.
                            self.instructions.push(Instruction::FinalizeOperator {
                                operator_idx: operator_idx + 1,
                            });

                            Ok(StackControlFlow::Continue)
                        }
                    }
                    PollFinalize::NeedsDrain => {
                        if operator_idx == self.num_operators - 1 {
                            return Err(RayexecError::new("Last operator returned NeedsDrain"));
                        }

                        // This operator is now the start of the pipeline.
                        self.instructions.push(Instruction::ExecuteOperator {
                            operator_idx,
                            is_pipeline_start: true,
                        });

                        Ok(StackControlFlow::Continue)
                    }
                    PollFinalize::Pending => {
                        // Try finalize again once woken.
                        self.instructions.push(instr);
                        Ok(StackControlFlow::Pending)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Stack effects handler that asserts expected operator indexes and the
    /// desired return value for influencing the stack.
    #[derive(Debug)]
    struct TestEffects {
        execute: Option<(usize, PollExecute)>,
        finalize: Option<(usize, PollFinalize)>,
    }

    impl TestEffects {
        fn execute(expected_idx: usize, poll: PollExecute) -> Self {
            TestEffects {
                execute: Some((expected_idx, poll)),
                finalize: None,
            }
        }

        fn finalize(expected_idx: usize, poll: PollFinalize) -> Self {
            TestEffects {
                execute: None,
                finalize: Some((expected_idx, poll)),
            }
        }
    }

    impl StackEffectHandler for TestEffects {
        fn handle_execute(&mut self, op_idx: usize) -> Result<PollExecute> {
            let (expected, poll) = self.execute.unwrap();
            assert_eq!(expected, op_idx);
            Ok(poll)
        }

        fn handle_finalize(&mut self, op_idx: usize) -> Result<PollFinalize> {
            let (expected, poll) = self.finalize.unwrap();
            assert_eq!(expected, op_idx);
            Ok(poll)
        }
    }

    /// Small wrapper to make the lines shorter in tests to make them easier to
    /// read.
    fn pop_next(
        stack: &mut ExecutionStack,
        mut effects: impl StackEffectHandler,
    ) -> StackControlFlow {
        stack.pop_next(&mut effects).unwrap()
    }

    #[test]
    fn stack_execution_resets() {
        let mut stack = ExecutionStack::try_new(3).unwrap();

        let out = pop_next(&mut stack, TestEffects::execute(0, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        let out = pop_next(&mut stack, TestEffects::execute(1, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        let out = pop_next(&mut stack, TestEffects::execute(2, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        // Should reset back to start.
        let out = pop_next(&mut stack, TestEffects::execute(0, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        let out = pop_next(&mut stack, TestEffects::execute(1, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);
    }

    #[test]
    fn stack_execution_pending() {
        let mut stack = ExecutionStack::try_new(2).unwrap();

        let out = pop_next(&mut stack, TestEffects::execute(0, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        let out = pop_next(&mut stack, TestEffects::execute(1, PollExecute::Pending));
        assert_eq!(out, StackControlFlow::Pending);

        // Should execute the same index.
        let out = pop_next(&mut stack, TestEffects::execute(1, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);
    }

    #[test]
    fn stack_execution_needs_more() {
        let mut stack = ExecutionStack::try_new(2).unwrap();

        let out = pop_next(&mut stack, TestEffects::execute(0, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        let out = pop_next(&mut stack, TestEffects::execute(1, PollExecute::NeedsMore));
        assert_eq!(out, StackControlFlow::Continue);

        // Should go back to start.
        let out = pop_next(&mut stack, TestEffects::execute(0, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        let out = pop_next(&mut stack, TestEffects::execute(1, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);
    }

    #[test]
    fn stack_execution_has_more() {
        let mut stack = ExecutionStack::try_new(3).unwrap();

        let out = pop_next(&mut stack, TestEffects::execute(0, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        let out = pop_next(&mut stack, TestEffects::execute(1, PollExecute::HasMore));
        assert_eq!(out, StackControlFlow::Continue);

        // Should move to next operator.
        let out = pop_next(&mut stack, TestEffects::execute(2, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        // But then reset back to operator that has more output.
        let out = pop_next(&mut stack, TestEffects::execute(1, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        // Move forward.
        let out = pop_next(&mut stack, TestEffects::execute(2, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        // Then go back to start.
        let out = pop_next(&mut stack, TestEffects::execute(0, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);
    }

    #[test]
    fn stack_execution_exhaust_first_finalize_last() {
        let mut stack = ExecutionStack::try_new(2).unwrap();

        let out = pop_next(&mut stack, TestEffects::execute(0, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);
        let out = pop_next(&mut stack, TestEffects::execute(1, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        // Exhaust first operator.
        let out = pop_next(&mut stack, TestEffects::execute(0, PollExecute::Exhausted));
        assert_eq!(out, StackControlFlow::Continue);

        // Execute remaining.
        let out = pop_next(&mut stack, TestEffects::execute(1, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        // Then finalize the next operator.
        let out = pop_next(
            &mut stack,
            TestEffects::finalize(1, PollFinalize::Finalized),
        );
        assert_eq!(out, StackControlFlow::Finished);
    }

    #[test]
    fn stack_execution_exhaust_first_finalize_second_then_last() {
        let mut stack = ExecutionStack::try_new(3).unwrap();

        let out = pop_next(&mut stack, TestEffects::execute(0, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);
        let out = pop_next(&mut stack, TestEffects::execute(1, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);
        let out = pop_next(&mut stack, TestEffects::execute(2, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        // Exhaust first operator.
        let out = pop_next(&mut stack, TestEffects::execute(0, PollExecute::Exhausted));
        assert_eq!(out, StackControlFlow::Continue);

        // Execute remaining.
        let out = pop_next(&mut stack, TestEffects::execute(1, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);
        let out = pop_next(&mut stack, TestEffects::execute(2, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        // Then finalize the second operator.
        let out = pop_next(
            &mut stack,
            TestEffects::finalize(1, PollFinalize::Finalized),
        );
        assert_eq!(out, StackControlFlow::Continue);

        // Then finalize last.
        let out = pop_next(
            &mut stack,
            TestEffects::finalize(2, PollFinalize::Finalized),
        );

        assert_eq!(out, StackControlFlow::Finished);
    }

    #[test]
    fn stack_execution_exhaust_first_needs_drain_second() {
        let mut stack = ExecutionStack::try_new(3).unwrap();

        let out = pop_next(&mut stack, TestEffects::execute(0, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);
        let out = pop_next(&mut stack, TestEffects::execute(1, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);
        let out = pop_next(&mut stack, TestEffects::execute(2, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        // Exhaust first operator.
        let out = pop_next(&mut stack, TestEffects::execute(0, PollExecute::Exhausted));
        assert_eq!(out, StackControlFlow::Continue);

        // Execute remaining in second.
        let out = pop_next(&mut stack, TestEffects::execute(1, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        // Execute remaining in last.
        let out = pop_next(&mut stack, TestEffects::execute(2, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        // Finalize second
        let out = pop_next(
            &mut stack,
            TestEffects::finalize(1, PollFinalize::NeedsDrain),
        );
        assert_eq!(out, StackControlFlow::Continue);

        // Drain second.
        let out = pop_next(&mut stack, TestEffects::execute(1, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        // Pass to last.
        let out = pop_next(&mut stack, TestEffects::execute(2, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        // Exhaust second.
        let out = pop_next(&mut stack, TestEffects::execute(1, PollExecute::Exhausted));
        assert_eq!(out, StackControlFlow::Continue);

        // Pass remaining to last.
        let out = pop_next(&mut stack, TestEffects::execute(2, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        // Finalize last.
        let out = pop_next(
            &mut stack,
            TestEffects::finalize(2, PollFinalize::Finalized),
        );
        assert_eq!(out, StackControlFlow::Finished);
    }

    #[test]
    fn stack_execution_multiple_has_more() {
        // Test that we can handle `poll_execute` returning `HasMore` from
        // multiple operators (e.g. nested joins).

        let mut stack = ExecutionStack::try_new(4).unwrap();

        let out = pop_next(&mut stack, TestEffects::execute(0, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        // First HasMore
        let out = pop_next(&mut stack, TestEffects::execute(1, PollExecute::HasMore));
        assert_eq!(out, StackControlFlow::Continue);

        // Second HasMore
        let out = pop_next(&mut stack, TestEffects::execute(2, PollExecute::HasMore));
        assert_eq!(out, StackControlFlow::Continue);

        // Push to last as normal.
        let out = pop_next(&mut stack, TestEffects::execute(3, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        // Poll second HasMore operator first.
        let out = pop_next(&mut stack, TestEffects::execute(2, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        // Push to last again.
        let out = pop_next(&mut stack, TestEffects::execute(3, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        // Poll first HasMore operator second.
        let out = pop_next(&mut stack, TestEffects::execute(1, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        // Push to parent operators as normal.
        let out = pop_next(&mut stack, TestEffects::execute(2, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);
        let out = pop_next(&mut stack, TestEffects::execute(3, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);
    }

    #[test]
    fn stack_execution_propagate_finalize_through_many() {
        let mut stack = ExecutionStack::try_new(4).unwrap();

        let out = pop_next(&mut stack, TestEffects::execute(0, PollExecute::Exhausted));
        assert_eq!(out, StackControlFlow::Continue);

        // Push through last three operators.
        let out = pop_next(&mut stack, TestEffects::execute(1, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);
        let out = pop_next(&mut stack, TestEffects::execute(2, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);
        let out = pop_next(&mut stack, TestEffects::execute(3, PollExecute::Ready));
        assert_eq!(out, StackControlFlow::Continue);

        // Finalize last three operators.
        let out = pop_next(
            &mut stack,
            TestEffects::finalize(1, PollFinalize::Finalized),
        );
        assert_eq!(out, StackControlFlow::Continue);
        let out = pop_next(
            &mut stack,
            TestEffects::finalize(2, PollFinalize::Finalized),
        );
        assert_eq!(out, StackControlFlow::Continue);
        let out = pop_next(
            &mut stack,
            TestEffects::finalize(3, PollFinalize::Finalized),
        );
        assert_eq!(out, StackControlFlow::Finished);
    }
}

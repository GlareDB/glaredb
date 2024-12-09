use futures::TryStreamExt;
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_parser::statement::RawStatement;

use super::result::ExecutionResult;
use super::session::Session;
use crate::runtime::{PipelineExecutor, Runtime};

/// Verify the results of a query.
#[derive(Debug, Clone)]
pub struct QueryVerifier {
    statement: RawStatement,
}

impl QueryVerifier {
    pub fn new(statement: RawStatement) -> Self {
        QueryVerifier { statement }
    }

    pub async fn verify<P, R>(
        self,
        session: &mut Session<P, R>,
        exec_result: ExecutionResult,
    ) -> Result<ExecutionResult>
    where
        P: PipelineExecutor,
        R: Runtime,
    {
        let config = session.config_mut();

        // Store variables so we can reset them after generating the unoptimized
        // plan.
        let enable_optimizer = config.enable_optimizer;
        let verify_optimized = config.verify_optimized_plan;

        config.enable_optimizer = false;
        config.verify_optimized_plan = false;

        const VERIFIED_NAME: &str = "__verify_optimized_plan";

        session.prepare(VERIFIED_NAME, self.statement.clone())?;
        session.bind(VERIFIED_NAME, VERIFIED_NAME).await?;

        let result = session.execute(VERIFIED_NAME).await;

        // Reset variables before returning any results.
        let config = session.config_mut();
        config.enable_optimizer = enable_optimizer;
        config.verify_optimized_plan = verify_optimized;

        // Session should not be used after this point.
        //
        // TODO: Can we enforce that? Currently we need to shim into the current
        // flow.

        let unoptimized_exec_result = result?;

        if exec_result.output_schema != unoptimized_exec_result.output_schema {
            return Err(RayexecError::new(format!(
                "Schemas don't match. Original: {:?}, unoptimized: {:?}",
                exec_result.output_schema, unoptimized_exec_result.output_schema,
            )));
        }

        let original_result: Result<Vec<_>> = exec_result.stream.try_collect().await;
        let unoptimized_result: Result<Vec<_>> = unoptimized_exec_result.stream.try_collect().await;

        match (original_result, unoptimized_result) {
            (Ok(_), Err(e)) => {
                return Err(RayexecError::new(format!(
                    "Original stream returned Ok, unoptimized stream returned error: {e}"
                )))
            }
            (Err(e), Ok(_)) => {
                return Err(RayexecError::new(format!(
                    "Unoptimized stream returned Ok, original stream returned error: {e}"
                )))
            }
            (Err(orig), Err(unopt)) => {
                if orig.get_msg() != unopt.get_msg() {
                    return Err(RayexecError::new(format!(
                        "Errors returned are different, original: {}, unoptimized: {}",
                        orig.get_msg(),
                        unopt.get_msg()
                    )));
                }
            }
            _ => (),
        }

        // TODO: Need to move the materialized table in and allow equality
        // checking.
        not_implemented!("query verification")
    }
}

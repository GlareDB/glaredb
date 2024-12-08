use rayexec_error::{RayexecError, Result};
use rayexec_parser::statement::RawStatement;

use super::session::Session;
use crate::execution::intermediate::{IntermediateMaterializationGroup, IntermediatePipelineGroup};
use crate::logical::binder::bind_context::BindContext;
use crate::logical::operator::LogicalOperator;
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

    pub fn verify<P, R>(self, session: &mut Session<P, R>) -> Result<()>
    where
        P: PipelineExecutor,
        R: Runtime,
    {
        session.prepare("__query_verification", self.statement.clone())?;

        unimplemented!()
    }
}

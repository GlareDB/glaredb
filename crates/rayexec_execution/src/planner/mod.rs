pub mod explainable;
pub mod expr;
pub mod operator;
pub mod plan;
pub mod scope;

use crate::{
    engine::vars::SessionVar,
    functions::{aggregate::GenericAggregateFunction, scalar::GenericScalarFunction},
};
use rayexec_error::Result;
use rayexec_parser::ast;

pub trait Resolver: std::fmt::Debug {
    fn resolve_scalar_function(
        &self,
        reference: &ast::ObjectReference,
    ) -> Option<Box<dyn GenericScalarFunction>>;

    fn resolve_aggregate_function(
        &self,
        reference: &ast::ObjectReference,
    ) -> Option<Box<dyn GenericAggregateFunction>>;

    fn get_session_variable(&self, name: &str) -> Result<SessionVar>;
}

#[derive(Debug)]
pub struct BindContext {}

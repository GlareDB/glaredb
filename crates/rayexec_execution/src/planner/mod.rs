pub mod explainable;
pub mod expr;
pub mod operator;
pub mod plan;
pub mod scope;

use crate::functions::table::TableFunction;
use rayexec_error::Result;
use rayexec_parser::ast;

pub trait Resolver: std::fmt::Debug {
    fn resolve_table_function(
        &self,
        reference: &ast::ObjectReference,
    ) -> Result<Box<dyn TableFunction>>;
}

#[derive(Debug)]
pub struct BindContext {}

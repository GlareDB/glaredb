use rayexec_bullet::datatype::DataType;
use rayexec_error::Result;
use rayexec_parser::ast;

use crate::logical::{
    binder::bind_query::QueryBinder,
    logical_explain::ExplainFormat,
    resolver::{resolve_context::ResolveContext, ResolvedMeta},
};

use super::{
    bind_context::{BindContext, BindScopeRef},
    bind_query::BoundQuery,
};

#[derive(Debug)]
pub struct BoundExplain {
    // TODO: Allow things other than just queries (e.g. create table)
    pub query: BoundQuery,
    pub format: ExplainFormat,
    pub verbose: bool,
    pub analyze: bool,
}

#[derive(Debug)]
pub struct ExplainBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> ExplainBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        ExplainBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind_explain(
        &self,
        bind_context: &mut BindContext,
        explain: ast::ExplainNode<ResolvedMeta>,
    ) -> Result<BoundExplain> {
        // TODO: Allow other inputs to the explain.
        let query = match explain.body {
            ast::ExplainBody::Query(query) => {
                let source_scope = bind_context.new_orphan_scope();
                let query_binder = QueryBinder::new(source_scope, self.resolve_context);
                query_binder.bind(bind_context, query)?
            }
        };

        let format = match explain.output {
            Some(ast::ExplainOutput::Text) => ExplainFormat::Text,
            Some(ast::ExplainOutput::Json) => ExplainFormat::Json,
            None => ExplainFormat::Text,
        };

        // Note this is done after the child planning to ensure consistent table
        // refs when running a query with and explain and without an explain
        // since this creates a table ref for the explain output.
        bind_context.push_table(
            self.current,
            None,
            vec![DataType::Utf8, DataType::Utf8],
            vec!["plan_type".to_string(), "plan".to_string()],
        )?;

        Ok(BoundExplain {
            query,
            format,
            verbose: explain.verbose,
            analyze: explain.analyze,
        })
    }
}

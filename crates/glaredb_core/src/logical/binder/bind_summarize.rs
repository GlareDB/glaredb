use glaredb_error::Result;
use glaredb_parser::ast;

use super::bind_context::{BindContext, BindScopeRef};
use super::bind_query::BoundQuery;
use super::table_list::TableType;
use crate::arrays::datatype::DataType;
use crate::arrays::field::{ColumnSchema, Field};
use crate::logical::binder::bind_query::QueryBinder;
use crate::logical::binder::bind_query::bind_from::FromBinder;
use crate::logical::logical_describe::LogicalDescribe;
use crate::logical::operator::{LocationRequirement, Node};
use crate::logical::resolver::ResolvedMeta;
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::statistics::value::StatisticsValue;

#[derive(Debug)]
pub struct SummarizeBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> SummarizeBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        SummarizeBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind_summarize(
        &self,
        bind_context: &mut BindContext,
        summarize: ast::Summarize<ResolvedMeta>,
    ) -> Result<BoundQuery> {
        unimplemented!()
    }
}

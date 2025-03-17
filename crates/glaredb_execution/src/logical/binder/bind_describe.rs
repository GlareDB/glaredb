use glaredb_error::Result;
use glaredb_parser::ast;

use super::bind_context::{BindContext, BindScopeRef};
use crate::arrays::datatype::DataType;
use crate::arrays::field::{ColumnSchema, Field};
use crate::logical::binder::bind_query::QueryBinder;
use crate::logical::binder::bind_query::bind_from::FromBinder;
use crate::logical::logical_describe::LogicalDescribe;
use crate::logical::operator::{LocationRequirement, Node};
use crate::logical::resolver::ResolvedMeta;
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::statistics::StatisticsValue;

#[derive(Debug)]
pub struct DescribeBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> DescribeBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        DescribeBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind_describe(
        &self,
        bind_context: &mut BindContext,
        describe: ast::Describe<ResolvedMeta>,
    ) -> Result<Node<LogicalDescribe>> {
        let table_ref = bind_context.push_table(
            self.current,
            None,
            vec![DataType::Utf8, DataType::Utf8],
            vec!["column_name".to_string(), "datatype".to_string()],
        )?;

        let query_scope = bind_context.new_orphan_scope();

        // We don't care about the results of the bind, just the changes it
        // makes to the bind context (columns).
        match describe {
            ast::Describe::Query(query) => {
                let _ = QueryBinder::new(query_scope, self.resolve_context)
                    .bind(bind_context, query)?;
            }
            ast::Describe::FromNode(from) => {
                let _ = FromBinder::new(query_scope, self.resolve_context)
                    .bind(bind_context, Some(from))?;
            }
        }

        let fields = bind_context
            .iter_tables_in_scope(query_scope)?
            .flat_map(|t| {
                t.column_names
                    .iter()
                    .zip(&t.column_types)
                    .map(|(name, datatype)| Field::new(name, datatype.clone(), true))
            });

        Ok(Node {
            node: LogicalDescribe {
                schema: ColumnSchema::new(fields),
                table_ref,
            },
            location: LocationRequirement::Any,
            children: Vec::new(),
            estimated_cardinality: StatisticsValue::Unknown,
        })
    }
}

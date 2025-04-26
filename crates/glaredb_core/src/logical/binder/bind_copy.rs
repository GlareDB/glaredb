use glaredb_error::{Result, not_implemented};
use glaredb_parser::ast;

use super::bind_context::{BindContext, BindScopeRef};
use super::bind_query::BoundQuery;
use super::bind_query::bind_from::BoundFrom;
use crate::arrays::datatype::DataType;
use crate::arrays::field::{ColumnSchema, Field};
use crate::logical::binder::bind_query::QueryBinder;
use crate::logical::binder::bind_query::bind_from::FromBinder;
use crate::logical::resolver::ResolvedMeta;
use crate::logical::resolver::resolve_context::ResolveContext;

#[derive(Debug)]
pub enum BoundCopyToSource {
    Query(BoundQuery),
    Table(BoundFrom), // TODO: Optionally specify columns to copy
}

#[derive(Debug)]
pub struct BoundCopyTo {
    pub source: BoundCopyToSource,
    pub source_schema: ColumnSchema,
    // pub location: FileLocation,
    // pub copy_to: Box<dyn CopyToFunction>,
}

#[derive(Debug)]
pub struct CopyBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> CopyBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        CopyBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind_copy_to(
        &self,
        bind_context: &mut BindContext,
        copy_to: ast::CopyTo<ResolvedMeta>,
    ) -> Result<BoundCopyTo> {
        bind_context.push_table(self.current, None, vec![DataType::UInt64], ["rows_copied"])?;

        let source_scope = bind_context.new_orphan_scope();

        let _source = match copy_to.source {
            ast::CopyToSource::Query(query) => {
                let query_binder = QueryBinder::new(source_scope, self.resolve_context);
                let bound_query = query_binder.bind(bind_context, query)?;

                BoundCopyToSource::Query(bound_query)
            }
            ast::CopyToSource::Table(table) => {
                let from_binder = FromBinder::new(source_scope, self.resolve_context);
                let bound_from = from_binder.bind_table(
                    bind_context,
                    ast::FromBaseTable { reference: table },
                    None,
                )?;

                BoundCopyToSource::Table(bound_from)
            }
        };

        let _source_schema = ColumnSchema::new(
            bind_context
                .iter_tables_in_scope(source_scope)?
                .flat_map(|t| {
                    t.column_names
                        .iter()
                        .zip(&t.column_types)
                        .map(|(name, datatype)| Field::new(name.as_str(), datatype.clone(), true))
                }),
        );

        // let resolved_copy_to = self
        //     .resolve_context
        //     .copy_to
        //     .as_ref()
        //     .ok_or_else(|| RayexecError::new("Missing COPY TO function"))?
        //     .clone();

        not_implemented!("Bind COPY TO")
        // Ok(BoundCopyTo {
        //     source,
        //     source_schema,
        //     location: copy_to.target,
        //     copy_to: resolved_copy_to.func,
        // })
    }
}

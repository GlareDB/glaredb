use glaredb_error::{RayexecError, Result};
use glaredb_parser::ast;

use super::bind_context::{BindContext, BindScopeRef};
use crate::catalog::create::OnConflict;
use crate::logical::binder::bind_query::QueryBinder;
use crate::logical::logical_create::LogicalCreateView;
use crate::logical::operator::{LocationRequirement, Node};
use crate::logical::resolver::ResolvedMeta;
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::statistics::StatisticsValue;

pub struct CreateViewBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> CreateViewBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        CreateViewBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind_create_view(
        &self,
        bind_context: &mut BindContext,
        mut create: ast::CreateView<ResolvedMeta>,
    ) -> Result<Node<LogicalCreateView>> {
        let on_conflict = if create.or_replace {
            OnConflict::Replace
        } else {
            OnConflict::Error
        };

        let column_aliases = create.column_aliases.map(|aliases| {
            aliases
                .into_iter()
                .map(|alias| alias.into_normalized_string())
                .collect::<Vec<_>>()
        });

        // We do query binding here just to catch any errors with the view
        // definition at this point in time.
        //
        // This does not mean the view remains valid forever given that
        // underlying data sources may change. We could alleviate that with
        // dependency references to other tables in _our_ database, but
        // everything goes out the window with remote data sources.
        let source_scope = bind_context.new_orphan_scope();
        let query = QueryBinder::new(source_scope, self.resolve_context)
            .bind(bind_context, create.query)?;

        if let Some(column_aliases) = &column_aliases {
            let table = bind_context.get_table(query.output_table_ref())?;
            if column_aliases.len() > table.num_columns() {
                return Err(RayexecError::new(format!(
                    "Expected at most {} column aliases for view, got {}",
                    table.num_columns(),
                    column_aliases.len()
                )));
            }
        }

        let [catalog, schema, name] = create.name.pop_3()?;

        Ok(Node {
            node: LogicalCreateView {
                catalog,
                schema,
                name,
                column_aliases,
                on_conflict,
                query_string: create.query_sql,
            },
            location: LocationRequirement::ClientLocal,
            children: Vec::new(),
            estimated_cardinality: StatisticsValue::Unknown,
        })
    }
}

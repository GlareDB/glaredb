use glaredb_error::{DbError, Result};
use glaredb_parser::ast;

use super::bind_context::{BindContext, BindScopeRef};
use super::bind_query::BoundQuery;
use super::table_list::TableRef;
use crate::arrays::datatype::DataType;
use crate::expr::column_expr::{ColumnExpr, ColumnReference};
use crate::expr::{Expression, cast};
use crate::logical::binder::bind_query::QueryBinder;
use crate::logical::operator::LocationRequirement;
use crate::logical::resolver::ResolvedMeta;
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::resolver::resolved_table::{
    ResolvedTableOrCteReference,
    ResolvedTableReference,
};

#[derive(Debug, Clone, PartialEq)]
pub struct InsertProjections {
    pub projections: Vec<Expression>,
    pub projection_table: TableRef,
}

#[derive(Debug, Clone)]
pub struct BoundInsert {
    /// Source of the insert.
    pub source: BoundQuery,
    /// Table we're inserting into.
    pub table: ResolvedTableReference,
    /// Location of destination table.
    pub table_location: LocationRequirement,
    /// Optional projections to apply to the source.
    ///
    /// None if no casts are needed.
    pub projections: Option<InsertProjections>,
}

#[derive(Debug)]
pub struct InsertBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> InsertBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        InsertBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind_insert(
        &self,
        bind_context: &mut BindContext,
        insert: ast::Insert<ResolvedMeta>,
    ) -> Result<BoundInsert> {
        bind_context.push_table(
            self.current,
            None,
            vec![DataType::Int64],
            vec!["rows_inserted".to_string()],
        )?;

        let source_scope = bind_context.new_orphan_scope();

        let query_binder = QueryBinder::new(source_scope, self.resolve_context);
        let bound_query = query_binder.bind(bind_context, insert.source)?;

        let (reference, location) = match self.resolve_context.tables.try_get_bound(insert.table)? {
            (ResolvedTableOrCteReference::Table(reference), location) => (reference, location),
            (ResolvedTableOrCteReference::Cte { .. }, _) => {
                // Shouldn't be possible.
                return Err(DbError::new("Cannot insert into CTE"));
            }
            (ResolvedTableOrCteReference::View(_), _) => {
                // Also shouldn't be possible.
                return Err(DbError::new("View should have been inlined during resolve"));
            }
        };

        // TODO: Handle specified columns. If provided, insert a projection that
        // maps the columns to the right position.
        //
        // Currently assumes we're inserting by position.

        // Check types, determine appropriate casts.
        let table_types = reference
            .entry
            .try_as_table_entry()?
            .columns
            .iter()
            .map(|c| &c.datatype);

        // Types from the source plan.
        let source_types: Vec<(TableRef, usize, &DataType)> = bind_context
            .iter_tables_in_scope(source_scope)?
            .flat_map(|t| {
                let table_ref = t.reference;
                t.column_types
                    .iter()
                    .enumerate()
                    .map(move |(col_idx, col_type)| (table_ref, col_idx, col_type))
            })
            .collect();

        if table_types.len() != source_types.len() {
            return Err(DbError::new(format!(
                "Invalid number of inputs. Expected {}, got {}",
                table_types.len(),
                source_types.len(),
            )));
        }

        let mut has_cast = false;
        let mut projections = Vec::with_capacity(source_types.len());

        for (have, want) in source_types.into_iter().zip(table_types) {
            let mut expr = Expression::Column(ColumnExpr {
                reference: ColumnReference {
                    table_scope: have.0,
                    column: have.1,
                },
                datatype: have.2.clone(),
            });

            if have.2 != want {
                expr = cast(expr, want.clone()).into();
                has_cast = true;
            }

            projections.push(expr);
        }

        // Only use projections if there's a cast.
        let projections = if has_cast {
            let projection_table = bind_context.new_ephemeral_table_with_columns(
                projections
                    .iter()
                    .map(|p| p.datatype())
                    .collect::<Result<Vec<_>>>()?,
                (0..projections.len())
                    .map(|idx| format!("__generated_insert_project_{idx}"))
                    .collect(),
            )?;

            Some(InsertProjections {
                projections,
                projection_table,
            })
        } else {
            None
        };

        Ok(BoundInsert {
            source: bound_query,
            table: reference.clone(),
            table_location: location,
            projections,
        })
    }
}

use glaredb_error::{DbError, Result};
use glaredb_parser::ast;

use super::bind_context::{BindContext, BindScopeRef};
use super::bind_query::BoundQuery;
use crate::arrays::datatype::DataType;
use crate::arrays::field::Field;
use crate::catalog::create::OnConflict;
use crate::logical::binder::bind_query::QueryBinder;
use crate::logical::resolver::ResolvedMeta;
use crate::logical::resolver::resolve_context::ResolveContext;

#[derive(Debug)]
pub struct BoundCreateTable {
    pub catalog: String,
    pub schema: String,
    pub name: String,
    pub columns: Vec<Field>,
    pub on_conflict: OnConflict,
    pub source: Option<BoundQuery>,
}

#[derive(Debug)]
pub struct CreateTableBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> CreateTableBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        CreateTableBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind_create_table(
        &self,
        bind_context: &mut BindContext,
        mut create: ast::CreateTable<ResolvedMeta>,
    ) -> Result<BoundCreateTable> {
        let on_conflict = match (create.or_replace, create.if_not_exists) {
            (true, false) => OnConflict::Replace,
            (false, true) => OnConflict::Ignore,
            (false, false) => OnConflict::Error,
            (true, true) => {
                return Err(DbError::new(
                    "Cannot specify both OR REPLACE and IF NOT EXISTS",
                ));
            }
        };

        // TODO: Verify column constraints.
        let mut columns: Vec<_> = create
            .columns
            .into_iter()
            .map(|col| Field::new(col.name.value, col.datatype, true))
            .collect();

        // TODO: Check if all columns have unique names, ignoring case.

        let input = match create.source {
            Some(source) => {
                // Return number of rows inserted for the table.
                bind_context.push_table(
                    self.current,
                    None,
                    [DataType::int64()],
                    ["rows_inserted"],
                )?;

                // If we have an input to the table, adjust the column definitions for the table
                // to be the output schema of the input.

                // TODO: We could allow this though. We'd just need to do some
                // projections as necessary.
                if !columns.is_empty() {
                    return Err(DbError::new(
                        "Cannot specify columns when running CREATE TABLE ... AS ...",
                    ));
                }

                let source_scope = bind_context.new_orphan_scope();
                let query_binder = QueryBinder::new(source_scope, self.resolve_context);
                let bound_query = query_binder.bind(bind_context, source)?;

                let fields: Vec<_> = bind_context
                    .iter_tables_in_scope(source_scope)?
                    .flat_map(|t| {
                        t.column_names
                            .iter()
                            .zip(&t.column_types)
                            .map(|(name, datatype)| {
                                Field::new(name.as_raw_str(), datatype.clone(), true)
                            })
                    })
                    .collect();

                // Update columns to the fields we've generated from the input.
                columns = fields;

                Some(bound_query)
            }
            None => None,
        };

        let [catalog, schema, name] = create.name.pop_3()?;

        Ok(BoundCreateTable {
            catalog,
            schema,
            name,
            columns,
            on_conflict,
            source: input,
        })
    }
}

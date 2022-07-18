use crate::catalog::{
    CatalogId, CatalogReader, CatalogWriter, SchemaId, TableReference, TableSchema,
};
use anyhow::{anyhow, Result};
use lemur::execute::stream::source::{ReadableSource, WriteableSource};
use sqlparser::ast;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

#[derive(Debug)]
pub struct Engine {}

impl Engine {
    pub fn begin_session(&self) -> Result<Session> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct Session {}

impl Session {
    pub fn execute_query(&self, query: &str) -> Result<()> {
        let statements = Parser::parse_sql(&PostgreSqlDialect {}, query)?;

        for statement in statements.into_iter() {}

        unimplemented!()
    }
}

pub struct Executor<W> {
    source: W,
}

impl<W: WriteableSource> Executor<W> {
    fn execute(statements: Vec<ast::Statement>) -> Result<()> {
        unimplemented!()
    }
}

impl<W: WriteableSource> CatalogReader for Executor<W> {
    fn get_table(&self, reference: &TableReference) -> Result<Option<TableSchema>> {
        todo!()
    }

    fn get_table_by_name(
        &self,
        catalog: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<(TableReference, TableSchema)>> {
        todo!()
    }

    fn current_catalog(&self) -> (CatalogId, &str) {
        (0, "catalog")
    }

    fn current_schema(&self) -> (SchemaId, &str) {
        (0, "schema")
    }
}

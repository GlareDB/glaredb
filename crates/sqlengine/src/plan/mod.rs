mod expr;
pub mod planner;
pub mod read;
pub mod rewrite;
pub mod write;
pub mod data_definition;

use crate::catalog::CatalogReader;
use planner::Planner;
use read::ReadPlan;
use rewrite::FilterPushdown;
use write::WritePlan;

use anyhow::Result;
use sqlparser::ast;

use self::data_definition::DataDefinitionPlan;

#[derive(Debug)]
pub enum QueryPlan {
    Read(ReadPlan),
    Write(WritePlan),
    DataDefinition(DataDefinitionPlan),
}

impl QueryPlan {
    pub fn plan<C>(stmt: ast::Statement, catalog: &C) -> Result<QueryPlan>
    where
        C: CatalogReader,
    {
        Planner::new(catalog).plan_statement(stmt)
    }

    pub fn rewrite(&mut self) -> Result<()> {
        let read_plan = match self {
            QueryPlan::Read(read) => read,
            QueryPlan::Write(write) => match write.get_read_mut() {
                Some(read) => read,
                None => return Ok(()),
            },
            QueryPlan::DataDefinition(_) => return Ok(()),
        };

        FilterPushdown.rewrite(read_plan)?;

        Ok(())
    }
}

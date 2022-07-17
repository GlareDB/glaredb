mod expr;
pub mod planner;
pub mod read;
pub mod rewrite;
pub mod write;

use crate::catalog::CatalogReader;
use planner::Planner;
use read::ReadPlan;
use rewrite::FilterPushdown;
use write::WritePlan;

use anyhow::Result;
use sqlparser::ast;

#[derive(Debug)]
pub enum QueryPlan {
    Read(ReadPlan),
    Write(WritePlan),
}

impl QueryPlan {
    pub fn plan<'a, C>(stmt: ast::Statement, catalog: &'a C) -> Result<QueryPlan>
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
        };

        FilterPushdown.rewrite(read_plan)?;

        Ok(())
    }
}

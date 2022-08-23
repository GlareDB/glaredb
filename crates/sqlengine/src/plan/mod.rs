pub mod data_definition;
mod expr;
pub mod planner;
pub mod read;
pub mod rewrite;
mod scope;
pub mod write;

use crate::catalog::CatalogReader;
use anyhow::{anyhow, Result};
use lemur::repr::expr::ScalarExpr;
use serde::{Deserialize, Serialize};
use sqlparser::ast;

use data_definition::DataDefinitionPlan;
use planner::Planner;
use read::ReadPlan;
use rewrite::FilterPushdown;
use scope::Scope;
use write::WritePlan;

/// The output description for a read plan.
// TODO: This may need be moved around. We're missing type information which
// pgsrv will eventually need.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Description {
    pub columns: Vec<String>,
}

impl Description {
    /// Create a new empty description.
    pub fn empty() -> Description {
        Description {
            columns: Vec::new(),
        }
    }

    /// Get the output description of a read plan using the scope that was used
    /// during planning.
    ///
    /// Errors if the top-level plan node is not a projection.
    pub fn from_read_plan_and_scope(plan: &ReadPlan, scope: &Scope) -> Result<Description> {
        let proj = match plan {
            ReadPlan::Project(proj) => proj,
            other => {
                return Err(anyhow!(
                    "cannot build description from non-projection root: {:?}",
                    other
                ))
            }
        };

        let mut names = Vec::with_capacity(proj.columns.len());
        for col in proj.columns.iter() {
            match col {
                ScalarExpr::Column(idx) => {
                    let (_, label) = scope.get_column(*idx)?;
                    match label {
                        Some(label) => names.push(label.to_string()),
                        None => names.push("?".to_string()),
                    }
                }
                _ => names.push("?".to_string()),
            }
        }

        Ok(Description { columns: names })
    }
}

#[derive(Debug)]
pub enum QueryPlan {
    Read { plan: ReadPlan, desc: Description },
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
            QueryPlan::Read { plan, .. } => plan,
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

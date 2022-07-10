use crate::engine::Transaction;
use crate::logical::{JoinOperator, RelationalPlan};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use coretypes::stream::BatchStream;
use diststore::engine::StorageTransaction;
use std::sync::Arc;

pub mod query;
pub use query::*;
pub mod mutation;
pub use mutation::*;
mod join;
use join::*;
mod spill;

#[async_trait]
pub trait PhysicalOperator<T: Transaction> {
    /// Execute the operator, optionally returning a stream for results.
    ///
    /// Node reordering and optimization must happen before a call to this
    /// method as building the stream may require some upfront execution.
    // TODO: Make this return more relevant results for mutations.
    async fn execute_stream(self, tx: &mut T) -> Result<Option<BatchStream>>;
}

#[derive(Debug)]
pub enum PhysicalPlan {
    Scan(Scan),
    Values(Values),
    Filter(Filter),
    NestedLoopJoin(NestedLoopJoin),
    Project(Project),

    CreateTable(CreateTable),
    Insert(Insert),
    Nothing(Nothing),
}

impl PhysicalPlan {
    pub fn from_logical(logical: RelationalPlan) -> Result<PhysicalPlan> {
        Ok(match logical {
            RelationalPlan::Scan(node) => PhysicalPlan::Scan(Scan {
                table: node.table,
                project: node.project,
                filter: node.filter,
            }),
            RelationalPlan::Values(node) => PhysicalPlan::Values(Values {
                schema: node.schema,
                values: node.values,
            }),
            RelationalPlan::Filter(node) => PhysicalPlan::Filter(Filter {
                predicate: node.predicate,
                input: Box::new(Self::from_logical(*node.input)?),
            }),
            RelationalPlan::Join(node) => {
                let predicate = match node.operator {
                    JoinOperator::On(expr) => expr,
                };
                PhysicalPlan::NestedLoopJoin(NestedLoopJoin {
                    join_type: node.join_type,
                    predicate,
                    left: Box::new(Self::from_logical(*node.left)?),
                    right: Box::new(Self::from_logical(*node.right)?),
                })
            }
            RelationalPlan::Project(node) => PhysicalPlan::Project(Project {
                expressions: node.expressions,
                input: Box::new(Self::from_logical(*node.input)?),
            }),
            RelationalPlan::CreateTable(node) => {
                PhysicalPlan::CreateTable(CreateTable { table: node.table })
            }
            RelationalPlan::Insert(node) => PhysicalPlan::Insert(Insert {
                table: node.table,
                input: Box::new(Self::from_logical(*node.input)?),
            }),
            RelationalPlan::Nothing => PhysicalPlan::Nothing(Nothing),
            other => return Err(anyhow!("unsupported logical node: {:?}", other)),
        })
    }

    pub async fn execute_stream<T>(self, tx: &mut T) -> Result<Option<BatchStream>>
    where
        T: Transaction + 'static,
    {
        Ok(match self {
            PhysicalPlan::Scan(node) => node.execute_stream(tx).await?,
            PhysicalPlan::Values(node) => node.execute_stream(tx).await?,
            PhysicalPlan::Filter(node) => node.execute_stream(tx).await?,
            PhysicalPlan::NestedLoopJoin(node) => node.execute_stream(tx).await?,
            PhysicalPlan::Project(node) => node.execute_stream(tx).await?,
            PhysicalPlan::CreateTable(node) => node.execute_stream(tx).await?,
            PhysicalPlan::Insert(node) => node.execute_stream(tx).await?,
            PhysicalPlan::Nothing(node) => node.execute_stream(tx).await?,
            _ => return Err(anyhow!("unimplemented physical node")),
        })
    }
}

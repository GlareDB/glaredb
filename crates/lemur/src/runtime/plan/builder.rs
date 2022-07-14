use super::{read::*, write::*};
use crate::repr::df::groupby::Accumulator;
use crate::repr::df::Schema;
use crate::repr::expr::ScalarExpr;
use crate::repr::value::Row;
use crate::runtime::datasource::TableKey;
use anyhow::{anyhow, Result};

#[derive(Debug)]
enum BuildPlan {
    None,
    Read(ReadPlan),
    Write(WritePlan),
}

#[derive(Debug)]
pub struct Builder {
    plan: BuildPlan,
}

impl Builder {
    pub fn new() -> Self {
        Builder {
            plan: BuildPlan::None,
        }
    }

    fn with_read_plan(plan: ReadPlan) -> Self {
        Builder {
            plan: BuildPlan::Read(plan),
        }
    }

    fn with_write_plan(plan: WritePlan) -> Self {
        Builder {
            plan: BuildPlan::Write(plan),
        }
    }

    pub fn into_read_plan(self) -> Result<ReadPlan> {
        match self.plan {
            BuildPlan::Read(plan) => Ok(plan),
            other => Err(anyhow!("not a read plan: {:?}", other)),
        }
    }

    pub fn into_write_plan(self) -> Result<WritePlan> {
        match self.plan {
            BuildPlan::Write(plan) => Ok(plan),
            other => Err(anyhow!("not a write plan: {:?}", other)),
        }
    }

    pub fn insert(self, table: TableKey) -> Result<Self> {
        match self.plan {
            BuildPlan::Read(plan) => Ok(Self::with_write_plan(WritePlan::Insert(Insert {
                table,
                input: plan,
            }))),
            _ => Err(anyhow!("invalid build plan for insert")),
        }
    }

    pub fn drop_table(self, table: TableKey) -> Result<Self> {
        match self.plan {
            BuildPlan::None => Ok(Self::with_write_plan(WritePlan::DropTable(DropTable {
                table,
            }))),
            _ => Err(anyhow!("invalid build plan for drop table")),
        }
    }

    pub fn create_table(self, table: TableKey, schema: Schema) -> Result<Self> {
        match self.plan {
            BuildPlan::None => Ok(Self::with_write_plan(WritePlan::CreateTable(CreateTable {
                table,
                schema,
            }))),
            _ => Err(anyhow!("invalid build plan for create table")),
        }
    }

    pub fn debug(self) -> Result<Self> {
        match self.plan {
            BuildPlan::Read(plan) => Ok(Self::with_read_plan(ReadPlan::Debug(Debug {
                input: Box::new(plan),
            }))),
            _ => Err(anyhow!("invalid build plan for debug")),
        }
    }

    pub fn aggregate(
        self,
        group_columns: Option<Vec<usize>>,
        accumulators: Vec<Accumulator>,
    ) -> Result<Self> {
        match self.plan {
            BuildPlan::Read(plan) => Ok(Self::with_read_plan(ReadPlan::Aggregate(Aggregate {
                group_columns,
                accumulators,
                input: Box::new(plan),
            }))),
            _ => Err(anyhow!("invalid build plan for aggregate")),
        }
    }

    pub fn order_by_group_by(self, columns: Vec<usize>) -> Result<Self> {
        match self.plan {
            BuildPlan::Read(plan) => Ok(Self::with_read_plan(ReadPlan::OrderByGroupBy(
                OrderByGroupBy {
                    columns,
                    input: Box::new(plan),
                },
            ))),
            _ => Err(anyhow!("invalid build plan for order by group by")),
        }
    }

    pub fn cross_join(self, other: Self) -> Result<Self> {
        match (self.plan, other.plan) {
            (BuildPlan::Read(left), BuildPlan::Read(right)) => {
                Ok(Self::with_read_plan(ReadPlan::CrossJoin(CrossJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                })))
            }
            _ => Err(anyhow!("invalid build plan for cross join")),
        }
    }

    pub fn project(self, columns: Vec<ScalarExpr>) -> Result<Self> {
        match self.plan {
            BuildPlan::Read(plan) => Ok(Self::with_read_plan(ReadPlan::Project(Project {
                columns,
                input: Box::new(plan),
            }))),
            _ => Err(anyhow!("invalid build plan for project")),
        }
    }

    pub fn filter(self, predicate: ScalarExpr) -> Result<Self> {
        match self.plan {
            BuildPlan::Read(plan) => Ok(Self::with_read_plan(ReadPlan::Filter(Filter {
                predicate,
                input: Box::new(plan),
            }))),
            _ => Err(anyhow!("invalid build plan for filter")),
        }
    }

    pub fn scan_source(self, table: TableKey, filter: Option<ScalarExpr>) -> Result<Self> {
        match self.plan {
            BuildPlan::None => Ok(Self::with_read_plan(ReadPlan::ScanSource(ScanSource {
                table,
                filter,
            }))),
            _ => Err(anyhow!("invalid build plan for scan source")),
        }
    }

    pub fn values(self, rows: Vec<Row>) -> Result<Self> {
        match self.plan {
            BuildPlan::None => Ok(Self::with_read_plan(ReadPlan::Values(Values { rows }))),
            _ => Err(anyhow!("invalid build plan for values")),
        }
    }
}

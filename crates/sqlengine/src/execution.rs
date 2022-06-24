use crate::catalog::ResolvedTableReference;
use coretypes::{
    datatype::{DataType, DataValue, NullableType, RelationSchema},
    expr::ScalarExpr,
};
use diststore::client::{BatchStream, Client};
use diststore::StoreError;

#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error("internal: {0}")]
    Internal(String),
    #[error(transparent)]
    StoreError(#[from] StoreError),
}

type Result<T, E = ExecutionError> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum PhysicalPlan {
    Scan(Scan),
}

#[derive(Debug)]
pub struct Scan {
    pub table: ResolvedTableReference,
    pub projected_schema: RelationSchema,
    pub project: Option<Vec<usize>>,
    pub filter: Option<ScalarExpr>,
}

impl Scan {
    pub async fn execute<C>(self, client: &C) -> Result<BatchStream>
    where
        C: Client,
    {
        let tbl = self.table.to_string();
        let stream = client.scan(&tbl, self.filter, 10).await?;
        Ok(stream)
    }
}

impl PhysicalPlan {}

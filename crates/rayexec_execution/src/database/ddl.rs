use futures::future::BoxFuture;
use rayexec_error::Result;
use std::fmt::Debug;

use super::create::{CreateScalarFunctionInfo, CreateSchemaInfo, CreateTableInfo};
use super::drop::DropInfo;
use super::table::DataTable;

/// Primary interface for making modifications to a catalog.
pub trait CatalogModifier: Debug + Sync + Send {
    fn create_schema(&self, create: CreateSchemaInfo) -> BoxFuture<'static, Result<()>>;

    fn create_table(
        &self,
        schema: &str,
        info: CreateTableInfo,
    ) -> BoxFuture<'static, Result<Box<dyn DataTable>>>; // TODO: The output might need to include some additional info like if the table was actually created or it already existed.

    fn create_scalar_function(
        &self,
        info: CreateScalarFunctionInfo,
    ) -> BoxFuture<'static, Result<()>>;

    fn create_aggregate_function(
        &self,
        info: CreateScalarFunctionInfo,
    ) -> BoxFuture<'static, Result<()>>;

    fn drop_entry(&self, drop: DropInfo) -> BoxFuture<'static, Result<()>>;
}

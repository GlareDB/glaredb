use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use futures::future::BoxFuture;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::field::{Field, Schema};
use rayexec_error::{RayexecError, Result};

use super::{PlannedTableFunction, TableFunction, TableFunctionArgs};
use crate::database::memory_catalog::MemoryCatalog;
use crate::database::DatabaseContext;
use crate::storage::catalog_storage::CatalogStorage;
use crate::storage::table_storage::DataTable;

pub trait RefreshOperation: Debug + Clone + Copy + PartialEq + Eq + Sync + Send + 'static {
    const NAME: &'static str;
    type State: Debug + Clone + Sync + Send;

    fn schema() -> Schema;

    fn create_state(context: &DatabaseContext, args: TableFunctionArgs) -> Result<Self::State>;

    fn refresh(state: &Self::State) -> Result<BoxFuture<'_, Result<()>>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefreshSchemas;

#[derive(Debug, Clone)]
pub struct RefreshSchemasState {
    catalog: Arc<MemoryCatalog>,
    catalog_storage: Arc<dyn CatalogStorage>,
}

impl RefreshOperation for RefreshSchemas {
    const NAME: &'static str = "refresh_schemas";

    type State = RefreshSchemasState;

    fn schema() -> Schema {
        Schema::new([Field::new("count", DataType::Int64, false)])
    }

    fn create_state(context: &DatabaseContext, args: TableFunctionArgs) -> Result<Self::State> {
        let database_name = args.try_get_position(0)?.try_as_str()?;

        let database = context.get_database(database_name)?;
        let catalog_storage = database.catalog_storage.as_ref().ok_or_else(|| {
            RayexecError::new(format!("Missing catalog storage for '{database_name}'"))
        })?;

        Ok(RefreshSchemasState {
            catalog: database.catalog.clone(),
            catalog_storage: catalog_storage.clone(),
        })
    }

    fn refresh(state: &Self::State) -> Result<BoxFuture<'_, Result<()>>> {
        state.catalog_storage.load_schemas(&state.catalog)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefreshObjects<O: RefreshOperation> {
    _op: PhantomData<O>,
}

impl<O: RefreshOperation> RefreshObjects<O> {
    pub const fn new() -> Self {
        RefreshObjects { _op: PhantomData }
    }
}

impl<O: RefreshOperation> Default for RefreshObjects<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: RefreshOperation> TableFunction for RefreshObjects<O> {
    fn name(&self) -> &'static str {
        O::NAME
    }

    fn plan_and_initialize<'a>(
        &self,
        _context: &'a DatabaseContext,
        _args: TableFunctionArgs,
    ) -> BoxFuture<'a, Result<Box<dyn super::PlannedTableFunction>>> {
        unimplemented!()
    }

    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedTableFunction>> {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct RefreshObjectsImpl<O: RefreshOperation> {
    func: RefreshObjects<O>,
    _state: Option<O::State>,
    _op: PhantomData<O>,
}

impl<O: RefreshOperation> PlannedTableFunction for RefreshObjectsImpl<O> {
    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn table_function(&self) -> &dyn TableFunction {
        &self.func
    }

    fn schema(&self) -> Schema {
        O::schema()
    }

    fn datatable(&self) -> Result<Box<dyn DataTable>> {
        unimplemented!()
    }
}

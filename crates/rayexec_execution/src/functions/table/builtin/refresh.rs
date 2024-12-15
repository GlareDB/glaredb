use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use futures::future::BoxFuture;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::field::{Field, Schema};
use rayexec_error::{RayexecError, Result};

use crate::database::memory_catalog::MemoryCatalog;
use crate::database::DatabaseContext;
use crate::functions::table::{TableFunction, TableFunctionInputs, TableFunctionPlanner};
use crate::functions::{FunctionInfo, Signature};
use crate::storage::catalog_storage::CatalogStorage;

pub trait RefreshOperation: Debug + Clone + Copy + PartialEq + Eq + Sync + Send + 'static {
    const NAME: &'static str;
    type State: Debug + Clone + Sync + Send;

    fn schema() -> Schema;

    #[allow(dead_code)]
    fn create_state(context: &DatabaseContext, args: TableFunctionInputs) -> Result<Self::State>;

    #[allow(dead_code)]
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

    fn create_state(context: &DatabaseContext, args: TableFunctionInputs) -> Result<Self::State> {
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

impl<O: RefreshOperation> FunctionInfo for RefreshObjects<O> {
    fn name(&self) -> &'static str {
        O::NAME
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[],
            variadic_arg: None,
            return_type: DataTypeId::Any,
        }]
    }
}

impl<O: RefreshOperation> TableFunction for RefreshObjects<O> {
    fn planner(&self) -> TableFunctionPlanner {
        unimplemented!()
    }
}

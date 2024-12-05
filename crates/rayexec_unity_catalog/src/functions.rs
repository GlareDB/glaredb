use std::fmt::Debug;
use std::marker::PhantomData;

use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::Result;
use rayexec_execution::database::DatabaseContext;
use rayexec_execution::functions::table::{PlannedTableFunction, TableFunction, TableFunctionArgs};
use rayexec_execution::runtime::Runtime;
use rayexec_execution::storage::table_storage::DataTable;

pub trait UnityObjectsOperation:
    Debug + Clone + Copy + PartialEq + Eq + Sync + Send + 'static
{
    const NAME: &'static str;
    type State: Debug + Clone + Sync + Send;

    fn schema() -> Schema;

    fn create_state(
        context: &DatabaseContext,
        args: TableFunctionArgs,
    ) -> BoxFuture<'_, Result<Self::State>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListSchemasOperation;

#[derive(Debug, Clone)]
pub struct ListSchemasOperationState {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnityObjects<R: Runtime, O: UnityObjectsOperation> {
    runtime: R,
    _op: PhantomData<O>,
}

impl<R: Runtime, O: UnityObjectsOperation> UnityObjects<R, O> {
    pub const fn new(runtime: R) -> Self {
        UnityObjects {
            runtime,
            _op: PhantomData,
        }
    }
}

impl<R: Runtime, O: UnityObjectsOperation> TableFunction for UnityObjects<R, O> {
    fn name(&self) -> &'static str {
        O::NAME
    }

    fn plan_and_initialize<'a>(
        &self,
        context: &'a DatabaseContext,
        args: TableFunctionArgs,
    ) -> BoxFuture<'a, Result<Box<dyn PlannedTableFunction>>> {
        Box::pin(async move {
            let state = O::create_state(context, args).await?;

            unimplemented!()
        })
    }

    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedTableFunction>> {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct UnityObjectsImpl<R: Runtime, O: UnityObjectsOperation> {
    func: UnityObjects<R, O>,
    state: O::State,
}

impl<R: Runtime, O: UnityObjectsOperation> PlannedTableFunction for UnityObjectsImpl<R, O> {
    fn table_function(&self) -> &dyn TableFunction {
        &self.func
    }

    fn schema(&self) -> Schema {
        O::schema()
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        unimplemented!()
    }

    fn datatable(&self) -> Result<Box<dyn DataTable>> {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct UnityObjectsDataTable<O> {
    _op: PhantomData<O>,
}

#[derive(Debug)]
pub struct UnityObjectsDataTableScan<O> {
    _op: PhantomData<O>,
}

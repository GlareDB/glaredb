use std::fmt::{self, Debug};
use std::marker::PhantomData;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use rayexec_bullet::array::Array;
use rayexec_bullet::batch::Batch;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::field::{Field, Schema};
use rayexec_error::{not_implemented, Result};
use rayexec_execution::database::DatabaseContext;
use rayexec_execution::functions::table::inputs::TableFunctionInputs;
use rayexec_execution::functions::table::{PlannedTableFunction, TableFunction};
use rayexec_execution::runtime::Runtime;
use rayexec_execution::storage::table_storage::{
    DataTable,
    DataTableScan,
    EmptyTableScan,
    ProjectedScan,
    Projections,
};

use crate::connection::UnityCatalogConnection;
use crate::rest::{UnityListSchemasResponse, UnityListTablesResponse};

pub trait UnityObjectsOperation<R: Runtime>:
    Debug + Clone + Copy + PartialEq + Eq + Sync + Send + 'static
{
    /// Name of the table function.
    const NAME: &'static str;

    /// State containing the catalog connection.
    type ConnectionState: Debug + Clone + Sync + Send;
    /// State containing the stream for reading responses.
    type StreamState: Debug + Send;

    /// Schema of the output batches.
    fn schema() -> Schema;

    /// Create the connection state.
    fn create_connection_state(
        runtime: R,
        context: &DatabaseContext,
        args: TableFunctionInputs,
    ) -> BoxFuture<'_, Result<Self::ConnectionState>>;

    /// Create a stream state from the connection state.
    fn create_stream_state(state: &Self::ConnectionState) -> Result<Self::StreamState>;

    /// Read the next batch from the stream.
    ///
    /// Returns Ok(None) when stream is finished.
    fn next_batch(state: &mut Self::StreamState) -> BoxFuture<'_, Result<Option<Batch>>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListSchemasOperation;

#[derive(Debug, Clone)]
pub struct ListSchemasConnectionState<R: Runtime> {
    conn: UnityCatalogConnection<R>,
}

pub struct ListSchemasStreamState {
    stream: BoxStream<'static, Result<UnityListSchemasResponse>>,
}

impl fmt::Debug for ListSchemasStreamState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListSchemasStreamState").finish()
    }
}

impl<R: Runtime> UnityObjectsOperation<R> for ListSchemasOperation {
    const NAME: &'static str = "unity_list_schemas";

    type ConnectionState = ListSchemasConnectionState<R>;
    type StreamState = ListSchemasStreamState;

    fn schema() -> Schema {
        Schema::new([
            Field::new("name", DataType::Utf8, false),
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("comment", DataType::Utf8, true),
        ])
    }

    fn create_connection_state(
        runtime: R,
        _context: &DatabaseContext,
        args: TableFunctionInputs,
    ) -> BoxFuture<'_, Result<Self::ConnectionState>> {
        Box::pin(async move {
            let endpoint = args.try_get_position(0)?.try_as_str()?;
            let catalog = args.try_get_position(1)?.try_as_str()?;

            let conn = UnityCatalogConnection::connect(runtime, endpoint, catalog).await?;

            Ok(ListSchemasConnectionState { conn })
        })
    }

    fn create_stream_state(state: &Self::ConnectionState) -> Result<Self::StreamState> {
        let stream = Box::pin(state.conn.list_schemas()?.into_stream());
        Ok(ListSchemasStreamState { stream })
    }

    fn next_batch(state: &mut Self::StreamState) -> BoxFuture<'_, Result<Option<Batch>>> {
        Box::pin(async {
            let resp = state.stream.try_next().await?;
            match resp {
                Some(resp) => {
                    let names = Array::from_iter(resp.schemas.iter().map(|s| s.name.as_str()));
                    let catalog_names =
                        Array::from_iter(resp.schemas.iter().map(|s| s.catalog_name.as_str()));
                    let comments =
                        Array::from_iter(resp.schemas.iter().map(|s| s.comment.as_deref()));

                    let batch = Batch::try_new([names, catalog_names, comments])?;
                    Ok(Some(batch))
                }
                None => Ok(None),
            }
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListTablesOperation;

#[derive(Debug, Clone)]
pub struct ListTablesConnectionState<R: Runtime> {
    conn: UnityCatalogConnection<R>,
    schema: String,
}

pub struct ListTablesStreamState {
    stream: BoxStream<'static, Result<UnityListTablesResponse>>,
}

impl fmt::Debug for ListTablesStreamState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListTablesStreamState ").finish()
    }
}

impl<R: Runtime> UnityObjectsOperation<R> for ListTablesOperation {
    const NAME: &'static str = "unity_list_tables";

    type ConnectionState = ListTablesConnectionState<R>;
    type StreamState = ListTablesStreamState;

    fn schema() -> Schema {
        Schema::new([
            Field::new("name", DataType::Utf8, false),
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
            Field::new("data_source_format", DataType::Utf8, false),
            Field::new("storage_location", DataType::Utf8, false),
            Field::new("comment", DataType::Utf8, true),
        ])
    }

    fn create_connection_state(
        runtime: R,
        _context: &DatabaseContext,
        args: TableFunctionInputs,
    ) -> BoxFuture<'_, Result<Self::ConnectionState>> {
        Box::pin(async move {
            let endpoint = args.try_get_position(0)?.try_as_str()?;
            let catalog = args.try_get_position(1)?.try_as_str()?;
            let schema = args.try_get_position(2)?.try_as_str()?;

            let conn = UnityCatalogConnection::connect(runtime, endpoint, catalog).await?;

            Ok(ListTablesConnectionState {
                conn,
                schema: schema.to_string(),
            })
        })
    }

    fn create_stream_state(state: &Self::ConnectionState) -> Result<Self::StreamState> {
        let stream = Box::pin(state.conn.list_tables(&state.schema)?.into_stream());
        Ok(ListTablesStreamState { stream })
    }

    fn next_batch(state: &mut Self::StreamState) -> BoxFuture<'_, Result<Option<Batch>>> {
        Box::pin(async {
            let resp = state.stream.try_next().await?;
            match resp {
                Some(resp) => {
                    let names = Array::from_iter(resp.tables.iter().map(|s| s.name.as_str()));
                    let catalog_names =
                        Array::from_iter(resp.tables.iter().map(|s| s.catalog_name.as_str()));
                    let schema_names =
                        Array::from_iter(resp.tables.iter().map(|s| s.schema_name.as_str()));
                    let table_types =
                        Array::from_iter(resp.tables.iter().map(|s| s.table_type.as_str()));
                    let data_source_formats =
                        Array::from_iter(resp.tables.iter().map(|s| s.data_source_format.as_str()));
                    let storage_locations =
                        Array::from_iter(resp.tables.iter().map(|s| s.storage_location.as_str()));
                    let comments =
                        Array::from_iter(resp.tables.iter().map(|s| s.comment.as_deref()));

                    let batch = Batch::try_new([
                        names,
                        catalog_names,
                        schema_names,
                        table_types,
                        data_source_formats,
                        storage_locations,
                        comments,
                    ])?;
                    Ok(Some(batch))
                }
                None => Ok(None),
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnityObjects<R: Runtime, O: UnityObjectsOperation<R>> {
    runtime: R,
    _op: PhantomData<O>,
}

impl<R: Runtime, O: UnityObjectsOperation<R>> UnityObjects<R, O> {
    pub const fn new(runtime: R) -> Self {
        UnityObjects {
            runtime,
            _op: PhantomData,
        }
    }
}

impl<R: Runtime, O: UnityObjectsOperation<R>> TableFunction for UnityObjects<R, O> {
    fn name(&self) -> &'static str {
        O::NAME
    }

    fn plan_and_initialize<'a>(
        &self,
        context: &'a DatabaseContext,
        args: TableFunctionInputs,
    ) -> BoxFuture<'a, Result<Box<dyn PlannedTableFunction>>> {
        let func = self.clone();
        let runtime = self.runtime.clone();

        Box::pin(async move {
            let state = O::create_connection_state(runtime, context, args).await?;
            Ok(Box::new(UnityObjectsImpl::<R, O> { func, state }) as _)
        })
    }

    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedTableFunction>> {
        not_implemented!("decode state for unity operation")
    }
}

#[derive(Debug, Clone)]
pub struct UnityObjectsImpl<R: Runtime, O: UnityObjectsOperation<R>> {
    func: UnityObjects<R, O>,
    state: O::ConnectionState,
}

impl<R: Runtime, O: UnityObjectsOperation<R>> PlannedTableFunction for UnityObjectsImpl<R, O> {
    fn table_function(&self) -> &dyn TableFunction {
        &self.func
    }

    fn schema(&self) -> Schema {
        O::schema()
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        not_implemented!("decode state for unity operation")
    }

    fn datatable(&self) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(UnityObjectsDataTable::<R, O> {
            state: self.state.clone(),
        }))
    }
}

#[derive(Debug, Clone)]
pub struct UnityObjectsDataTable<R: Runtime, O: UnityObjectsOperation<R>> {
    state: O::ConnectionState,
}

impl<R: Runtime, O: UnityObjectsOperation<R>> DataTable for UnityObjectsDataTable<R, O> {
    fn scan(
        &self,
        projections: Projections,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn DataTableScan>>> {
        let stream = O::create_stream_state(&self.state)?;

        let mut scans: Vec<Box<dyn DataTableScan>> = vec![Box::new(ProjectedScan::new(
            UnityObjectsDataTableScan::<R, O> { stream },
            projections,
        ))];
        scans.extend((1..num_partitions).map(|_| Box::new(EmptyTableScan) as _));

        Ok(scans)
    }
}

#[derive(Debug)]
pub struct UnityObjectsDataTableScan<R: Runtime, O: UnityObjectsOperation<R>> {
    stream: O::StreamState,
}

impl<R: Runtime, O: UnityObjectsOperation<R>> DataTableScan for UnityObjectsDataTableScan<R, O> {
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>> {
        O::next_batch(&mut self.stream)
    }
}

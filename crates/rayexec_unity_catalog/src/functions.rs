use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, TryStreamExt};
use rayexec_error::Result;
use rayexec_execution::arrays::array::Array;
use rayexec_execution::arrays::batch::Batch;
use rayexec_execution::arrays::datatype::{DataType, DataTypeId};
use rayexec_execution::arrays::field::{Field, Schema};
use rayexec_execution::arrays::scalar::ScalarValue;
use rayexec_execution::database::DatabaseContext;
use rayexec_execution::expr;
use rayexec_execution::functions::table::{
    try_get_positional,
    PlannedTableFunction,
    ScanPlanner,
    TableFunction,
    TableFunctionImpl,
    TableFunctionPlanner,
};
use rayexec_execution::functions::{FunctionInfo, Signature};
use rayexec_execution::logical::statistics::StatisticsValue;
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
    /// Function signatures.
    const SIGNATURES: &[Signature];

    /// State containing the catalog connection.
    type ConnectionState: Debug + Clone + Sync + Send;
    /// State containing the stream for reading responses.
    type StreamState: Debug + Send;

    /// Schema of the output batches.
    fn schema() -> Schema;

    /// Create the connection state.
    fn create_connection_state(
        info: UnityObjects<R, Self>,
        context: &DatabaseContext,
        positional_args: Vec<ScalarValue>,
        named_args: HashMap<String, ScalarValue>,
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
    const SIGNATURES: &[Signature] = &[Signature {
        positional_args: &[DataTypeId::Utf8, DataTypeId::Utf8],
        variadic_arg: None,
        return_type: DataTypeId::Any,
        doc: None,
    }];

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
        info: UnityObjects<R, Self>,
        _context: &DatabaseContext,
        positional_args: Vec<ScalarValue>,
        _named_args: HashMap<String, ScalarValue>,
    ) -> BoxFuture<'_, Result<Self::ConnectionState>> {
        Box::pin(async move {
            let endpoint = try_get_positional(&info, 0, &positional_args)?.try_as_str()?;
            let catalog = try_get_positional(&info, 1, &positional_args)?.try_as_str()?;

            let conn = UnityCatalogConnection::connect(info.runtime, endpoint, catalog).await?;

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

                    let batch = Batch::from_arrays([names, catalog_names, comments])?;
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
    const SIGNATURES: &[Signature] = &[Signature {
        positional_args: &[DataTypeId::Utf8, DataTypeId::Utf8, DataTypeId::Utf8],
        variadic_arg: None,
        return_type: DataTypeId::Any,
        doc: None,
    }];

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
        info: UnityObjects<R, Self>,
        _context: &DatabaseContext,
        positional_args: Vec<ScalarValue>,
        _named_args: HashMap<String, ScalarValue>,
    ) -> BoxFuture<'_, Result<Self::ConnectionState>> {
        Box::pin(async move {
            let endpoint = try_get_positional(&info, 0, &positional_args)?.try_as_str()?;
            let catalog = try_get_positional(&info, 1, &positional_args)?.try_as_str()?;
            let schema = try_get_positional(&info, 2, &positional_args)?.try_as_str()?;

            let conn = UnityCatalogConnection::connect(info.runtime, endpoint, catalog).await?;

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

                    let batch = Batch::from_arrays([
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

impl<R: Runtime, O: UnityObjectsOperation<R>> FunctionInfo for UnityObjects<R, O> {
    fn name(&self) -> &'static str {
        O::NAME
    }

    fn signatures(&self) -> &[Signature] {
        O::SIGNATURES
    }
}

impl<R: Runtime, O: UnityObjectsOperation<R>> TableFunction for UnityObjects<R, O> {
    fn planner(&self) -> TableFunctionPlanner {
        TableFunctionPlanner::Scan(self)
    }
}

impl<R: Runtime, O: UnityObjectsOperation<R>> ScanPlanner for UnityObjects<R, O> {
    fn plan<'a>(
        &self,
        context: &'a DatabaseContext,
        positional_inputs: Vec<ScalarValue>,
        named_inputs: HashMap<String, ScalarValue>,
    ) -> BoxFuture<'a, Result<PlannedTableFunction>> {
        Self::plan_inner(self.clone(), context, positional_inputs, named_inputs).boxed()
    }
}

impl<R: Runtime, O: UnityObjectsOperation<R>> UnityObjects<R, O> {
    async fn plan_inner(
        self,
        context: &DatabaseContext,
        positional_inputs: Vec<ScalarValue>,
        named_inputs: HashMap<String, ScalarValue>,
    ) -> Result<PlannedTableFunction> {
        // TODO: Remove clones.
        let state = O::create_connection_state(
            self.clone(),
            context,
            positional_inputs.clone(),
            named_inputs.clone(),
        )
        .await?;

        Ok(PlannedTableFunction {
            function: Box::new(self),
            positional: positional_inputs.into_iter().map(expr::lit).collect(),
            named: named_inputs,
            function_impl: TableFunctionImpl::Scan(Arc::new(UnityObjectsDataTable::<R, O> {
                state,
            })),
            cardinality: StatisticsValue::Unknown,
            schema: O::schema(),
        })
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

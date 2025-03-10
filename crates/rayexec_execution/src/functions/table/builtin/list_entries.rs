use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{stream, Stream, StreamExt};
use rayexec_error::Result;

use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::arrays::field::{ColumnSchema, Field};
use crate::catalog::context::DatabaseContext;
use crate::catalog::database::Database;
use crate::catalog::entry::CatalogEntry;
use crate::catalog::memory::MemorySchema;
use crate::catalog::{Catalog, Schema};
use crate::execution::operators::{ExecutionProperties, PollPull};
use crate::functions::function_set::TableFunctionSet;
use crate::functions::table::scan::TableScanFunction;
use crate::functions::table::{TableFunctionBindState, TableFunctionInput};
use crate::logical::statistics::StatisticsValue;
use crate::storage::projections::Projections;

pub const FUNCTION_SET_LIST_TABLES: TableFunctionSet = TableFunctionSet {
    name: "list_tables",
    aliases: &[],
    doc: None,
    functions: &[],
};

pub struct ListTablesBindState {
    databases: Vec<Arc<Database>>,
}

pub struct ListTablesOperatorState {
    projections: Projections,
    databases: Vec<Arc<Database>>,
}

pub struct ListTablesPartitionState {
    db_offset: usize,
    databases: Vec<Arc<Database>>,
    curr_stream: Option<Pin<Box<dyn Stream<Item = Result<SchemaWithEntries>> + Sync + Send>>>,
    curr: Option<SchemaWithEntries>,
}

struct SchemaWithEntries {
    schema: Arc<MemorySchema>,
    entries_offset: usize,
    entries: Vec<Arc<CatalogEntry>>,
}

#[derive(Debug, Clone, Copy)]
pub struct ListTables;

impl TableScanFunction for ListTables {
    type BindState = ListTablesBindState;
    type OperatorState = ListTablesOperatorState;
    type PartitionState = ListTablesPartitionState;

    fn bind<'a>(
        &self,
        db_context: &'a DatabaseContext,
        input: TableFunctionInput,
    ) -> impl Future<Output = Result<TableFunctionBindState<Self::BindState>>> + Sync + Send + 'a
    {
        let databases = db_context.iter_databases().cloned().collect();
        async move {
            Ok(TableFunctionBindState {
                state: ListTablesBindState { databases },
                input,
                schema: ColumnSchema::new([
                    Field::new("database_name", DataType::Utf8, false),
                    Field::new("schema_name", DataType::Utf8, false),
                    Field::new("table_name", DataType::Utf8, false),
                ]),
                cardinality: StatisticsValue::Unknown,
            })
        }
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: &Projections,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(ListTablesOperatorState {
            projections: projections.clone(),
            databases: bind_state.databases.clone(),
        })
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        unimplemented!()
        // let mut states = vec![ListTablesPartitionState {
        //     db_offset: 0,
        //     databases: op_state.databases.clone(),
        //     curr_schema_stream: None,
        //     schemas: Vec::new(),
        //     schemas_offset: 0,
        //     curr_entry_stream: None,
        //     entries_offset: 0,
        //     entries: Vec::new(),
        // }];

        // states.resize_with(partitions, || ListTablesPartitionState {
        //     db_offset: 0,
        //     databases: Vec::new(),
        //     curr_schema_stream: None,
        //     schemas: Vec::new(),
        //     schemas_offset: 0,
        //     curr_entry_stream: None,
        //     entries_offset: 0,
        //     entries: Vec::new(),
        // });

        // Ok(states)
    }

    fn poll_pull(
        cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        let schema_stream = state.databases[0].catalog.list_schemas();

        // let entries_stream = schema_stream.flat_map(|result| match result {
        //     Ok(schema) => {
        //         let streams = schema.into_iter().flat_map(|schema| );
        //         Box::pin(stream::iter(streams).flatten())
        //     }
        //     Err(err) => Box::pin(stream::once(async { Err(err) })),
        // });

        // state.curr_stream = Some(Box::pin(entries_stream));

        unimplemented!()
    }
}

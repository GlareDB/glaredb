use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;

use futures::Stream;
use glaredb_error::{not_implemented, Result};

use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::arrays::field::{ColumnSchema, Field};
use crate::catalog::context::DatabaseContext;
use crate::catalog::database::Database;
use crate::catalog::entry::CatalogEntry;
use crate::catalog::memory::MemorySchema;
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
    _databases: Vec<Arc<Database>>,
}

pub struct ListTablesOperatorState {
    _projections: Projections,
    _databases: Vec<Arc<Database>>,
}

pub struct ListTablesPartitionState {
    _db_offset: usize,
    _databases: Vec<Arc<Database>>,
    _curr_stream: Option<Pin<Box<dyn Stream<Item = Result<SchemaWithEntries>> + Sync + Send>>>,
    _curr: Option<SchemaWithEntries>,
}

struct SchemaWithEntries {
    _schema: Arc<MemorySchema>,
    _entries_offset: usize,
    _entries: Vec<Arc<CatalogEntry>>,
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
                state: ListTablesBindState {
                    _databases: databases,
                },
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
        _bind_state: &Self::BindState,
        _projections: &Projections,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        not_implemented!("LIST TABLES")
    }

    fn create_pull_partition_states(
        _op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        _partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        not_implemented!("LIST TABLES")
    }

    fn poll_pull(
        _cx: &mut Context,
        _op_state: &Self::OperatorState,
        _state: &mut Self::PartitionState,
        _output: &mut Batch,
    ) -> Result<PollPull> {
        not_implemented!("LIST TABLES")
    }
}

use crate::repr::df::{DataFrame, Schema};
use crate::repr::expr::{BinaryOperation, ScalarExpr};
use crate::repr::relation::{PrimaryKeyIndices, RelationKey};
use crate::repr::value::Value;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bitvec::vec::BitVec;
use futures::stream::Stream;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// An async stream of dataframes.
///
/// Every dataframe in the stream must have the same schema.
pub type DataFrameStream = Pin<Box<dyn Stream<Item = Result<DataFrame>> + Send>>;

#[async_trait]
pub trait DataSource: Clone + Sync + Send {
    type Tx: WriteTx;

    async fn begin(&self) -> Result<Self::Tx>;
}

/// A readable source is able read dataframes and dataframe schemas.
#[async_trait]
pub trait ReadTx: Sync + Send {
    /// Get the schema for a given table.
    ///
    /// Returns `None` if the table doesn't exist.
    async fn get_schema(&self, table: &RelationKey) -> Result<Option<Schema>>;

    /// Read from a source, returning a stream of dataframes.
    ///
    /// An optional filter expression can be provided.
    ///
    /// Returns `None` if the table doesn't exist.
    async fn scan(
        &self,
        table: &RelationKey,
        filter: Option<ScalarExpr>,
    ) -> Result<Option<DataFrameStream>>;

    /// Read from a source, returning a stream of dataframes
    ///
    /// accepts a list of tuples with a column index and values
    /// expected to be found for that column
    async fn scan_values_equal(
        &self,
        table: &RelationKey,
        values: &[(usize, Value)],
    ) -> Result<Option<DataFrameStream>> {
        let filter_values_equal = values
            .iter()
            .map(|(i, v)| ScalarExpr::Binary {
                op: BinaryOperation::Eq,
                left: ScalarExpr::Constant(v.clone()).boxed(),
                right: ScalarExpr::Column(*i).boxed(),
            })
            .reduce(|accum, expr| accum.and(expr));

        match filter_values_equal {
            Some(expr) => self.scan(table, Some(expr)).await,
            None => Ok(None),
        }
    }
}

/// A writeable source is able to write dataframes to underlying tables, as well
/// as create, alter, and delete tables.
#[async_trait]
pub trait WriteTx: ReadTx + Sync + Send {
    async fn commit(self) -> Result<()>;
    async fn rollback(self) -> Result<()>;

    /// Allocate a table with the given schema. Errors if the table already
    /// exists.
    async fn allocate_table(&self, table: RelationKey, schema: Schema) -> Result<()>;

    /// Deallocate a table. Errors if the table doesn't exist.
    async fn deallocate_table(&self, table: &RelationKey) -> Result<()>;

    /// Allocate a table if it doens't exist, returning true if the table was
    /// allocated.
    async fn allocate_table_if_not_exists(
        &self,
        table: RelationKey,
        schema: Schema,
    ) -> Result<bool> {
        match self.get_schema(&table).await? {
            Some(_) => Ok(false),
            None => {
                self.allocate_table(table, schema).await?;
                Ok(true)
            }
        }
    }

    /// Insert data into a table. Errors if the table doesn't exist.
    async fn insert(
        &self,
        table: &RelationKey,
        pk_idxs: PrimaryKeyIndices<'_>,
        data: DataFrame,
    ) -> Result<()>;
}

/// Execute a reading operation.
#[async_trait]
pub trait ReadExecutor<R: ReadTx> {
    /// Execute a read against `source`, returning a data stream.
    async fn execute_read(self, source: &R) -> Result<DataFrameStream>;
}

/// Execute a writing operation.
#[async_trait]
pub trait WriteExecutor<W: WriteTx> {
    /// Execute a write against `source`, returning an optional data stream.
    async fn execute_write(self, source: &W) -> Result<Option<DataFrameStream>>;
}

/// A simple in-memory stream holding already materialized dataframes.
#[derive(Debug)]
pub struct MemoryStream {
    dfs: VecDeque<DataFrame>,
}

impl MemoryStream {
    pub fn empty() -> Self {
        MemoryStream {
            dfs: VecDeque::new(),
        }
    }

    pub fn one(df: DataFrame) -> Self {
        let mut dfs = VecDeque::with_capacity(1);
        dfs.push_back(df);
        MemoryStream { dfs }
    }

    pub fn with_dataframes(dfs: impl IntoIterator<Item = DataFrame>) -> Self {
        MemoryStream {
            dfs: dfs.into_iter().collect(),
        }
    }
}

impl Stream for MemoryStream {
    type Item = Result<DataFrame>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.dfs.pop_front() {
            Some(df) => Poll::Ready(Some(Ok(df))),
            None => Poll::Ready(None),
        }
    }
}

/// Initial capacity for each dataframe in the memory source.
const MEMORY_SOURCE_DF_CAPACITY: usize = 256;

/// A data source for testing. The entire source is read or write locked on
/// every action.
#[derive(Debug, Clone)]
pub struct MemoryDataSource {
    tables: Arc<RwLock<HashMap<RelationKey, DataFrame>>>,
}

impl MemoryDataSource {
    pub fn new() -> MemoryDataSource {
        Default::default()
    }
}

impl Default for MemoryDataSource {
    fn default() -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl DataSource for MemoryDataSource {
    type Tx = Self;

    async fn begin(&self) -> Result<Self::Tx> {
        Ok(self.clone())
    }
}

#[async_trait]
impl ReadTx for MemoryDataSource {
    async fn get_schema(&self, table: &RelationKey) -> Result<Option<Schema>> {
        let tables = self.tables.read();
        match tables.get(table) {
            Some(df) => Ok(Some(df.schema())),
            None => Ok(None),
        }
    }

    async fn scan(
        &self,
        table: &RelationKey,
        filter: Option<ScalarExpr>,
    ) -> Result<Option<DataFrameStream>> {
        if self.get_schema(table).await?.is_none() {
            return Err(anyhow!("missing table: {}", table));
        }

        let tables = self.tables.read();
        match tables.get(table) {
            Some(df) => {
                // Note that this has a slightly different than what would
                // happen on "remote" data sources. If the filter errors, we
                // return the result directly. On a remote data source, the
                // result would be sent on the stream.
                match filter {
                    Some(filter) => {
                        // Logic duplicated with the filter node.
                        let evaled = filter.evaluate(df)?;
                        let bools = evaled
                            .as_ref()
                            .downcast_bool_vec()
                            .ok_or_else(|| anyhow!("vec not a bool vec"))?;
                        let mut mask = BitVec::with_capacity(bools.len());
                        for v in bools.iter_values() {
                            mask.push(*v);
                        }
                        let filtered = df.filter(&mask)?;
                        Ok(Some(Box::pin(MemoryStream::one(filtered))))
                    }
                    None => Ok(Some(Box::pin(MemoryStream::one(df.clone())))),
                }
            }
            None => Ok(None),
        }
    }
}

#[async_trait]
impl WriteTx for MemoryDataSource {
    async fn commit(self) -> Result<()> {
        Ok(())
    }

    async fn rollback(self) -> Result<()> {
        Ok(())
    }

    async fn allocate_table(&self, table: RelationKey, schema: Schema) -> Result<()> {
        use std::collections::hash_map::Entry;
        let mut tables = self.tables.write();
        match tables.entry(table) {
            Entry::Occupied(entry) => Err(anyhow!("table {} already exists", entry.key())),
            Entry::Vacant(entry) => {
                entry.insert(DataFrame::with_schema_and_capacity(
                    &schema,
                    MEMORY_SOURCE_DF_CAPACITY,
                )?);
                Ok(())
            }
        }
    }

    async fn deallocate_table(&self, table: &RelationKey) -> Result<()> {
        let mut tables = self.tables.write();
        match tables.remove(table) {
            Some(_) => Ok(()),
            None => Err(anyhow!("cannot drop non-existent table {}", table)),
        }
    }

    async fn insert(
        &self,
        table: &RelationKey,
        _pk_idxs: PrimaryKeyIndices<'_>,
        data: DataFrame,
    ) -> Result<()> {
        // TODO: Ensure no pk duplicates.

        let mut tables = self.tables.write();
        let df = tables
            .get_mut(table)
            .ok_or_else(|| anyhow!("missing table {}", table))?;
        *df = df.clone().vstack(data)?;
        Ok(())
    }
}

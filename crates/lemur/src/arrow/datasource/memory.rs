//! Provides an in-memory data source suitable for testing.
use crate::arrow::chunk::Chunk;
use crate::arrow::expr::ScalarExpr;
use crate::arrow::queryexec::PinnedChunkStream;
use crate::arrow::row::Row;
use crate::errors::{internal, Result};
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::{MutableDataSource, PkRowRef, QueryDataSource, TableSchema};

const SCAN_BUFFER: usize = 16;
const SCAN_CHUNK_SIZE: usize = 256;

#[derive(Debug, Default)]
pub struct MemorySource {
    tables: Mutex<BTreeMap<String, Table>>,
}

#[async_trait]
impl QueryDataSource for MemorySource {
    async fn get(&self, table: &str, key: PkRowRef) -> Result<Option<Row>> {
        let tables = self.tables.lock();
        let table = tables
            .get(table)
            .ok_or_else(|| internal!("missing table: {}", table))?;

        let row_num = match table.indexes.pk_index.get(&key) {
            Some(num) => *num,
            None => return Ok(None),
        };

        let row = table
            .chunks
            .get_row(row_num)
            .ok_or_else(|| internal!("missing row for row num: {}", row_num))?;

        Ok(Some(row))
    }

    async fn scan(
        &self,
        table: &str,
        filter: Option<ScalarExpr>,
        projection: Option<Vec<usize>>,
    ) -> Result<PinnedChunkStream> {
        let chunks = {
            let tables = self.tables.lock();
            let table = tables
                .get(table)
                .ok_or_else(|| internal!("missing table: {}", table))?;

            table.chunks.clone() // Cheap, columns behind an arc.
        };

        let (tx, rx) = mpsc::channel(SCAN_BUFFER);
        tokio::spawn(async move {
            for mut chunk in chunks.chunk_slice_iter(SCAN_CHUNK_SIZE) {
                // Filter chunks.
                if let Some(filter) = &filter {
                    let col = match filter.evaluate(&chunk) {
                        Ok(col) => match col.into_column_or_expand(chunk.num_rows()) {
                            Ok(col) => col,
                            Err(e) => {
                                let _ = tx.send(Err(e)).await;
                                return;
                            }
                        },
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                            return;
                        }
                    };
                    match col.try_downcast_bool() {
                        Some(mask) => {
                            chunk = match chunk.filter(mask) {
                                Ok(chunk) => chunk,
                                Err(e) => {
                                    let _ = tx.send(Err(e)).await;
                                    return;
                                }
                            }
                        }
                        None => {
                            let _ = tx
                                .send(Err(internal!("result of expression not a bool")))
                                .await;
                            return;
                        }
                    }
                }

                // Project chunks.
                if let Some(projection) = &projection {
                    chunk = match chunk.project(projection) {
                        Ok(chunk) => chunk,
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                            return;
                        }
                    }
                }

                let _ = tx.send(Ok(chunk)).await;
            }
        });

        let stream = ReceiverStream::new(rx);
        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl MutableDataSource for MemorySource {
    async fn allocate_table(&self, table: &str, schema: TableSchema) -> Result<()> {
        let mut tables = self.tables.lock();
        if tables.contains_key(table) {
            return Err(internal!("duplicate table: {}", table));
        }
        tables.insert(table.to_string(), Table::new(schema));
        Ok(())
    }

    async fn deallocate_table(&self, _table: &str) -> Result<()> {
        todo!()
    }

    async fn insert_chunk(&self, table: &str, chunk: Chunk) -> Result<()> {
        let mut tables = self.tables.lock();
        let table = tables
            .get_mut(table)
            .ok_or_else(|| internal!("missing table: {}", table))?;
        table.insert_chunk(chunk)?;
        Ok(())
    }

    async fn delete_one(&self, _table: &str, _key: PkRowRef) -> Result<()> {
        todo!()
    }

    async fn delete_many(&self, _table: &str, _filter: ScalarExpr) -> Result<()> {
        todo!()
    }
}

#[derive(Debug)]
struct Table {
    schema: TableSchema,
    indexes: Indexes,
    chunks: Chunk,
}

impl Table {
    fn new(schema: TableSchema) -> Table {
        let start_chunk = Chunk::empty_with_schema(schema.schema.clone());
        Table {
            schema,
            indexes: Indexes::default(),
            chunks: start_chunk,
        }
    }

    fn insert_chunk(&mut self, chunk: Chunk) -> Result<()> {
        let mut chunk_index = Indexes::new_for_chunk(&chunk, &self.schema.pk_idxs)?;
        chunk_index.bump_row_numbers(self.chunks.num_rows());
        self.indexes.try_union(chunk_index)?;
        self.chunks = self.chunks.vstack(&chunk)?;
        Ok(())
    }
}

#[derive(Debug, Default)]
struct Indexes {
    /// Index between the primary key and the relative row number.
    pk_index: BTreeMap<PkRowRef, usize>,
}

impl Indexes {
    /// Create a new set of indexes from a chunk.
    fn new_for_chunk(chunk: &Chunk, pk_idxs: &[usize]) -> Result<Indexes> {
        let pk_cols = pk_idxs
            .iter()
            .map(|idx| chunk.get_column(*idx).cloned())
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| internal!("missing column"))?;
        let pk_chunk: Chunk = pk_cols.try_into()?;

        let mut pk_index = BTreeMap::new();
        for (row_num, pk_row) in pk_chunk.row_iter().enumerate() {
            let pk_val = PkRowRef(pk_row.into_boxed_scalars());
            if pk_index.insert(pk_val, row_num).is_some() {
                return Err(internal!("duplicate primary key"));
            }
        }

        Ok(Indexes { pk_index })
    }

    fn bump_row_numbers(&mut self, amount: usize) {
        for (_, row_num) in self.pk_index.iter_mut() {
            *row_num += amount;
        }
    }

    fn try_union(&mut self, other: Indexes) -> Result<()> {
        use std::collections::btree_map::Entry;

        for (pk, row_num) in other.pk_index.into_iter() {
            match self.pk_index.entry(pk) {
                Entry::Vacant(entry) => {
                    entry.insert(row_num);
                }
                Entry::Occupied(_) => return Err(internal!("duplicate primary key")),
            }
        }
        Ok(())
    }
}

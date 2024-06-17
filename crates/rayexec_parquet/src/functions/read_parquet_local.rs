use futures::{future::BoxFuture, stream::BoxStream, StreamExt};
use rayexec_bullet::{batch::Batch, field::Schema};
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_execution::{
    database::table::{DataTable, DataTableScan},
    engine::EngineRuntime,
    execution::operators::PollPull,
    functions::table::{InitializedTableFunction, SpecializedTableFunction},
};
use std::{
    collections::VecDeque,
    fmt,
    fs::{File, OpenOptions},
    os::unix::fs::MetadataExt,
    path::PathBuf,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{array::AsyncBatchReader, metadata::Metadata, schema::convert_schema};

#[derive(Debug, Clone)]
pub struct ReadParquetLocal {
    pub(crate) path: PathBuf,
}

impl SpecializedTableFunction for ReadParquetLocal {
    fn name(&self) -> &'static str {
        "read_parquet_local"
    }

    fn initialize(
        self: Box<Self>,
        _runtime: &EngineRuntime,
    ) -> BoxFuture<Result<Box<dyn InitializedTableFunction>>> {
        Box::pin(async move { self.initialize_inner().await })
    }
}

impl ReadParquetLocal {
    async fn initialize_inner(self) -> Result<Box<dyn InitializedTableFunction>> {
        let file = self.open_file()?;
        let size = file
            .metadata()
            .context("failed to get file metadata")?
            .size();

        let metadata = Metadata::load_from(file, size as usize).await?;
        let schema = convert_schema(metadata.parquet_metadata.file_metadata().schema_descr())?;

        Ok(Box::new(ReadParquetLocalRowGroupPartitioned {
            specialized: self,
            metadata: Arc::new(metadata),
            schema,
        }))
    }

    fn open_file(&self) -> Result<File> {
        OpenOptions::new().read(true).open(&self.path).map_err(|e| {
            RayexecError::with_source(
                format!(
                    "Failed to open file at location: {}",
                    self.path.to_string_lossy()
                ),
                Box::new(e),
            )
        })
    }
}

#[derive(Debug, Clone)]
pub struct ReadParquetLocalRowGroupPartitioned {
    pub(crate) specialized: ReadParquetLocal,
    pub(crate) metadata: Arc<Metadata>,
    pub(crate) schema: Schema,
}

impl InitializedTableFunction for ReadParquetLocalRowGroupPartitioned {
    fn specialized(&self) -> &dyn SpecializedTableFunction {
        &self.specialized
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn datatable(&self, _runtime: &Arc<EngineRuntime>) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(RowGroupPartitionedDataTable {
            specialized: self.specialized.clone(),
            metadata: self.metadata.clone(),
            schema: self.schema.clone(),
        }))
    }
}

/// Data table implementation which parallelizes on row groups. During scanning,
/// each returned scan object is responsible for distinct row groups to read.
#[derive(Debug)]
struct RowGroupPartitionedDataTable {
    specialized: ReadParquetLocal,
    metadata: Arc<Metadata>,
    schema: Schema,
}

impl DataTable for RowGroupPartitionedDataTable {
    fn scan(&self, num_partitions: usize) -> Result<Vec<Box<dyn DataTableScan>>> {
        let mut partitioned_row_groups = vec![VecDeque::new(); num_partitions];

        // Split row groups into individual partitions.
        for row_group in 0..self.metadata.parquet_metadata.row_groups().len() {
            partitioned_row_groups[row_group % num_partitions].push_back(row_group);
        }

        let readers = partitioned_row_groups
            .into_iter()
            .map(|row_groups| {
                let file = self.specialized.open_file()?;
                const BATCH_SIZE: usize = 2048; // TODO
                AsyncBatchReader::try_new(
                    file,
                    row_groups,
                    self.metadata.clone(),
                    &self.schema,
                    BATCH_SIZE,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        let scans: Vec<Box<dyn DataTableScan>> = readers
            .into_iter()
            .map(|reader| {
                Box::new(RowGroupsScan {
                    stream: reader.into_stream(),
                }) as _
            })
            .collect();

        Ok(scans)
    }
}

struct RowGroupsScan {
    stream: BoxStream<'static, Result<Batch>>,
}

impl DataTableScan for RowGroupsScan {
    fn poll_pull(&mut self, cx: &mut Context) -> Result<PollPull> {
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => Ok(PollPull::Batch(batch)),
            Poll::Ready(Some(Err(e))) => {
                println!("ERR: {e}");
                Err(e)
            }
            Poll::Ready(None) => Ok(PollPull::Exhausted),
            Poll::Pending => Ok(PollPull::Pending),
        }
    }
}

impl fmt::Debug for RowGroupsScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowGroupsScan").finish_non_exhaustive()
    }
}

use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_execution::{
    database::table::DataTable,
    functions::table::{InitializedTableFunction, SpecializedTableFunction},
    runtime::ExecutionRuntime,
};
use std::{
    fs::{File, OpenOptions},
    path::PathBuf,
    sync::Arc,
};

use crate::{metadata::Metadata, schema::convert_schema};

use super::datatable::{ReaderBuilder, RowGroupPartitionedDataTable};

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
        _runtime: &Arc<dyn ExecutionRuntime>,
    ) -> BoxFuture<Result<Box<dyn InitializedTableFunction>>> {
        Box::pin(async move { self.initialize_inner().await })
    }
}

impl ReadParquetLocal {
    async fn initialize_inner(self) -> Result<Box<dyn InitializedTableFunction>> {
        let mut file = self.open_file()?;
        let size = file
            .metadata()
            .context("failed to get file metadata")?
            .len();

        let metadata = Metadata::load_from(&mut file, size as usize).await?;
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

impl ReaderBuilder<File> for ReadParquetLocal {
    fn new_reader(&self) -> Result<File> {
        self.open_file()
    }
}

#[derive(Debug, Clone)]
pub struct ReadParquetLocalRowGroupPartitioned {
    specialized: ReadParquetLocal,
    metadata: Arc<Metadata>,
    schema: Schema,
}

impl InitializedTableFunction for ReadParquetLocalRowGroupPartitioned {
    fn specialized(&self) -> &dyn SpecializedTableFunction {
        &self.specialized
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn datatable(&self, _runtime: &Arc<dyn ExecutionRuntime>) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(RowGroupPartitionedDataTable::new(
            self.specialized.clone(),
            self.metadata.clone(),
            self.schema.clone(),
        )))
    }
}

use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::Result;
use rayexec_execution::{
    database::table::DataTable,
    functions::table::{InitializedTableFunction, SpecializedTableFunction},
    runtime::ExecutionRuntime,
};
use rayexec_io::filesystem::{FileReader, FileSystemProvider};
use std::{path::PathBuf, sync::Arc};

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
        runtime: &Arc<dyn ExecutionRuntime>,
    ) -> BoxFuture<Result<Box<dyn InitializedTableFunction>>> {
        Box::pin(async move { self.initialize_inner(runtime.as_ref()).await })
    }
}

impl ReadParquetLocal {
    async fn initialize_inner(
        self,
        runtime: &dyn ExecutionRuntime,
    ) -> Result<Box<dyn InitializedTableFunction>> {
        let fs = runtime.filesystem()?;

        let mut file = fs.reader(&self.path)?;
        let size = file.size().await?;

        let metadata = Metadata::load_from(&mut file, size).await?;
        let schema = convert_schema(metadata.parquet_metadata.file_metadata().schema_descr())?;

        Ok(Box::new(ReadParquetLocalRowGroupPartitioned {
            reader_builder: FileReaderBuilder {
                fs,
                path: self.path.clone(),
            },
            specialized: self,
            metadata: Arc::new(metadata),
            schema,
        }))
    }
}

#[derive(Debug, Clone)]
struct FileReaderBuilder {
    fs: Arc<dyn FileSystemProvider>,
    path: PathBuf,
}

impl ReaderBuilder<Box<dyn FileReader>> for FileReaderBuilder {
    fn new_reader(&self) -> Result<Box<dyn FileReader>> {
        self.fs.reader(&self.path)
    }
}

#[derive(Debug, Clone)]
pub struct ReadParquetLocalRowGroupPartitioned {
    specialized: ReadParquetLocal,
    reader_builder: FileReaderBuilder,
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
            self.reader_builder.clone(),
            self.metadata.clone(),
            self.schema.clone(),
        )))
    }
}

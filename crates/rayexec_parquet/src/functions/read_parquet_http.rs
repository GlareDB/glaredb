use futures::future::BoxFuture;
use rayexec_bullet::field::Schema;
use rayexec_error::Result;
use rayexec_execution::{
    database::table::DataTable,
    functions::table::{InitializedTableFunction, SpecializedTableFunction},
    runtime::ExecutionRuntime,
};
use rayexec_io::http::{HttpClient, HttpReader};
use std::sync::Arc;
use tracing::trace;
use url::Url;

use crate::{metadata::Metadata, schema::convert_schema};

use super::datatable::{ReaderBuilder, RowGroupPartitionedDataTable};

#[derive(Debug, Clone)]
pub struct ReadParquetHttp {
    pub(crate) url: Url,
}

impl SpecializedTableFunction for ReadParquetHttp {
    fn name(&self) -> &'static str {
        "read_parquet_http"
    }

    fn initialize(
        self: Box<Self>,
        runtime: &Arc<dyn ExecutionRuntime>,
    ) -> BoxFuture<Result<Box<dyn InitializedTableFunction>>> {
        Box::pin(async move { self.initialize_inner(runtime.as_ref()).await })
    }
}

impl ReadParquetHttp {
    async fn initialize_inner(
        self,
        runtime: &dyn ExecutionRuntime,
    ) -> Result<Box<dyn InitializedTableFunction>> {
        let client = runtime.http_client()?;
        let mut reader = client.reader(self.url.clone());
        let size = reader.content_length().await?;
        trace!(%size, "got content length of file");

        let metadata = Metadata::load_from(&mut reader, size).await?;
        let schema = convert_schema(metadata.parquet_metadata.file_metadata().schema_descr())?;

        Ok(Box::new(ReadParquetHttpRowGroupPartitioned {
            reader_builder: HttpReaderBuilder {
                client,
                url: self.url.clone(),
            },
            specialized: self,
            metadata: Arc::new(metadata),
            schema,
        }))
    }
}

#[derive(Debug, Clone)]
struct HttpReaderBuilder {
    client: Arc<dyn HttpClient>,
    url: Url,
}

impl ReaderBuilder<Box<dyn HttpReader>> for HttpReaderBuilder {
    fn new_reader(&self) -> Result<Box<dyn HttpReader>> {
        Ok(self.client.reader(self.url.clone()))
    }
}

#[derive(Debug, Clone)]
pub struct ReadParquetHttpRowGroupPartitioned {
    specialized: ReadParquetHttp,
    reader_builder: HttpReaderBuilder,
    metadata: Arc<Metadata>,
    schema: Schema,
}

impl InitializedTableFunction for ReadParquetHttpRowGroupPartitioned {
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

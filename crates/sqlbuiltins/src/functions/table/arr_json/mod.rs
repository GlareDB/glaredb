use std::{any::Any, io::BufReader, sync::Arc};

use async_trait::async_trait;
use bytes::Buf;
use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    common::FileType,
    datasource::{
        file_format::{file_compression_type::FileCompressionType, FileFormat},
        physical_plan::{FileScanConfig, FileSinkConfig},
    },
    error::{DataFusionError, Result},
    execution::context::SessionState,
    physical_plan::{ExecutionPlan, PhysicalExpr, Statistics},
};
use object_store::{GetResultPayload, ObjectMeta, ObjectStore};
use serde_json::Value;

use exec::ArrayJsonExec;
use schema::infer_schema_from_value;

mod exec;
mod schema;

/// Format for array json
#[derive(Debug, Clone)]
pub struct ArrayJsonFormat {
    /// Maximum Number of values in the array
    max_data_size: usize,
    file_compression_type: FileCompressionType,
}

impl Default for ArrayJsonFormat {
    fn default() -> Self {
        Self {
            max_data_size: Default::default(),
            file_compression_type: FileCompressionType::UNCOMPRESSED,
        }
    }
}

impl ArrayJsonFormat {
    pub fn set_max_data_size(mut self, size: usize) -> Self {
        self.max_data_size = size;
        self
    }

    pub fn set_file_compression_type(mut self, file_compression_type: FileCompressionType) -> Self {
        self.file_compression_type = file_compression_type;
        self
    }
}

#[async_trait]
impl FileFormat for ArrayJsonFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn file_type(&self) -> FileType {
        FileType::JSON
    }

    async fn infer_schema(
        &self,
        _state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        println!("schema inference");
        // TODO infer schema from only elements between 0 and the range
        let mut schemas = Vec::new();
        let file_compression_type = self.file_compression_type.to_owned();
        let docs_to_read = self.max_data_size;

        for object in objects {
            let r = store.as_ref().get(&object.location).await?;
            let schema = match r.payload {
                GetResultPayload::File(file, _) => {
                    let decoder = file_compression_type.convert_read(file)?;
                    let mut reader = BufReader::new(decoder);

                    let iter: Value = serde_json::from_reader(&mut reader)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    infer_schema_from_value(iter, docs_to_read)?
                }
                GetResultPayload::Stream(_) => {
                    let data = r.bytes().await?;
                    let decoder = file_compression_type.convert_read(data.reader())?;
                    let mut reader = BufReader::new(decoder);

                    let iter: Value = serde_json::from_reader(&mut reader)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    infer_schema_from_value(iter, docs_to_read)?
                }
            };

            println!("{:#?}", schema);

            schemas.push(schema);
        }

        let schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(schema))
    }

    async fn infer_stats(
        &self,
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::default())
    }

    async fn create_physical_plan(
        &self,
        _state: &SessionState,
        conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        println!("physical plan");
        let exec: ArrayJsonExec = ArrayJsonExec::new(conf, self.file_compression_type.to_owned());
        Ok(Arc::new(exec))
    }

    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &SessionState,
        _conf: FileSinkConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        println!("writer physical plan");
        todo!()
    }
}

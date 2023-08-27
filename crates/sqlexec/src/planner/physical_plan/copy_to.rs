use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::execute_stream;
use datafusion::physical_plan::insert::DataSink;
use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, Statistics,
};
use datasources::common::sink::csv::{CsvSink, CsvSinkOpts};
use datasources::common::sink::json::{JsonSink, JsonSinkOpts};
use datasources::common::sink::parquet::{ParquetSink, ParquetSinkOpts};
use datasources::object_store::gcs::GcsStoreAccess;
use datasources::object_store::local::LocalStoreAccess;
use datasources::object_store::s3::S3StoreAccess;
use datasources::object_store::ObjStoreAccess;
use futures::stream;
use protogen::metastore::types::options::{CopyToDestinationOptions, CopyToFormatOptions};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

use super::{new_operation_with_count_batch, GENERIC_OPERATION_AND_COUNT_PHYSICAL_SCHEMA};

#[derive(Debug, Clone)]
pub struct CopyToExec {
    pub format: CopyToFormatOptions,
    pub dest: CopyToDestinationOptions,
    pub source: Arc<dyn ExecutionPlan>,
}

impl ExecutionPlan for CopyToExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        GENERIC_OPERATION_AND_COUNT_PHYSICAL_SCHEMA.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.source.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CopyToExec {
            format: self.format.clone(),
            dest: self.dest.clone(),
            source: children.get(0).unwrap().clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "CopyToExec only supports 1 partition".to_string(),
            ));
        }

        let this = self.clone();
        let stream = stream::once(this.copy_to(context));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for CopyToExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CopyToExec")
    }
}

impl CopyToExec {
    async fn copy_to(self, context: Arc<TaskContext>) -> DataFusionResult<RecordBatch> {
        let sink = match (self.dest, self.format) {
            (CopyToDestinationOptions::Local(local_options), format) => {
                {
                    // Create the path if it doesn't exist (for local).
                    let _ = tokio::fs::File::create(&local_options.location).await?;
                }
                let access = LocalStoreAccess;
                get_sink_for_obj(format, &access, &local_options.location)?
            }
            (CopyToDestinationOptions::Gcs(gcs_options), format) => {
                let access = GcsStoreAccess {
                    bucket: gcs_options.bucket,
                    service_account_key: gcs_options.service_account_key,
                };
                get_sink_for_obj(format, &access, &gcs_options.location)?
            }
            (CopyToDestinationOptions::S3(s3_options), format) => {
                let access = S3StoreAccess {
                    region: s3_options.region,
                    bucket: s3_options.bucket,
                    access_key_id: s3_options.access_key_id,
                    secret_access_key: s3_options.secret_access_key,
                };
                get_sink_for_obj(format, &access, &s3_options.location)?
            }
        };

        let stream = execute_stream(self.source, context.clone())?;
        let count = sink.write_all(stream, &context).await?;

        Ok(new_operation_with_count_batch("copy", count))
    }
}

fn get_sink_for_obj(
    format: CopyToFormatOptions,
    access: &dyn ObjStoreAccess,
    location: &str,
) -> DataFusionResult<Box<dyn DataSink>> {
    let store = access
        .create_store()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let path = access
        .path(location)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let sink: Box<dyn DataSink> = match format {
        CopyToFormatOptions::Csv(csv_opts) => Box::new(CsvSink::from_obj_store(
            store,
            path,
            CsvSinkOpts {
                delim: csv_opts.delim,
                header: csv_opts.header,
            },
        )),
        CopyToFormatOptions::Parquet(parquet_opts) => Box::new(ParquetSink::from_obj_store(
            store,
            path,
            ParquetSinkOpts {
                row_group_size: parquet_opts.row_group_size,
            },
        )),
        CopyToFormatOptions::Json(json_opts) => Box::new(JsonSink::from_obj_store(
            store,
            path,
            JsonSinkOpts {
                array: json_opts.array,
            },
        )),
    };
    Ok(sink)
}

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::execute_stream;
use datafusion::physical_plan::insert::DataSink;
use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    SendableRecordBatchStream,
};
use datafusion_ext::metrics::WriteOnlyDataSourceMetricsExecAdapter;
use datasources::common::sink::csv::{CsvSink, CsvSinkOpts};
use datasources::common::sink::json::{JsonSink, JsonSinkOpts};
use datasources::common::sink::parquet::{ParquetSink, ParquetSinkOpts};
use datasources::common::url::DatasourceUrl;
use datasources::object_store::gcs::GcsStoreAccess;
use datasources::object_store::generic::GenericStoreAccess;
use datasources::object_store::local::LocalStoreAccess;
use datasources::object_store::s3::S3StoreAccess;
use datasources::object_store::ObjStoreAccess;
use futures::stream;
use object_store::azure::AzureConfigKey;
use protogen::metastore::types::options::{
    CopyToDestinationOptions, CopyToFormatOptions, StorageOptions,
};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

use super::{new_operation_with_count_batch, GENERIC_OPERATION_AND_COUNT_PHYSICAL_SCHEMA};

#[derive(Debug, Clone)]
pub struct CopyToExec {
    pub format: CopyToFormatOptions,
    pub dest: CopyToDestinationOptions,
    pub source: Arc<WriteOnlyDataSourceMetricsExecAdapter>,
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
            source: Arc::new(WriteOnlyDataSourceMetricsExecAdapter::new(
                children.get(0).unwrap().clone(),
            )),
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
            (CopyToDestinationOptions::Azure(azure_options), format) => {
                // Create storage options using well-known key names.
                let opts = StorageOptions::new_from_iter([
                    (AzureConfigKey::AccountName.as_ref(), azure_options.account),
                    (AzureConfigKey::AccessKey.as_ref(), azure_options.access_key),
                ]);
                let access =
                    GenericStoreAccess::new_from_location_and_opts(&azure_options.location, opts)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                // TODO: It's weird we need to do this here, but
                // `get_sink_for_obj` is expected a path relative to the root of
                // the store. The location we have here is the full url
                // (azure://...) and so will actually cause object store to
                // error.
                //
                // By converting to a data source url, we can get the path we
                // need.
                //
                // @vaibhav I'd like for us to look into switchin all object
                // store "locations" to use the full url (with scheme) so that
                // we can be consistent with this. It would also help with user
                // experience since they wouldn't need to know which part of the
                // location is the "bucket" and which is the "location" (path).
                let source_url = DatasourceUrl::try_new(&azure_options.location)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                get_sink_for_obj(format, &access, &source_url.path())?
            }
        };

        let stream = execute_stream(self.source, context.clone())?;
        let count = sink.write_all(stream, &context).await?;

        Ok(new_operation_with_count_batch("copy", count))
    }
}

/// Get a sink for writing a file to.
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

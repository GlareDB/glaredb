use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::insert::DataSink;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    execute_stream,
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    SendableRecordBatchStream,
    Statistics,
};
use datafusion_ext::metrics::WriteOnlyDataSourceMetricsExecAdapter;
use datasources::common::sink::bson::BsonSink;
use datasources::common::sink::csv::{CsvSink, CsvSinkOpts};
use datasources::common::sink::json::{JsonSink, JsonSinkOpts};
use datasources::common::sink::lance::{LanceSink, LanceSinkOpts, LanceWriteParams};
use datasources::common::sink::parquet::{ParquetSink, ParquetSinkOpts};
use datasources::object_store::azure::AzureStoreAccess;
use datasources::object_store::gcs::GcsStoreAccess;
use datasources::object_store::local::LocalStoreAccess;
use datasources::object_store::s3::S3StoreAccess;
use datasources::object_store::ObjStoreAccess;
use futures::stream;
use protogen::metastore::types::options::{CopyToDestinationOptions, CopyToFormatOptions};

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
                children.first().unwrap().clone(),
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

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
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
            (CopyToDestinationOptions::Local(local_options), CopyToFormatOptions::Lance(opts)) => {
                get_sink_for_obj(
                    CopyToFormatOptions::Lance(opts),
                    &LocalStoreAccess {},
                    &local_options.location,
                )?
            }
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
                    opts: HashMap::new(),
                };
                get_sink_for_obj(format, &access, &gcs_options.location)?
            }
            (CopyToDestinationOptions::S3(s3_options), format) => {
                let access = S3StoreAccess {
                    bucket: s3_options.bucket,
                    region: Some(s3_options.region),
                    access_key_id: s3_options.access_key_id,
                    secret_access_key: s3_options.secret_access_key,
                    opts: HashMap::new(),
                };
                get_sink_for_obj(format, &access, &s3_options.location)?
            }
            (CopyToDestinationOptions::Azure(azure_options), format) => {
                let access = AzureStoreAccess {
                    container: azure_options.container,
                    account_name: Some(azure_options.account),
                    access_key: Some(azure_options.access_key),
                    opts: HashMap::new(),
                };
                get_sink_for_obj(format, &access, &azure_options.location)?
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
        CopyToFormatOptions::Lance(opts) => {
            let wp = LanceWriteParams::default();

            Box::new(LanceSink::from_obj_store(
                store,
                path,
                LanceSinkOpts {
                    url: Some(
                        url::Url::parse(
                            access
                                .base_url()
                                .map_err(|e| DataFusionError::External(Box::new(e)))?
                                .as_str(),
                        )
                        .map_err(|e| DataFusionError::External(Box::new(e)))?,
                    ),
                    max_rows_per_file: opts.max_rows_per_file.unwrap_or(wp.max_rows_per_file),
                    max_rows_per_group: opts.max_rows_per_group.unwrap_or(wp.max_rows_per_group),
                    max_bytes_per_file: opts.max_bytes_per_file.unwrap_or(wp.max_bytes_per_file),
                    input_batch_size: opts.input_batch_size.unwrap_or(64),
                },
            ))
        }
        CopyToFormatOptions::Json(json_opts) => Box::new(JsonSink::from_obj_store(
            store,
            path,
            JsonSinkOpts {
                array: json_opts.array,
            },
        )),
        CopyToFormatOptions::Bson(_) => Box::new(BsonSink::from_obj_store(store, path)),
    };
    Ok(sink)
}

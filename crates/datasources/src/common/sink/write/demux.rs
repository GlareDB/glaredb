// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Module containing helper methods/traits related to enabling
//! dividing input stream into multiple output files at execution time
//!
//!
//! -- NOTE --
//! This code was originally sourced from:
//! Repo: https://github.com/apache/arrow-datafusion
//! Commit: ae882356171513c9d6c22b3bd966898fb4e8cac0
//! Path: datafusion/core/src/datasource/file_format/write/demux.rs
//! Date: 10 Feb 2024

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{AsArray, StructArray, UInt64Builder};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_string_array;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use object_store::path::Path;
use rand::distributions::DistString;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

type RecordBatchReceiver = Receiver<RecordBatch>;
type DemuxedStreamReceiver = UnboundedReceiver<(Path, RecordBatchReceiver)>;

/// Splits a single [SendableRecordBatchStream] into a dynamically determined
/// number of partitions at execution time. The partitions are determined by
/// factors known only at execution time, such as total number of rows and
/// partition column values. The demuxer task communicates to the caller
/// by sending channels over a channel. The inner channels send RecordBatches
/// which should be contained within the same output file. The outer channel
/// is used to send a dynamic number of inner channels, representing a dynamic
/// number of total output files. The caller is also responsible to monitor
/// the demux task for errors and abort accordingly. The single_file_ouput parameter
/// overrides all other settings to force only a single file to be written.
/// partition_by parameter will additionally split the input based on the unique
/// values of a specific column `<https://github.com/apache/arrow-datafusion/issues/7744>``
///                                                                              ┌───────────┐               ┌────────────┐    ┌─────────────┐
///                                                                     ┌──────▶ │  batch 1  ├────▶...──────▶│   Batch a  │    │ Output File1│
///                                                                     │        └───────────┘               └────────────┘    └─────────────┘
///                                                                     │
///                                                 ┌──────────┐        │        ┌───────────┐               ┌────────────┐    ┌─────────────┐
/// ┌───────────┐               ┌────────────┐      │          │        ├──────▶ │  batch a+1├────▶...──────▶│   Batch b  │    │ Output File2│
/// │  batch 1  ├────▶...──────▶│   Batch N  ├─────▶│  Demux   ├────────┤ ...    └───────────┘               └────────────┘    └─────────────┘
/// └───────────┘               └────────────┘      │          │        │
///                                                 └──────────┘        │        ┌───────────┐               ┌────────────┐    ┌─────────────┐
///                                                                     └──────▶ │  batch d  ├────▶...──────▶│   Batch n  │    │ Output FileN│
///                                                                              └───────────┘               └────────────┘    └─────────────┘
pub(crate) fn start_demuxer_task(
    input: SendableRecordBatchStream,
    context: &Arc<TaskContext>,
    partition_by: Option<Vec<(String, DataType)>>,
    base_output_path: Path,
    file_extension: String,
) -> (JoinHandle<Result<()>>, DemuxedStreamReceiver) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let context = context.clone();

    let task: JoinHandle<std::result::Result<(), DataFusionError>> = match partition_by {
        Some(parts) => {
            // There could be an arbitrarily large number of parallel hive style partitions being written to, so we cannot
            // bound this channel without risking a deadlock.
            tokio::spawn(async move {
                hive_style_partitions_demuxer(
                    tx,
                    input,
                    context,
                    parts,
                    base_output_path,
                    file_extension,
                )
                .await
            })
        }

        // This is implemented in the DF repo.
        None => unimplemented!(),
    };

    (task, rx)
}

/// Splits an input stream based on the distinct values of a set of columns
/// Assumes standard hive style partition paths such as
/// /col1=val1/col2=val2/outputfile.parquet
async fn hive_style_partitions_demuxer(
    tx: UnboundedSender<(Path, Receiver<RecordBatch>)>,
    mut input: SendableRecordBatchStream,
    context: Arc<TaskContext>,
    partition_by: Vec<(String, DataType)>,
    base_output_path: Path,
    file_extension: String,
) -> Result<()> {
    let write_id = rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 16);

    let exec_options = &context.session_config().options().execution;
    let max_buffered_recordbatches = exec_options.max_buffered_batches_per_output_file;

    // To support non string partition col types, cast the type to &str first
    let mut value_map: HashMap<Vec<String>, Sender<RecordBatch>> = HashMap::new();

    while let Some(rb) = input.next().await.transpose()? {
        // First compute partition key for each row of batch, e.g. (col1=val1, col2=val2, ...)
        let all_partition_values = compute_partition_keys_by_row(&rb, &partition_by)?;

        // Next compute how the batch should be split up to take each distinct key to its own batch
        let take_map = compute_take_arrays(&rb, all_partition_values);

        // Divide up the batch into distinct partition key batches and send each batch
        for (part_key, mut builder) in take_map.into_iter() {
            // Take method adapted from https://github.com/lancedb/lance/pull/1337/files
            // TODO: upstream RecordBatch::take to arrow-rs
            let take_indices = builder.finish();
            let struct_array: StructArray = rb.clone().into();
            let parted_batch =
                RecordBatch::from(take(&struct_array, &take_indices, None)?.as_struct());

            // Get or create channel for this batch
            let part_tx = match value_map.get_mut(&part_key) {
                Some(part_tx) => part_tx,
                None => {
                    // Create channel for previously unseen distinct partition key and notify consumer of new file
                    let (part_tx, part_rx) =
                        tokio::sync::mpsc::channel::<RecordBatch>(max_buffered_recordbatches);
                    let file_path = compute_hive_style_file_path(
                        &part_key,
                        &partition_by,
                        &write_id,
                        &file_extension,
                        &base_output_path,
                    );

                    tx.send((file_path, part_rx)).map_err(|_| {
                        DataFusionError::Execution("Error sending new file stream!".into())
                    })?;

                    value_map.insert(part_key.clone(), part_tx);
                    value_map
                        .get_mut(&part_key)
                        .ok_or(DataFusionError::Internal(
                            "Key must exist since it was just inserted!".into(),
                        ))?
                }
            };

            // remove partitions columns
            let final_batch_to_send = remove_partition_by_columns(&parted_batch, &partition_by)?;

            // Finally send the partial batch partitioned by distinct value!
            part_tx.send(final_batch_to_send).await.map_err(|_| {
                DataFusionError::Internal("Unexpected error sending parted batch!".into())
            })?;
        }
    }

    Ok(())
}

fn compute_partition_keys_by_row<'a>(
    rb: &'a RecordBatch,
    partition_by: &'a [(String, DataType)],
) -> Result<Vec<Vec<&'a str>>> {
    let mut all_partition_values = vec![];

    for (col, dtype) in partition_by.iter() {
        let mut partition_values = vec![];
        let col_array = rb
            .column_by_name(col)
            .ok_or(DataFusionError::Execution(format!(
                "PartitionBy Column {} does not exist in source data!",
                col
            )))?;

        match dtype {
            DataType::Utf8 => {
                let array = as_string_array(col_array)?;
                for i in 0..rb.num_rows() {
                    partition_values.push(array.value(i));
                }
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "it is not yet supported to write to hive partitions with datatype {}",
                    dtype
                )))
            }
        }

        all_partition_values.push(partition_values);
    }

    Ok(all_partition_values)
}

fn compute_take_arrays(
    rb: &RecordBatch,
    all_partition_values: Vec<Vec<&str>>,
) -> HashMap<Vec<String>, UInt64Builder> {
    let mut take_map = HashMap::new();
    for i in 0..rb.num_rows() {
        let mut part_key = vec![];
        for vals in all_partition_values.iter() {
            part_key.push(vals[i].to_owned());
        }
        let builder = take_map.entry(part_key).or_insert(UInt64Builder::new());
        builder.append_value(i as u64);
    }
    take_map
}


// This function differs to the original DF code.
// DF assumes that partition columns will always be at the end of the
// of the record batch, but that doesn't map to the way we handle
// partition columns.
//
// I believe DF currently only supports inserts, and not copy to -
// Partitioned columns have to be declared during table creation
// and those columns are always appended to the end of the schema.
// We could be copying from any supported source, so we wan't make
// assumptions about the order of the columns.
fn remove_partition_by_columns(
    parted_batch: &RecordBatch,
    partition_by: &[(String, DataType)],
) -> Result<RecordBatch> {
    let schema = parted_batch.schema();
    let partition_names: Vec<_> = partition_by.iter().map(|(s, _)| s).collect();

    let indices: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, f)| {
            if !partition_names.contains(&f.name()) {
                Some(i)
            } else {
                None
            }
        })
        .collect();

    let projected = parted_batch.project(indices.as_slice())?;
    Ok(projected)
}

fn compute_hive_style_file_path(
    part_key: &[String],
    partition_by: &[(String, DataType)],
    write_id: &str,
    file_extension: &str,
    base_output_path: &Path,
) -> Path {
    // let mut file_path = base_output_path.prefix().clone();
    let mut file_path = base_output_path.clone();
    for j in 0..part_key.len() {
        file_path = file_path.child(format!("{}={}", partition_by[j].0, part_key[j]));
    }

    file_path.child(format!("{}.{}", write_id, file_extension))
}

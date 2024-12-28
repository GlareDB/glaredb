use std::collections::VecDeque;
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use rayexec_error::{not_implemented, RayexecError, Result, ResultExt};
use rayexec_execution::arrays::batch::Batch2;
use rayexec_execution::arrays::datatype::{DataType, DecimalTypeMeta, TimeUnit, TimestampTypeMeta};
use rayexec_execution::arrays::field::{Field, Schema};
use rayexec_execution::arrays::scalar::decimal::{Decimal128Type, DecimalType};
use rayexec_execution::storage::table_storage::Projections;
use rayexec_io::location::{AccessConfig, FileLocation};
use rayexec_io::{FileProvider, FileSource};
use rayexec_parquet::metadata::Metadata;
use rayexec_parquet::reader::AsyncBatchReader;
use serde_json::Deserializer;

use super::action::Action;
use super::schema::{StructField, StructType};
use super::snapshot::Snapshot;
use crate::protocol::schema::{PrimitiveType, SchemaType};

/// Relative path to delta log files.
const DELTA_LOG_PATH: &str = "_delta_log";

#[derive(Debug)]
pub struct Table {
    /// Root of the table.
    root: FileLocation,
    /// Provider for accessing files.
    provider: Arc<dyn FileProvider>,
    conf: AccessConfig,
    /// Snapshot of the table, including what files we have available to use for
    /// reading.
    snapshot: Snapshot,
}

impl Table {
    pub async fn create(
        _root: FileLocation,
        _schema: Schema,
        _provider: Arc<dyn FileProvider>,
        _conf: AccessConfig,
    ) -> Result<Self> {
        unimplemented!()
    }

    /// Try to load a table at the given location.
    pub async fn load(
        root: FileLocation,
        provider: Arc<dyn FileProvider>,
        conf: AccessConfig,
    ) -> Result<Self> {
        // TODO: Look at checkpoints & compacted logs
        let log_root = root.join([DELTA_LOG_PATH])?;
        let mut log_stream = provider.list_prefix(log_root.clone(), &conf);

        let first_page = log_stream
            .try_next()
            .await?
            .ok_or_else(|| RayexecError::new("No logs for delta table"))?;

        let mut snapshot = match first_page.first() {
            Some(first) => {
                let actions =
                    Self::read_actions_from_log(provider.as_ref(), &conf, &log_root, first).await?;
                Snapshot::try_new_from_actions(actions)?
            }
            None => {
                return Err(RayexecError::new(
                    "No logs in first page returned from provider",
                ))
            }
        };

        // Apply rest of first page.
        for log_path in first_page.iter().skip(1) {
            let actions =
                Self::read_actions_from_log(provider.as_ref(), &conf, &log_root, log_path).await?;
            snapshot.apply_actions(actions)?;
        }

        // Apply rest of log stream.
        while let Some(page) = log_stream.try_next().await? {
            for log_path in page {
                let actions =
                    Self::read_actions_from_log(provider.as_ref(), &conf, &log_root, &log_path)
                        .await?;
                snapshot.apply_actions(actions)?;
            }
        }

        Ok(Table {
            root,
            provider,
            conf,
            snapshot,
        })
    }

    // TODO: Maybe don't allocate new vec for every log file.
    async fn read_actions_from_log(
        provider: &dyn FileProvider,
        conf: &AccessConfig,
        root: &FileLocation,
        path: &str,
    ) -> Result<Vec<Action>> {
        let bytes = provider
            .file_source(root.join([path])?, conf)? // TODO: Path segments
            .read_stream()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        // TODO: Either move this to a utility, or avoid doing it.
        let bytes = bytes.into_iter().fold(Vec::new(), |mut v, buf| {
            v.extend_from_slice(buf.as_ref());
            v
        });

        let actions = Deserializer::from_slice(&bytes)
            .into_iter::<Action>()
            .collect::<Result<Vec<_>, _>>()
            .context("failed to read actions from log file")?;

        Ok(actions)
    }

    pub fn table_schema(&self) -> Result<Schema> {
        let schema = self.snapshot.schema()?;
        schema_from_struct_type(schema)
    }

    // TODO: batch size, projection
    // TODO: Reference partition values.
    // TODO: Properly filter based on deletion vector.
    pub fn scan(&self, projections: Projections, num_partitions: usize) -> Result<Vec<TableScan>> {
        // Each partitions gets some subset of files.
        let mut paths: Vec<_> = (0..num_partitions).map(|_| VecDeque::new()).collect();

        for (idx, file_key) in self.snapshot.add.keys().enumerate() {
            let partition = idx % num_partitions;
            paths[partition].push_back(file_key.path.clone());
        }

        let schema = self.table_schema()?;

        let scans = paths
            .into_iter()
            .map(|partition_paths| TableScan {
                root: self.root.clone(),
                schema: schema.clone(),
                projections: projections.clone(),
                paths: partition_paths,
                provider: self.provider.clone(),
                conf: self.conf.clone(),
                current: None,
            })
            .collect();

        Ok(scans)
    }
}

#[derive(Debug)]
pub struct TableScan {
    root: FileLocation,
    /// Schema of the table as determined by the metadata action.
    schema: Schema,
    /// Root column projections.
    projections: Projections,
    /// Paths to data files this scan should read one after another.
    paths: VecDeque<String>,
    /// File provider for getting the actual file sources.
    provider: Arc<dyn FileProvider>,
    conf: AccessConfig,
    /// Current reader, initially empty and populated on first stream.
    ///
    /// Once a reader runs out, the next file is loaded, and gets placed here.
    current: Option<AsyncBatchReader<Box<dyn FileSource>>>,
}

impl TableScan {
    /// Read the next batch.
    pub async fn read_next(&mut self) -> Result<Option<Batch2>> {
        loop {
            if self.current.is_none() {
                let path = match self.paths.pop_front() {
                    Some(path) => path,
                    None => return Ok(None), // We're done.
                };

                self.current = Some(
                    Self::load_reader(
                        &self.root,
                        &self.conf,
                        path,
                        self.provider.as_ref(),
                        &self.schema,
                        self.projections.clone(),
                    )
                    .await?,
                )
            }

            match self.current.as_mut().unwrap().read_next().await {
                Ok(Some(batch)) => return Ok(Some(batch)),
                Ok(None) => {
                    // Loads next read at beginning of loop.
                    self.current = None;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn load_reader(
        root: &FileLocation,
        conf: &AccessConfig,
        path: String,
        provider: &dyn FileProvider,
        schema: &Schema,
        projections: Projections,
    ) -> Result<AsyncBatchReader<Box<dyn FileSource>>> {
        // TODO: Need to split path into segments.
        let location = root.join([path])?;
        let mut source = provider.file_source(location, conf)?;

        let size = source.size().await?;
        let metadata = Arc::new(Metadata::new_from_source(source.as_mut(), size).await?);
        let row_groups: VecDeque<_> = (0..metadata.decoded_metadata.row_groups().len()).collect();

        const BATCH_SIZE: usize = 4096; // TODO
        let reader = AsyncBatchReader::try_new(
            source,
            row_groups,
            metadata,
            schema,
            BATCH_SIZE,
            projections,
        )?;

        Ok(reader)
    }
}

/// Create a schema from a struct type representing the schema of a delta table.
pub fn schema_from_struct_type(typ: StructType) -> Result<Schema> {
    let fields = typ
        .fields
        .into_iter()
        .map(struct_field_to_field)
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new(fields))
}

fn struct_field_to_field(field: StructField) -> Result<Field> {
    let datatype = match field.typ {
        SchemaType::Primitive(prim) => match prim {
            PrimitiveType::String => DataType::Utf8,
            PrimitiveType::Long => DataType::Int64,
            PrimitiveType::Integer => DataType::Int32,
            PrimitiveType::Short => DataType::Int16,
            PrimitiveType::Byte => DataType::Int8,
            PrimitiveType::Float => DataType::Float32,
            PrimitiveType::Double => DataType::Float64,
            PrimitiveType::Decimal => DataType::Decimal128(DecimalTypeMeta::new(
                Decimal128Type::MAX_PRECISION,
                Decimal128Type::DEFAULT_SCALE,
            )),
            PrimitiveType::Boolean => DataType::Boolean,
            PrimitiveType::Binary => DataType::Binary,
            PrimitiveType::Date => DataType::Timestamp(TimestampTypeMeta::new(TimeUnit::Second)), // TODO: This is just year/month/day
            PrimitiveType::Timestamp => {
                DataType::Timestamp(TimestampTypeMeta::new(TimeUnit::Microsecond))
            }
        },
        SchemaType::Struct(_) => not_implemented!("delta struct"),
        SchemaType::Array(_) => not_implemented!("delta array"),
        SchemaType::Map(_) => not_implemented!("delta map"),
    };

    Ok(Field::new(field.name, datatype, field.nullable))
}

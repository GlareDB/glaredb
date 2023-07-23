use crate::common::url::DatasourceUrl;
use crate::lake::iceberg::errors::{IcebergError, Result};
use crate::lake::iceberg::spec::{ManifestList, TableMetadata};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::avro_to_arrow as avro;
use datafusion::datasource::TableProvider;
use object_store::{path::Path as ObjectPath, ObjectStore};
use std::io::Cursor;
use std::sync::Arc;

#[derive(Debug)]
pub struct IcebergTable {
    /// The root of the table.
    location: DatasourceUrl,

    /// Store for accessing the table.
    store: Arc<dyn ObjectStore>,

    metadata: TableMetadata,
}

impl IcebergTable {
    /// Open a table at a location using the provided object store.
    pub async fn open(
        location: DatasourceUrl,
        store: Arc<dyn ObjectStore>,
    ) -> Result<IcebergTable> {
        // Get table version.
        let version = {
            let path = format_object_path(&location, "metadata/version-hint.text")?;
            let path = ObjectPath::parse(path)?;
            let bs = store.get(&path).await?.bytes().await?;
            let s = String::from_utf8(bs.to_vec()).map_err(|e| {
                IcebergError::DataInvalid(format!("Expected utf-8 in version hint: {}", e))
            })?;

            let table_version = s.parse::<i32>().map_err(|e| {
                IcebergError::DataInvalid(format!("Version hint to be a number: {}", e))
            })?;

            table_version
        };

        // Read metadata.
        let metadata = {
            let path = format_object_path(&location, format!("metadata/v{version}.metadata.json"))?;
            let bs = store.get(&path).await?.bytes().await?;
            let metadata: TableMetadata = serde_json::from_slice(&bs).map_err(|e| {
                IcebergError::DataInvalid(format!("Failed to read table metadata: {}", e))
            })?;
            metadata
        };

        Ok(IcebergTable {
            location,
            store,
            metadata,
        })
    }

    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    pub async fn table_reader(&self) -> Result<Arc<dyn TableProvider>> {
        let current_snapshot_id = self
            .metadata
            .current_snapshot_id
            .ok_or_else(|| IcebergError::DataInvalid("Missing current snapshot id".to_string()))?;

        let current_snapshot = self
            .metadata
            .snapshots
            .iter()
            .find(|s| s.snapshot_id == current_snapshot_id)
            .ok_or_else(|| {
                IcebergError::DataInvalid(format!(
                    "Missing snapshot for id: {}",
                    current_snapshot_id
                ))
            })?;

        // Read manifest list from snapshot.
        let manifest_list = {
            let manifest_list_path = self.relative_path(&current_snapshot.manifest_list);

            let path = format_object_path(&self.location, manifest_list_path)?;
            let bs = self.store.get(&path).await?.bytes().await?;

            let mut cursor = Cursor::new(bs);

            // HACK: Avro reader will panic since it thinks there's a null in
            // the partitions list during array validation. I'm not sure why yet
            // though.
            let schema = avro::read_avro_schema_from_reader(&mut cursor)?;
            let fields: Vec<_> = schema
                .fields
                .iter()
                .map(|f| {
                    if f.name() == "partitions" {
                        let struct_field = Field::new_struct(
                            "struct",
                            vec![
                                Field::new("r508.contains_null", DataType::Boolean, false),
                                Field::new("r508.contains_nan", DataType::Boolean, true),
                                Field::new("r508.lower_bound", DataType::Binary, true),
                                Field::new("r508.upper_bound", DataType::Binary, true),
                            ],
                            true, // This is the difference. It get inferred as 'false' by the reader.
                        );
                        Field::new_list("partitions", struct_field, true)
                    } else {
                        Field::new(f.name(), f.data_type().clone(), f.is_nullable())
                    }
                })
                .collect();
            let schema = Schema::new(fields);

            let mut reader = avro::ReaderBuilder::new()
                .with_schema(Arc::new(schema))
                .build(cursor)?;

            let batch = reader.next().transpose()?.ok_or_else(|| {
                IcebergError::DataInvalid("No data found in manifest list".to_string())
            })?;

            datafusion::arrow::util::pretty::print_batches(&[batch.clone()]).unwrap();

            ManifestList::try_from_batch(batch)?
        };

        let data_files = {
            // let mut data_files = Vec::new();

            for ent in manifest_list.entries {
                let manifest_path = self.relative_path(&ent.manifest_path);

                let path = format_object_path(&self.location, manifest_path)?;
                let bs = self.store.get(&path).await?.bytes().await?;

                let mut cursor = Cursor::new(bs);

                let schema = avro::read_avro_schema_from_reader(&mut cursor)?;
                println!("schema: {schema:#?}");
            }
        };

        unimplemented!()
    }

    /// Get the relative path for a file according to the table's metadata.
    ///
    /// File paths in the table metadata and manifests will include the base
    /// table's location, so we need to remove that when accessing those files.
    ///
    /// E.g.
    /// manifest-list => out/iceberg_table/metadata/snap-4160073268445560424-1-095d0ad9-385f-406f-b29c-966a6e222e58.avro
    /// location      => out/iceberg_table
    ///
    /// This should give us:
    /// metadata/snap-4160073268445560424-1-095d0ad9-385f-406f-b29c-966a6e222e58.avro
    fn relative_path<'a, 'b>(&'a self, path: &'b str) -> &'b str {
        path.trim_start_matches(&self.metadata.location)
            .trim_matches('/')
    }
}

/// Formats an object path depending on if it's a url (for real object stores),
/// or if it's a local path.
fn format_object_path(
    url: &DatasourceUrl,
    path: impl AsRef<str>,
) -> Result<ObjectPath, object_store::path::Error> {
    let path = path.as_ref();
    match url {
        DatasourceUrl::Url(_) => {
            let path = format!("{}/{path}", url.path());
            ObjectPath::parse(path)
        }
        DatasourceUrl::File(root_path) => {
            let path = root_path.join(path);
            ObjectPath::from_filesystem_path(path)
        }
    }
}

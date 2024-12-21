pub mod read_postgres;

mod decimal;

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use decimal::PostgresDecimal;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{StreamExt, TryFutureExt};
use rayexec_bullet::array::Array;
use rayexec_bullet::batch::BatchOld;
use rayexec_bullet::datatype::{DataType, DecimalTypeMeta};
use rayexec_bullet::field::Field;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_execution::database::catalog_entry::{CatalogEntry, TableEntry};
use rayexec_execution::database::memory_catalog::MemoryCatalog;
use rayexec_execution::datasource::{
    check_options_empty,
    take_option,
    DataSource,
    DataSourceBuilder,
    DataSourceConnection,
};
use rayexec_execution::functions::table::TableFunction;
use rayexec_execution::runtime::{Runtime, TokioHandlerProvider};
use rayexec_execution::storage::catalog_storage::CatalogStorage;
use rayexec_execution::storage::table_storage::{
    DataTable,
    DataTableScan,
    EmptyTableScan,
    ProjectedScan,
    Projections,
    TableStorage,
};
use read_postgres::ReadPostgres;
use tokio_postgres::binary_copy::{BinaryCopyOutRow, BinaryCopyOutStream};
use tokio_postgres::types::{FromSql, Type as PostgresType};
use tokio_postgres::NoTls;
use tracing::debug;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresDataSource<R: Runtime> {
    runtime: R,
}

impl<R: Runtime> DataSourceBuilder<R> for PostgresDataSource<R> {
    fn initialize(runtime: R) -> Box<dyn DataSource> {
        Box::new(PostgresDataSource { runtime })
    }
}

impl<R: Runtime> DataSource for PostgresDataSource<R> {
    fn connect(
        &self,
        options: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<'_, Result<DataSourceConnection>> {
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let connection = Arc::new(PostgresConnection::connect(runtime, options).await?);

            Ok(DataSourceConnection {
                catalog_storage: Some(connection.clone()),
                table_storage: connection,
            })
        })
    }

    fn initialize_table_functions(&self) -> Vec<Box<dyn TableFunction>> {
        vec![Box::new(ReadPostgres {
            runtime: self.runtime.clone(),
        })]
    }
}

#[derive(Debug, Clone)]
pub struct PostgresConnection<R: Runtime> {
    _runtime: R,
    client: PostgresClient,
}

impl<R: Runtime> PostgresConnection<R> {
    async fn connect(runtime: R, mut options: HashMap<String, OwnedScalarValue>) -> Result<Self> {
        let conn_str = take_option("connection_string", &mut options)?.try_into_string()?;
        check_options_empty(&options)?;

        // Check we can connect.
        let client = PostgresClient::connect(&conn_str, &runtime).await?;

        let _ = client
            .client
            .query("select 1", &[])
            .await
            .context("Failed to send test query")?;

        Ok(Self {
            _runtime: runtime,
            client,
        })
    }
}

impl<R: Runtime> CatalogStorage for PostgresConnection<R> {
    fn initial_load(&self, _catalog: &MemoryCatalog) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { Ok(()) })
    }

    fn persist(&self, _catalog: &MemoryCatalog) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { Ok(()) })
    }

    fn load_table(&self, schema: &str, name: &str) -> BoxFuture<'_, Result<Option<TableEntry>>> {
        // TODO: We can avoid this, need to change lifetimes on trait.
        let schema = schema.to_string();
        let name = name.to_string();

        Box::pin(async move {
            let fields = match self.client.get_fields_and_types(&schema, &name).await? {
                Some((fields, _)) => fields,
                None => return Ok(None),
            };

            Ok(Some(TableEntry { columns: fields }))
        })
    }
}

impl<R: Runtime> TableStorage for PostgresConnection<R> {
    fn data_table(&self, schema: &str, ent: &CatalogEntry) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(PostgresDataTable {
            client: self.client.clone(),
            schema: schema.to_string(),
            table: ent.name.clone(),
        }))
    }

    fn create_physical_table(
        &self,
        _schema: &str,
        _ent: &CatalogEntry,
    ) -> BoxFuture<'_, Result<Box<dyn DataTable>>> {
        Box::pin(async {
            Err(RayexecError::new(
                "Create physical table unsupported (postgres)",
            ))
        })
    }

    fn drop_physical_table(&self, _schema: &str, _ent: &CatalogEntry) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            Err(RayexecError::new(
                "Drop physical table unsupported (postgres)",
            ))
        })
    }
}

#[derive(Debug)]
pub struct PostgresDataTable {
    pub(crate) client: PostgresClient,
    pub(crate) schema: String,
    pub(crate) table: String,
}

impl DataTable for PostgresDataTable {
    fn scan(
        &self,
        projections: Projections,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn DataTableScan>>> {
        let schema = self.schema.clone();
        let table = self.table.clone();

        let client = self.client.clone();

        let binary_copy_open = async move {
            // TODO: Remove this, we should already have the types.
            let (fields, typs) = match client.get_fields_and_types(&schema, &table).await? {
                Some((fields, typs)) => (fields, typs),
                None => return Err(RayexecError::new("Missing table")),
            };

            let projection_string = fields
                .iter()
                .map(|field| field.name.clone())
                .collect::<Vec<_>>()
                .join(", ");

            let data_types: Vec<_> = fields.into_iter().map(|field| field.datatype).collect();

            let query = format!(
                "COPY (SELECT {} FROM {}.{}) TO STDOUT (FORMAT binary)",
                projection_string, // SELECT <str>
                schema,            // FROM <schema>
                table,             // .<table>
            );

            let copy_stream = client
                .client
                .copy_out(&query)
                .await
                .context("Failed to create copy out stream")?;
            let copy_stream = BinaryCopyOutStream::new(copy_stream, &typs);
            // let copy_stream = BinaryCopyOutStream::new(copy_stream,)
            let chunked = copy_stream.chunks(1024).boxed(); // TODO: Batch size

            let batch_stream = chunked.map(move |rows| {
                let rows = rows
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .context("Failed to collect binary rows")?;
                let batch = PostgresClient::binary_rows_to_batch(&data_types, rows)?;
                Ok(batch)
            });

            Ok(batch_stream)
        };

        let binary_copy_stream = binary_copy_open.try_flatten_stream().boxed();

        let mut scans = vec![Box::new(ProjectedScan::new(
            PostgresDataTableScan {
                stream: binary_copy_stream,
            },
            projections,
        )) as _];

        // Extend with empty scans...
        (1..num_partitions).for_each(|_| scans.push(Box::new(EmptyTableScan) as _));

        Ok(scans)
    }
}

pub struct PostgresDataTableScan {
    stream: BoxStream<'static, Result<BatchOld>>,
}

impl DataTableScan for PostgresDataTableScan {
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<BatchOld>>> {
        Box::pin(async { self.stream.next().await.transpose() })
    }
}

impl fmt::Debug for PostgresDataTableScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresDataTableScan").finish()
    }
}

#[derive(Debug, Clone)]
struct PostgresClient {
    client: Arc<tokio_postgres::Client>,
    // TODO: Runtime spawn handle
}

impl PostgresClient {
    async fn connect<R: Runtime>(conn_str: impl Into<String>, runtime: &R) -> Result<Self> {
        let tokio_handle = runtime.tokio_handle().handle()?;

        let conn_str = conn_str.into();
        let (client, connection) = tokio_handle
            .spawn(async move {
                let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
                Ok::<_, tokio_postgres::Error>((client, connection))
            })
            .await
            .context("Join error")?
            .context("Failed to connect to postgres instance")?;

        // TODO: Doesn't need to be on tokio.
        tokio_handle.spawn(async move {
            if let Err(e) = connection.await {
                debug!(%e, "postgres connection errored");
            }
        });

        Ok(PostgresClient {
            client: Arc::new(client),
        })
    }

    async fn get_fields_and_types(
        &self,
        schema: &str,
        name: &str,
    ) -> Result<Option<(Vec<Field>, Vec<PostgresType>)>> {
        // Get oid of table, and approx number of pages for the relation.
        let mut rows = self
            .client
            .query(
                "
                SELECT
                    pg_class.oid,
                    GREATEST(relpages, 1)
                FROM pg_class INNER JOIN pg_namespace ON relnamespace = pg_namespace.oid
                WHERE nspname=$1 AND relname=$2;
                ",
                &[&schema, &name],
            )
            .await
            .context("Failed to get table OID and page size")?;
        // Should only return 0 or 1 row. If 0 rows, then table/schema doesn't
        // exist.
        let row = match rows.pop() {
            Some(row) => row,
            None => return Ok(None),
        };
        let oid: u32 = row.try_get(0).context("Missing OID for table")?;

        // TODO: Get approx pages to allow us to calculate number of pages to
        // scan per thread once we do parallel scanning.
        // let approx_pages: i64 = row.try_get(1)?;

        // Get table schema.
        let rows = self
            .client
            .query(
                "
                SELECT
                    attname,
                    pg_type.oid
                FROM pg_attribute
                    INNER JOIN pg_type ON atttypid=pg_type.oid
                WHERE attrelid=$1 AND attnum > 0
                ORDER BY attnum;
                ",
                &[&oid],
            )
            .await
            .context("Failed to get column metadata for table")?;

        let mut names: Vec<String> = Vec::with_capacity(rows.len());
        let mut type_oids: Vec<u32> = Vec::with_capacity(rows.len());
        for row in rows {
            names.push(row.try_get(0).context("Missing column name")?);
            type_oids.push(row.try_get(1).context("Missing type OID")?);
        }

        let pg_types = type_oids
            .iter()
            .map(|oid| {
                PostgresType::from_oid(*oid)
                    .ok_or_else(|| RayexecError::new("Unknown postgres OID: {oid}"))
            })
            .collect::<Result<Vec<_>>>()?;

        let fields = Self::fields_from_columns(names, &pg_types)?;

        Ok(Some((fields, pg_types)))
    }

    fn fields_from_columns(names: Vec<String>, typs: &[PostgresType]) -> Result<Vec<Field>> {
        let mut fields = Vec::with_capacity(names.len());

        for (name, typ) in names.into_iter().zip(typs) {
            let dt = match typ {
                &PostgresType::BOOL => DataType::Boolean,
                &PostgresType::INT2 => DataType::Int16,
                &PostgresType::INT4 => DataType::Int32,
                &PostgresType::INT8 => DataType::Int64,
                &PostgresType::FLOAT4 => DataType::Float32,
                &PostgresType::FLOAT8 => DataType::Float64,
                &PostgresType::CHAR
                | &PostgresType::BPCHAR
                | &PostgresType::VARCHAR
                | &PostgresType::TEXT
                | &PostgresType::JSONB
                | &PostgresType::JSON
                | &PostgresType::UUID => DataType::Utf8,
                &PostgresType::BYTEA => DataType::Binary,
                // While postgres numerics are "unconstrained" by default, we need
                // to specify the precision and scale for the column. Setting these
                // same as bigquery.
                &PostgresType::NUMERIC => DataType::Decimal128(DecimalTypeMeta::new(38, 9)),

                other => {
                    return Err(RayexecError::new(format!(
                        "Unsupported postgres type: {other}"
                    )))
                }
            };

            fields.push(Field::new(name, dt, true));
        }

        Ok(fields)
    }

    fn binary_rows_to_batch(typs: &[DataType], rows: Vec<BinaryCopyOutRow>) -> Result<BatchOld> {
        fn row_iter<'a, T: FromSql<'a>>(
            rows: &'a [BinaryCopyOutRow],
            idx: usize,
        ) -> impl Iterator<Item = Option<T>> + 'a {
            rows.iter().map(move |row| row.try_get(idx).ok())
        }

        let mut arrays = Vec::with_capacity(typs.len());
        for (idx, typ) in typs.iter().enumerate() {
            let arr = match typ {
                DataType::Boolean => Array::from_iter(row_iter::<bool>(&rows, idx)),
                DataType::Int8 => Array::from_iter(row_iter::<i8>(&rows, idx)),
                DataType::Int16 => Array::from_iter(row_iter::<i16>(&rows, idx)),
                DataType::Int32 => Array::from_iter(row_iter::<i32>(&rows, idx)),
                DataType::Int64 => Array::from_iter(row_iter::<i64>(&rows, idx)),
                DataType::Decimal128(m) => {
                    let primitives = Array::from_iter(rows.iter().map(|row| {
                        let decimal = row.try_get::<PostgresDecimal>(idx).ok();
                        // TODO: Rescale
                        decimal.map(|d| d.0.value)
                    }));

                    match primitives.validity() {
                        Some(validity) => Array::new_with_validity_and_array_data(
                            DataType::Decimal128(DecimalTypeMeta::new(m.precision, m.scale)),
                            validity.clone(),
                            primitives.array_data().clone(),
                        ),
                        None => Array::new_with_array_data(
                            DataType::Decimal128(DecimalTypeMeta::new(m.precision, m.scale)),
                            primitives.array_data().clone(),
                        ),
                    }
                }

                DataType::Utf8 => Array::from_iter(
                    rows.iter()
                        .map(|row| -> Option<&str> { row.try_get(idx).ok() }),
                ),
                other => {
                    return Err(RayexecError::new(format!(
                        "Unimplemented data type conversion: {other:?} (postgres)"
                    )))
                }
            };
            arrays.push(arr);
        }

        BatchOld::try_new(arrays)
    }
}

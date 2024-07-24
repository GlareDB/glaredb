pub mod read_postgres;

use futures::{future::BoxFuture, stream::BoxStream, StreamExt, TryFutureExt};
use rayexec_bullet::{
    array::{Array, BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array},
    batch::Batch,
    datatype::DataType,
    field::Field,
    scalar::OwnedScalarValue,
};
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_execution::{
    database::{
        catalog::{Catalog, CatalogTx},
        entry::TableEntry,
        table::{DataTable, DataTableScan, EmptyTableScan},
    },
    datasource::{check_options_empty, take_option, DataSource},
    functions::table::TableFunction,
    runtime::ExecutionRuntime,
};
use read_postgres::ReadPostgres;
use std::fmt;
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::{
    binary_copy::{BinaryCopyOutRow, BinaryCopyOutStream},
    types::Type as PostgresType,
};
use tokio_postgres::{types::FromSql, NoTls};
use tracing::debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostgresDataSource;

impl DataSource for PostgresDataSource {
    fn create_catalog(
        &self,
        runtime: &Arc<dyn ExecutionRuntime>,
        options: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<Result<Box<dyn Catalog>>> {
        Box::pin(self.create_catalog_inner(runtime.clone(), options))
    }

    fn initialize_table_functions(&self) -> Vec<Box<dyn TableFunction>> {
        vec![Box::new(ReadPostgres)]
    }
}

impl PostgresDataSource {
    async fn create_catalog_inner(
        &self,
        runtime: Arc<dyn ExecutionRuntime>,
        mut options: HashMap<String, OwnedScalarValue>,
    ) -> Result<Box<dyn Catalog>> {
        let conn_str = take_option("connection_string", &mut options)?.try_into_string()?;
        check_options_empty(&options)?;

        // Check we can connect.
        let client = PostgresClient::connect(&conn_str, runtime.as_ref()).await?;

        let _ = client
            .client
            .query("select 1", &[])
            .await
            .context("Failed to send test query")?;

        Ok(Box::new(PostgresCatalog { runtime, conn_str }))
    }
}

#[derive(Debug)]
pub struct PostgresCatalog {
    runtime: Arc<dyn ExecutionRuntime>,
    // TODO: Connection pooling.
    conn_str: String,
}

impl Catalog for PostgresCatalog {
    fn get_table_entry(
        &self,
        _tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> BoxFuture<Result<Option<TableEntry>>> {
        let client = PostgresClient::connect(&self.conn_str, self.runtime.as_ref());
        let schema = schema.to_string();
        let name = name.to_string();
        Box::pin(async move {
            let client = client.await?;
            let fields = match client.get_fields_and_types(&schema, &name).await? {
                Some((fields, _)) => fields,
                None => return Ok(None),
            };

            Ok(Some(TableEntry {
                name: name.to_string(),
                columns: fields,
            }))
        })
    }

    fn data_table(
        &self,
        _tx: &CatalogTx,
        schema: &str,
        ent: &TableEntry,
    ) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(PostgresDataTable {
            runtime: self.runtime.clone(),
            conn_str: self.conn_str.clone(),
            schema: schema.to_string(),
            table: ent.name.clone(),
        }))
    }
}

#[derive(Debug)]
pub struct PostgresDataTable {
    pub(crate) runtime: Arc<dyn ExecutionRuntime>,
    pub(crate) conn_str: String,
    pub(crate) schema: String,
    pub(crate) table: String,
}

impl DataTable for PostgresDataTable {
    fn scan(&self, num_partitions: usize) -> Result<Vec<Box<dyn DataTableScan>>> {
        let runtime = self.runtime.clone();
        let conn_str = self.conn_str.clone();
        let schema = self.schema.clone();
        let table = self.table.clone();

        let binary_copy_open = async move {
            let client = PostgresClient::connect(&conn_str, runtime.as_ref()).await?;

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

        let mut scans = vec![Box::new(PostgresDataTableScan {
            stream: binary_copy_stream,
        }) as _];

        // Extend with empty scans...
        (1..num_partitions).for_each(|_| scans.push(Box::new(EmptyTableScan) as _));

        Ok(scans)
    }
}

pub struct PostgresDataTableScan {
    stream: BoxStream<'static, Result<Batch>>,
}

impl DataTableScan for PostgresDataTableScan {
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>> {
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
    async fn connect(conn_str: impl Into<String>, runtime: &dyn ExecutionRuntime) -> Result<Self> {
        let tokio_handle = runtime
            .tokio_handle()
            .ok_or_else(|| RayexecError::new("Missing tokio runtime"))?;

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

    fn binary_rows_to_batch(typs: &[DataType], rows: Vec<BinaryCopyOutRow>) -> Result<Batch> {
        fn row_iter<'a, T: FromSql<'a>>(
            rows: &'a [BinaryCopyOutRow],
            idx: usize,
        ) -> impl Iterator<Item = Option<T>> + 'a {
            rows.iter().map(move |row| row.try_get(idx).ok())
        }

        let mut arrays = Vec::with_capacity(typs.len());
        for (idx, typ) in typs.iter().enumerate() {
            let arr = match typ {
                DataType::Boolean => {
                    Array::Boolean(BooleanArray::from_iter(row_iter::<bool>(&rows, idx)))
                }
                DataType::Int8 => Array::Int8(Int8Array::from_iter(row_iter::<i8>(&rows, idx))),
                DataType::Int16 => Array::Int16(Int16Array::from_iter(row_iter::<i16>(&rows, idx))),
                DataType::Int32 => Array::Int32(Int32Array::from_iter(row_iter::<i32>(&rows, idx))),
                DataType::Int64 => Array::Int64(Int64Array::from_iter(row_iter::<i64>(&rows, idx))),
                other => {
                    return Err(RayexecError::new(format!(
                        "Unimplemented data type conversion: {other:?}"
                    )))
                }
            };
            arrays.push(arr);
        }

        Batch::try_new(arrays)
    }
}

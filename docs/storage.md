# Storage

The storage portion will be responsible for data replication, sharding, transactions, and
physical data storage.

## Storage API

A first-pass API for the storage layer might look something like this:

``` rust
/// Column indices pointing to the columns making up the primary key.
struct PrimaryKeyIndices<'a>(&[usize]);

/// A vector of value making up a row's primary key.
struct PrimaryKey(Vec<Value>);

type TableId = String;

pub trait Storage {
    async fn create_table(&self, table: TableId) -> Result<()>;
    async fn drop_table(&self, table: TableId) -> Result<()>;

    /// Insert a row for some primary key.
    async fn insert(&self, table: TableId, pk: PrimaryKeyIndices, row: Row) -> Result<()>;

    /// Get a single row.
    async fn get(&self, table: TableId, pk: PrimaryKey) -> Result<Option<Row>>;

    /// Delete a row.
    async fn delete(&self, table: TableId, pk: PrimaryKey) -> Result<()>;

    /// Scan a table, with some optional arguments.
    async fn scan(
        &self,
        table: TableId,
        begin: Option<PrimaryKey>,
        end: Option<PrimaryKey>,
        filter: Option<ScalarExpr>,
        projection: Option<Vec<ScalarExpr>>
    ) -> Result<Stream<DataFrame>>;
}
```

See the [DataFrame](https://github.com/GlareDB/glaredb/blob/3577409682122ce046709ae93512499da7253fb7/crates/lemur/src/repr/df/mod.rs), [Value](https://github.com/GlareDB/glaredb/blob/3577409682122ce046709ae93512499da7253fb7/crates/lemur/src/repr/value.rs), and [Scalar](https://github.com/GlareDB/glaredb/blob/3577409682122ce046709ae93512499da7253fb7/crates/lemur/src/repr/expr/scalar.rs) modules for the relevant
definitions for `DataFrame`, `Row`, and `ScalarExpr` respectively. Note that
this API lacks a few things, including updating tables, and providing schema
information during table creates. The primary thing to point out with this API
is that it should use that same data representation as the query engine to
enable pushing down projections and filters as a much as possible (and other
things like aggregates in the future).

## Physical Storage

We should initially use RocksDB for getting to a prototype quickly. The
tradeoff here is we might lose out on analytical performance, and lose
fine-grained control of reads/writes to and from the disk.

Keys should look something like the following:

```
<table_id>/<index_id>/<index_value_1>/<index_value_2>/.../<timestamp>
```

Breaking it down:
- **table_id**: The table identifier. This should remain constant across the
  lifetime of the table, even if the table name is changed.
- **index_id**: The identifier of the index. `0` indicates the primary index,
  and the value for a primary index should be the serialized row. Other index
  IDs correspond to additional secondary indexes. Values in such cases should be
  the composite primary key.
- **index_value_n**: The column value corresponding to the column in the index.
- **timestamp**: The transaction timestamp.


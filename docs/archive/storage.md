# Storage

The storage portion will be responsible for data replication, sharding, transactions, and physical data storage.

## Storage API

TBD

Requirements:
- Use Arrow like format as much as possible for easy
- Build with future compression in mind

## Physical Storage

We should initially use RocksDB for getting to a prototype quickly.  The tradeoff here is we might lose out on analytical performance, and lose fine-grained control of reads/writes to and from the disk.

We shall be using 2 column families.

Keys should look something like the following:

Default column family:
```
<tenant_id><table_id>/<index_id>/<index_value_1>/<index_value_2>/.../<start transaction timestamp>
```
- **tenant_id(optional)**: The tenant identifier to enable having multiple cloud users/versions of a table (dev, staging, production) share storage
- **table_id**: The table identifier. This should remain constant across the
  lifetime of the table, even if the table name is changed.
- **index_id**: The identifier of the index. `0` indicates the primary index, other values indicate the secondary indexes.
- **index_value_n**: The column value corresponding to the column in the index. This will be in serialized form
- **timestamp**: The timestamp for the start of the transaction (hybrid logical clock)
- End time stamp in separate column family (Transactional information)

- For the primary index the value will be the serialized row (for now in the datafusion_row::layout::RowType format)
- For the secondary indexes the values will be should the composite primary key (just the primary index values)
    - The rest of the key is constructed from already known information. The timestamp from the current transaction
- Comparison for the keys is also important. Ideally we would want each start transaction timestamps to be descending. This way we see the most recent provisional write and latest value first and make a determination whether we should be using that. While still allowing prefix search to work appropriately

Transactional information column family:
```
<tenant_id>/<table_id>/<index_id>/<index_value_1>/<index_value_2>/.../<start transaction timestamp>
```
- Same key as default column family
- Value will be the commit status and later the timestamp at which point the mutable operations done in the transaction are visible (i.e. committed)
- When the transaction is still in progress there is is no timestamp.

## Future considerations
- Is there a need to consider separate LSM trees per table or collection of tables within one logical database
    - Should we further partition tables into separate LSM trees
- Cleanup of old versions (configurable to keep everything, default for now, though default should be shorter so this can scale with use in cloud product)
    - To start we can have 2 configs:
        - Keep everything
        - Keep a week (or max transaction length, which can default to a week)
- Should index values in the key be in serialized form
- Should column values be stored concatenated per key
    - Alternatively store in NSMish format with column id before timestamp/HCL
    - Alternatively store in DSMish format with column id before index_id 0
- Should secondary indexes be in a separate column family
- Investigate use of manual compaction to upload sst files to object store
- Restoring db from object store sst files

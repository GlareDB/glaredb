# Exp Store

A document detailing the design of an experimental storage system.

This is a living document and should be updated as we uncover new
requirements/restrictions.

## Priorities

Primary priorities:

- Get something working with Object Storage as a proof-of-concept.
- Having a working storage system suitable for inserting and querying data.

Secondary priorities:

- Transactions.
- Durability (logging).
- Updates, deletes, table alterations.
- Secondary indexes.

Secondary priorities should be focused on after we have a POC.

## Cloud Native

A big focus for this storage system will be integrating with cloud Object
Storage. The main driver behind this is that it allows for "bottomless" storage,
a big win for analytic databases. 

Secondary motivations are object storage is cheap, and I don't want to have to
manage persistent disks containing user data.

Tertiary is this may enable a "bring your own storage" model for customer who
prefer to have complete control over their data at rest.

### Why not RocksDB/RocksDB Cloud?

RocksDB would be a very attractive solution in the following cases:

- We're ok with storing user data on persistent disks instead of in object
  storage (RocksDB Cloud alleviates this).
- We're ok with requiring users to select the amount of storage they need
  upfront for their databases. Note that we would be able to modify the amount
  of storage after database creation, but this would end up being a "manual"
  process for the user in that they would need to specify an increased amount.
- We're ok with the mismatch between the data layout in RocksDB and the query
  execution engine.

## Directory Structure

``` text
- schema_0
  - table_0
    - table_0.meta
    - table_0_part_0.data
    - table_0_part_0.delta
    - table_0_part_1.data
    - table_0_part_1.delta
  - table_1
    ...
```

The directory structure on the local disk will mimic the structure in object
storage. Entire files will be written to and read from object storage. Data
files will be split once they reach a certain size to avoid massive files.

### Meta files

Meta files hold info about the partitioning schema for the table. Specifically,
a "begin" and "end" primary key will be listed for each partition.

### Data files

``` text
table_<a>_part_<b>.data
a -> table identifier
b -> partition identifier
```

Data files contain user data, and follow the Arrow IPC format. Each file's
schema will include the appropriate data types with synthetic field names. These
synthetic field names will map to the proper field names higher up in the system.

A data file will contain multiple blocks (record batches), each prepended with
some metadata about that block.

Each file will try to reach some target size and/or number of rows, whichever is
hit first. Once the file size reaches this target, it will undergo a split.

Rows will be sorted by their primary key.

### Delta files

Delta files are append-only files for storing data modifications (inserts,
deletes). Insertion data will be stored in some format that can easily be
converted to and from an Arrow record batch. Once a file exceeds a certain size,
it will be merged into the data file.

Delta files will have a one-to-one mapping to a table partition file.

## Operations

How various SQL commands will map to operations at this layer.

Note that catalog operations are assumed to eventually become transactional.

### `CREATE SCHEMA ...`

1. Insert info for schema into catalog. This should return a unique schema id.
2. "Allocate" the schema in object storage with schema id by creating the
   directory.

### `CREATE TABLE ...`

1. Insert info for table into catalog. This should generate a unique table id.
2. "Allocate" the table in object storage with table id by creating the
   directory.
3. Insert an empty meta file for the table.

### `INSERT INTO ...`

1. Lookup schema/table id from catalog.
2. Partition insert values.
3. Pull and cache data and delta files relevant to each partition for the insert.
4. Append insert values to the relevant delta files.

Note that this won't check for primary key duplicates for now. Once we have
transaction support, we'll be able to insert uncommitted data, check the primary
key index (optionally with a bloom filter in front) and then "commit" that data.

### `SELECT ...`

If the predicate is on primary key:

1. Get partition from meta file (should be cached in memory).
2. Scan delta file (likely cached in memory).
3. If predicate not satisfied, scan partition file(s).
   - This will currently scan the entire file. Eventually we'll want an index at
     the beginning of the file to allow jumping directly to the appropriate
     record batch in the file.

## Delta Merges

Once a delta file exceeds a certain size, it will be merged back into the main
data file. The process is as follows:

1. Acquire exclusive lock for table/partition/partition_chunk.
   - This **will** prevent concurrent access, so we'll want to rethink this later.
2. Read in the entirety of the delta file, sort by primary key.
3. Merge sort with data file.
4. Reset delta file.
5. If data file exceeds threshold, retain lock and split data file.

This will be where we reconcile deletes and updates. When we have a version
file, we'll append the old data to that file.

## Data File Splits

If a data file exceeds a size threshold, it will be split into two smaller
files/partitions. The process is as follows:

1. Acquire exclusive lock for table/partition. Acquire an exclusive lock on the
   meta file.
   - The meta lock should prevent concurrent partition splits, but not access to
     other partitions.
2. Get next partition id to use from the meta file.
3. Split and write out files.
4. Allocate an empty delta file for the new parition, and clear the delta file
   for the partition that was just split.
5. Write new primary key ranges to meta file.
6. Release locks.

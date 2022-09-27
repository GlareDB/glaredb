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
    - table_0_part_0_0.data
    - table_0_part_0_0.delta
    - table_0_part_0_1.data
    - table_0_part_0_1.delta
    - table_0_part_1_0.data
    - table_0_part_1_0.delta
  - table_1
    ...
```

The directory structure on the local disk will mimic the structure in object
storage.

### Data files

``` text
table_<a>_part_<b>_<c>.data
a -> table identifier
b -> partition identifier
c -> partition chunk identifier
```

Data files contain user data, and follow the Arrow IPC format. Each file's
schema will include the appropriate data types with synthetic field names. These
synthetic field names will map to the proper field names higher up in the system.

A data file will contain multiple blocks, each prepended with some metadata
about that block.

Each file will try to reach some target size and/or number of rows, whichever is
hit first. Once the file size reaches this target, it will undergo a split.

Rows will be sorted by their primary key.

### Delta files

Delta files are append-only files for storing data modifications (inserts,
deletes). Data is this file will be stored in the Arrow IPC format. Once a file
exceeds a certain size, it will be merged into the data file.

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

### `INSERT INTO ...`

1. Lookup schema/table id from catalog.
2. Partition insert values.
3. Pull and cache data and delta files relevant to each partition for the insert.
4. Append insert values to the relevant delta files.

## Delta Merges

Once a delta file exceeds a certain size, it will be merged back into the main
data file. The process is as follows:

1. Acquire exclusive lock for table/partition/partition_chunk.
  - This **will** prevent concurrent access, so we'll want to rethink this later.
2. Read in the entirety of the delta file, sort by primary key.
3. Merge sort with data file.
4. Reset delta file.
5. If data file exceeds threshold, retain lock and split data file.

## Data File Splits

If a data file exceeds a size threshold, it will be split into two smaller
files. The process is as follows:

1. Acquire exclusive lock for table/partition.
2. Split and write out files.

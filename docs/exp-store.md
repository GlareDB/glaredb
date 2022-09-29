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
- Secondary indexes and uniqueness.

Secondary priorities should be focused on after we have a POC.

### Rationale

If we look at the priorities from a "features" perspective, the rough order of
support looks something like this:

1. POC with table creates and inserts
2. Transactions
3. Secondary indexes
4. Table changes
5. Updates, deletes

If we have 1 (in combination with a fast query execution), we could actually
ship a product early with a very targeted use case (ingestion, analytics).
Updates and deletes happen to not fit this use case, which is the rationale
behind not having it in the POC. I'm not saying we should focus entirely on this
use case and disregard everything else, I'm more concerned with how the product
is able to evolve as we add features. I also believe that the
merging/reconciliation methods that we do for transactions will tie heavily into
how we do updates/deletes.

2 and 3 are somewhat interchangeable in terms of priority.

4 follows pretty closely, and how we handle that will go through how we handle
merges. E.g. the delta will store a "table update" which we'll be able to
reconcile with the base data file when it's merged in.

And 5 is last because we'll want to have some concept of a version store (e.g.
an append-only version file(s) per table) which I think would be too much to do
right now.

Durability/logging fits in "somewhere" but not sure where yet. That might just
be the raft log itself that we can ship around and flush to object storage as
necessary.

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

### Storage Format

The initial storage format will be lightweight encoding of the Arrow format with
minimal compression. A custom header will be prepended for each record batch.
This header will contain size and schema info, allowing use to use the buffer
directly for Arrow arrays with no deserialization overhead.

#### On not using Parquet

It's unlikely that we'll use parquet directly as the storage format at any level
in the near-term. It's been less than fun trying to use the existing parquet
crates (parquet and parquet2) to try to do "point batch reads". I don't
necessarily think this is a limitation of the file format, more just the
libraries themselves. I believe it'll be easier/quicker to use Arrow's array
data directly as the basis for the file format in the near-term

Longer term we'll have to look at:

- Are these assumptions about using Arrow data easier/quicker correct?
- Do customers want to be able to access database data directly through parquet
  (or similar)?
- If so, do they want the latest data in parquet? Would it be sufficient to
  serialize just old data (like the version file)?

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

Rows will be sorted by their primary key. This allows for easy partitioning, and
it will also allow for faster primary key uniqueness checks, e.g. we could layer
bloom filters on top of groups of record batches to avoid needing to read them
in, and if bloom filter check indicates a possible duplicate, then we only need
to scan a single record batch instead of the whole file.

### Delta files

Delta files are append-only files for storing data modifications (inserts,
deletes). Insertion data will be stored in some format that can easily be
converted to and from an Arrow record batch. Once a file exceeds a certain size,
it will be merged into the data file.

Delta files will have a one-to-one mapping to a table partition file.

## Operations

How various SQL commands will map to operations at this layer.

Note that catalog operations are assumed to eventually become transactional.
Ideally everything will use the same transaction framework. Every "delta"
(inserts, deletes, updates, ddl) being inserted into the delta file will contain
a start transaction timestamp, and a null commit timestamp. To commit, a
"commit" delta will be added and we updated the provisional record with the
commit timestamp.

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
data file. 

The process is as follows:

1. Acquire exclusive lock for table/partition.
   - This **will** prevent concurrent access, so we'll want to rethink this later.
2. Read in the entirety of the delta file, sort by primary key.
3. Merge sort with data file.
4. Reset delta file.
5. If data file exceeds threshold, retain lock and split data file.

This will be where we reconcile deletes and updates. When we have a version
file, we'll append the old data to that file.

Delta files will be decently large such that merges are relatively infrequent.
E.g. we might go with a 10MB target size for deltas, and a 250MB target size
base data files. These are not hard limits which allows for flexibility with
when we do the merge.

## Data File Splits

If a data file exceeds a size threshold, it will be split into two smaller
files/partitions. In most cases, a split should occur immediately after a delta
merge if necessary, preventing having to re-acquire the lock.

The process is as follows:

1. Acquire exclusive lock for table/partition. Acquire an exclusive lock on the
   meta file.
   - The meta lock should prevent concurrent partition splits, but not access to
     other partitions.
2. Get next partition id to use from the meta file.
3. Split and write out files.
4. Allocate an empty delta file for the new partition, and clear the delta file
   for the partition that was just split.
5. Write new primary key ranges to meta file.
6. Release locks.

## System Catalogs

All system catalogs will belong to a "system" schema, with the schema having
a predefined identifier. This schema will contain multiple catalog tables, each
with their own predefined identifiers.

Using the structure as defined above for catalogs gives us two things:

- No global, schema local, or table local manifests that we have to read.
- Can use the same operations as any other table in the system. This will be
  very useful for transactional DDL as we can reuse the transactional operations
  defined for normal tables.

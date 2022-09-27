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
- table_0
  - table_0_part_0.data
  - table_0_part_0.idx
  - table_0_part_0.delta
  - table_0_part_1.data
  - table_0_part_1.idx
  - table_0_part_1.delta
- table_1
  ...
```

### Data files

Data files contain user data, and follow the Arrow IPC format. Each file's
schema will include the appropriate data types with synthetic field names. These
synthetic field names will map to the proper field names higher up in the system.

### Index files

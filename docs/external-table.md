# External Table Support

A living doc describing our external table implementation.

Example of what we are looking for:
```sql
create external table hits stored as parquet location 'gs://glaredb-testdata/hits.parquet';
create external table hits stored as parquet location 's3://glaredb-testdata/hits.parquet';
```

## External Tables

External tables are read-only tables managed by GlareDB and no different to regular tables for the purposes of querying however they are backed by data living elsewhere.
Currently they will be data via available via a public object store.
In the future releases support for private object storage and other network stores will be considered.

There will be a system table that includes the metadata we require for any given external table
Additionally other data needed for tables in general will exist for external tables such as attributes and the relations table

### `object_store_registry` Table

Contains all the information to create an object store registry by which we can provide fully qualified URLs to access a specific object store registered by GlareDB.

This will build on the existing support for external tables in datafusion and it's [object store registry](https://github.com/apache/arrow-datafusion/blob/master/datafusion/core/src/datasource/datasource.rs).

| Column name | Column type | Description                         |
|-------------|-------------|-------------------------------------|
| `object_store_id` | UInt32      | Object id for this object store |
| `scheme`    | Utf8      | URL prefix to registry for this object store |
| `bucket`    | Utf8      | The bucket/host the data is on |
| `access`    | Binary      | Information needed to access data and provide to the storage vendor being used |

Note: the access column will need different data for different types of object store, S3/GCS

Alternatively we could exclusively use the S3 protocol for all object stores as that is the de facto standard at this time. Work on expansion, if pessary, later

We will also need SQL-like command to register an external object store provider or automatically process all kinds of providers, much like [this example](https://arrow.apache.org/datafusion/user-guide/cli.html?highlight=external+table#querying-s3-data-sources) in datafusion

### `external_relations` Table

Describes all external relations in the database.

| Column name | Column type | Description                         |
|-------------|-------------|-------------------------------------|
| `schema_id` | UInt32      | Schema this relation is in.         |
| `table_id`  | UInt32      | Table id for this relation, unique per schema. |
| `object_store_id` | UInt32 | Object store this external relation utilizes |
| `format`    | Utf8        | Format data is stored in (e.g. parquet, csv, etc.)  |
| `location`  | Utf8        | Location of data |

### Caching

Currently we employ a disk cache to improve latency when requesting data from GlareDB's persistence layer (an object store of choice).
The cache contains only the permissions for a single object store location. If we are to reuse this cache for the external table to improve latency of data retrieval this will require modifications to the caching layer to determine where any given data is from. Likely we will have to use this object store registry.

An alternative would be to create a copy of the external table in the database's object store and use that to back the external table as we would any other table.

Another issue with caching is related to cache invalidation as the external table source is modified/updated over time.

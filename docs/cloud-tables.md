# Cloud Tables

_Cloud Tables_ are virtual tables in the database backed by the Cloud service.
Cloud Tables will allow for data sharing between GlareDB and Cloud.

The primary use-case is to allow Cloud transparent access to data that is mostly
easily accessible and updateable by GlareDB. For example, GlareDB will have the
most intimate knowledge about object storage usage.

Cloud Tables will support inserts, updates, deletes, and selects. Cloud Tables
_will not_ be transactional, since table access will usually happen outside the
context of a transaction.

Cloud Tables are a completely internal concept, however tables that are backed
by Cloud Tables should be queryable. Database users will not be able to create
Cloud Tables themeselves.

## Implementation

This section is more to help convey the idea of "what" Cloud Tables are, and is
not prescriptive.

A Cloud Table will just be another `TableProvider`/`MutableTableProvider`
implementation, with the implementation being backed by API requests to Cloud.
Fancy things like predicate pushdown and projection will be handled where they
make sense for our use-case. 

On Cloud, each Cloud Table will be backed by an API endpoint and corresponding
Postgres tables. Each row in each Postgres table will be namespaced by database
id. Because these are just regular Postgres tables, Cloud will be able to query
them as necessary.

### Example Table: Object Storage Usage

Logically, object storage may be logically represented internally as the
following:

| Schema Name | Num Objects | Total Bytes |
|-------------|-------------|-------------|
| my_schema_1 | 3           | 897         |
| my_schema_2 | 87          | 54,543      |
| ...         |             |             |

This would map approximately to the following json:

``` json
{
  "database_id": "<uuid>",
  "rows": [
    {
      "schema": "my_schema_1",
      "num_objects": 3,
      "total_bytes": 897,
    },
    {
      "schema": "my_schema_2",
      "num_objects": 87,
      "total_bytes": 54543,
    }
  ]
}
```

On inserts/updates, GlareDB would send the above json to Cloud. On selects,
GlareDB would receive the above from Cloud.

The Postgres table on Cloud might look like this:

| Database ID | Schema Name | Num Objects | Total Bytes |
|-------------|-------------|-------------|-------------|
| <uuid>      | my_schema_1 | 3           | 897         |
| <uuid>      | my_schema_2 | 87          | 54,543      |
| ...         |             |             |             |

And just like that, Cloud is able to calculate the total number of bytes used by
a database with a simple query to Postgres.

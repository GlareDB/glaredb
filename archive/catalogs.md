# Catalogs and Data Sources

Data sources will be implemented in the context of catalogs.

## Catalog Overview

The `Catalog` trait provides methods for returning dynamically dispatched
`Schema`s which themselve provide access to entries in the catalog.

The (cut down) catalog trait:

```rust
pub trait Catalog {
    fn try_get_schema(&self, tx: &CatalogTx, name: &str) -> Result<Option<&dyn Schema>>;
    fn create_schema(&mut self, tx: &CatalogTx, create: CreateSchema) -> Result<()>;
}
```

The (cut down) schema trait:

```rust
pub trait Schema {
    fn try_get_entry(&self, tx: &CatalogTx, name: &str) -> Result<Option<&CatalogEntry>>;
    fn create_table(&mut self, tx: &CatalogTx, create: CreateTable) -> Result<()>;
}
```

These traits provide the typically flow for resolving catalog entries. First we
try to get the schema from the catalog, then try to get the fully resolved entry
from the schema. The types returned from the schema are _concrete types_ (more
on why later in the doc).

## Database Context Overview

The `DatabaseContext` struct holds all catalogs for session. Essentialy this
maps database names to a catalog. When a session starts, a new database context
is created containing two catalogs; the system catalog and a temp catalog.

### System Catalog

The system catalog contains system entries like the `glare_catalog` and
`pg_catalog` schemas, as well as the builtin functions. The system catalog is
namespaced under "system", so a query using a fully-qualified path to a system
table would look like `SELECT * FROM system.glare_catalog.tables`. The system
catalog is read-only.

### Temp Catalog

The temp catalog contains entries that last only the lifetime of a session, and
gets dropped at session end. This catalog will contain things like temp tables.

### Default Catalog

The default catalog would be the catalog created for the user for storing user
tables and data. Tables in the default catalog would be accessed like `SELECT *
FROM default.public.my_table`.

Creating the default catalog on session start is not a requirement.

### Catalog Search Path

By default, builtins are placed in the `system.glare_catalog` schema. To avoid
the user having to write `SELECT system.glare_catalog.sum(a) FROM ...`,
functions will default to resoling from `system.glare_catalog`.

Similar considerations will be taken for user tables (the experience should not
be worse than what we have now). I'm just not yet sure what this looks like.

### Attaching/detaching Catalogs

Each session has full control over its `DatabaseContext`, and can attach and
detach catalogs by name.

## Data Sources as Catalogs

Each data source will implement the `Catalog` and `Schema` traits. For example,
the BigQuery data source would implement a `BigQueryCatalog` type, which returns
a `BigQuerySchema`, which returns a catalog entry we can use during planning and
preparing for execution.

When a user/session wants to add bigquery data source, they'll be attaching a
catalog to the session's `DatabaseContext`. The could would look something like:

```rust
let bq_catalog = BigQueryCatalog::try_new(options)?;
self.context.attach_catalog("my_bq", bq_catalog)?;
```

The catalog will then be queryable like so: `SELECT * FROM
my_bq.air_quality.metrics`.

### External Tables

TODO: Probably just a "symlink" into a data source catalog.

### Planning

I'm not sure exactly what planning around attaching external database looks
like, just that I don't want it to be like how we currently do it where the
planning has to have knowledge about all the data sources.

Ideally we we register data sources on startup, then we pass in options to
_something_ that creates a catalog from the options, then we just attach the
catalog to the session. Persistence of attached databases is still an unknown,
possibly write a blob to object store or call out to cloud to register a data
source for an org.

Data source crates should contain all the implementation required for adding new
data sources.

### Implementation Details on the Actual Querying

As mentioned above, the `Schema` implementation will return concrete catalog
entry types instead of a trait. This is because the catalog/table entry will be
used during planning, and also be part of the pipeline that gets sent around for
remote execution.

Planning with a data source catalog will take the following steps:

1. Catalog table entry from the data source catalog.
2. Table entry gets stored on the logical plan, optimized, etc.
3. Pipeline planning will place the table entry onto a `PhysicalScan` pipeline.
4. During state creation, a `DatabaseContext` containing the catalogs for the
   data sources we're querying will be passed to `create_states`.
5. The data source catalog will have a method that accepts the table entry, and
   returns a `PhysicalTable` that gets stored on the states, and is what's used
   during execution.
   
What exactly this `PhysicalTable` looks like, I'm not sure yet. But it'll be
what provides the functionality for scans/inserts/updates/deletes for data
sources (and internal tables too).

When running all on a single node, the `DatabaseContext` that's passed to
`create_states` will just be the one on the session.

When running remotely, the `DatabaseContext` that's passed in will be built up
on-demand by the remote node containing catalogs needed for the query. For
example, for the query `SELECT * FROM my_pg.public.table`, we'll take the
following steps:

1. Plan the query locally ("local" meaning just the node that received the
   query).
2. Create the initial pipeline, slice it up, and send it to remote nodes.
3. Remote node gets parts of the pipeline, and reads the pipeline to see which
   catalogs we need. Remote does something (read a blob for an org in object
   store, contact cloud, or something else) to enable building up of the
   appropriate catalogs.
4. Remote node calls `create_states` to finish initializing everything.
5. Remote node executes.

## Multiple GlareDB Catalogs

What this design also enables is allowing a single session to access multiple
glaredb catalogs. So if I'm in my organization `glaredb_org`, and I have the
databases `gold` and `silver`, I should be able to access both from the same
session by fully qualifying the table names I'm trying to query.

When a session starts, we'll be providing it info about catalogs it should open
and attach by passing in that information from cloud (somehow).

This also means that my BigQuery or Postgres databases I've added are just
sibling catalogs to my GlareDB catalogs. If I "add" a BigQuery database to my
org, then that data source can be accessible from any of my databases (minus any
ACL stuff we do).

### Default Catalog

Related to the catalog search path, we should "default" to single catalog when
initially connecting to database.

So if I connect via:

```
psql postgres://glaredb_org.proxy.glaredb.com:6543/gold
```

The "gold" database should be set to default. When running queries like
`SELECT * FROM my_table`, it will first get the "gold" catalog, then utilize the
search path to get the "public" schema, to then finally resolve "my_table".

The "silver" database will also be available by fully qualifying: `SELECT * FROM silver.other.table`.


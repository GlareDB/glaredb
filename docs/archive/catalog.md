# Catalog

A living doc describing our catalog.

## System Tables

System tables describe tables that are managed completely by GlareDB and cannot
be directly manipulated by database users. In general, GlareDB system tables
will align closely with the design of Postgres system tables.

System tables will exist in a single, dedicated schema. Additional schemas
offering alternative views of system info will be implemented in the future, for
example `information_schema` and a Postgres compactible catalog. A decent number
of schema IDs will be reserved to accomodate future "system" schemas.

System tables will be bootstrapped on startup. Bootstrapping includes ensure the
tables physically exist, as well as ensuring that each table contains system
records as appropriate. System tables may either be backed by persistent
storage, or may exist completely in-memory. A future table type backed by API
requests to Cloud will be implemented in the future.

A quick overview of system tables:

| Table name      | Description                             |
|-----------------|-----------------------------------------|
| `schemas`       | Schemas (namespaces) in the database.   |
| `sequences`     | Sequences within the database.          |
| `relations`     | Any "table-like" thing in the database. |
| `attributes`    | Table columns.                          |
| `builtin_types` | Builtin types.                          |

### `schemas` Table

Desribes all schemas (namespaces) in the database.

| Column name | Column type | Description                         |
|-------------|-------------|-------------------------------------|
| `schema_id` | UInt32      | Globally unique id of the schema.   |
| `name`      | Utf8        | Globally unique name of the schema. |

### `sequences` Table

| Column name     | Column type | Description                                    |
|-----------------|-------------|------------------------------------------------|
| `seq_schema_id` | UInt32      | Schema this sequence is in.                    |
| `seq_rel_id`    | UInt32      | Table id for this schema.                      |
| `seq_data_type` | UInt32      | Datatype for this sequence.                    |
| `seq_start`     | Int64       | Next value in the sequence.                    |
| `seq_min`       | Int64       | Min value for the sequence.                    |
| `seq_max`       | Int64       | Max value for the sequence.                    |
| `seq_inc`       | Int64       | How much to increment to reach the next value. |

### `relations` Table

| Column name  | Column type | Description                                    |
|--------------|-------------|------------------------------------------------|
| `schema_id`  | UInt32      | Schema this relation is in.                    |
| `table_id`   | UInt32      | Table id for this relation, unique per schema. |
| `table_name` | Utf8        | Name of the table.                             |

### `relations` Table

| Column name  | Column type | Description                                    |
|--------------|-------------|------------------------------------------------|
| `schema_id`  | UInt32      | Schema this relation is in.                    |
| `table_id`   | UInt32      | Table id for this relation, unique per schema. |
| `table_name` | Utf8        | Name of the table.                             |

### `attributes` Table

| Column name | Column type | Description                        |
|-------------|-------------|------------------------------------|
| `schema_id` | UInt32      | Schema this relation is in.        |
| `table_id`  | UInt32      | Table this attribute belongs to.   |
| `attr_name` | Utf8        | Name of the attribute.             |
| `attr_ord`  | UInt32      | Ordinal position of the attribute. |
| `type_id`   | UInt32      | Data type for this attribute.      |
| `nullable`  | Bool        | If this attribute is nullable.     |

### `builtin_types` Table

Describes all builtin types for the database. This will be backed by an
in-memory table.

| Column name | Column type | Description        |
|-------------|-------------|--------------------|
| `type_id`   | UInt32      | ID of this type.   |
| `type_name` | Utf8        | Name of this type. |

## Bootstrap sequence

When a GlareDB node starts up, it will be provided with configuration options
for accessing persistent storage. This persistent storage will back most of the
system tables.

On start up, GlareDB will do roughly the following:

1. Read a sentinel file from persistent storage which will indicate if the
   database has been bootstrapped. If this file exists, GlareDB can skip system
   tables bootstrapping.
2. Check if a lockfile exists on persistent storage. If it exists, continually
   read it until it's deleted, then go to step 1. Otherwise create the lockfile.
   This prevents multiple nodes from trying to bootstrap at the same time.
3. Create system tables, insert system records.
4. Write the sentinel file indicating bootstrapping is complete.

_Note that these steps do not cover things like how we want to handle system
table changes across versions._


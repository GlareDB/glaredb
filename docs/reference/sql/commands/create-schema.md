---
title: CREATE SCHEMA
---

# Create schema

The `CREATE SCHEMA` statement creates a new schema in a database catalog.

## Syntax

```sql
CREATE SCHEMA [IF NOT EXISTS] [catalog_name.]schema_name
```

## Parameters

- `IF NOT EXISTS`: Prevents an error from being raised if a schema with the same name already exists.
- `catalog_name`: Optional name of the catalog in which to create the schema.
- `schema_name`: The name of the schema to create.

## Examples

Create a schema in the current catalog:

```sql
CREATE SCHEMA my_schema;
```

Create a schema in a specified catalog:

```sql
CREATE SCHEMA temp.my_schema;
```

Create a schema only if it doesn't already exist:

```sql
CREATE SCHEMA IF NOT EXISTS temp.my_schema;
```

This command won't raise an error if the schema already exists.

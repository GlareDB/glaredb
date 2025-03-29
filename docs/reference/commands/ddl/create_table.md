---
title: CREATE TABLE
---

# CREATE TABLE

The `CREATE TABLE` statement creates a new table in the database.

> Only temporary tables are currently supported.

## Syntax

```sql
CREATE [TEMP | TEMPORARY] TABLE [IF NOT EXISTS] table_name (
    column_name data_type,
    ...
)
```

Or using a query to define the table structure:

```sql
CREATE [TEMP | TEMPORARY] TABLE [IF NOT EXISTS] table_name AS query
```

## Parameters

- `TEMP` or `TEMPORARY`: Creates a temporary table that exists only for the duration of the session.
- `IF NOT EXISTS`: Prevents an error from being raised if a table with the same name already exists.
- `table_name`: The name of the table to create.
- `column_name`: The name of a column in the new table.
- `data_type`: The data type of the column.
- `query`: A SELECT query that defines the columns and data for the new table.

## Examples

Create a temporary table with columns:

```sql
CREATE TEMP TABLE cities (
    name TEXT,
    state_code TEXT,
    population INTEGER
);
```

Create a temporary table from a query:

```sql
CREATE TEMP TABLE large_cities AS
SELECT * FROM cities WHERE population > 1000000;
```

Create a temporary table if it doesn't already exist:

```sql
CREATE TEMP TABLE IF NOT EXISTS cities (
    name TEXT,
    state_code TEXT,
    population INTEGER
);
```

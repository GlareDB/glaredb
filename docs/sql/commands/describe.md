---
title: DESCRIBE
---

# DESCRIBE

`DESCRIBE` statements provide information about the schema of queries, tables,
table functions, or files.

## Syntax

```sql
DESCRIBE query_or_table
```

## Parameters

- `query_or_table`: A query or table-like object.

## Describing a query

`DESCRIBE` followed by a query can be used to show the names and data types that
would be returned by that query:

```sql
DESCRIBE SELECT 1 as a, 'hello' as b;
```

Shows the following query info:

| column_name | datatype |
|-------------|----------|
| a           | Int32    |
| b           | Utf8     |

## Describing tables and table functions

`DESCRIBE` followed by a table, table function, or file shows the column names
and data types of that object.

Describe `my_table`:

```sql
CREATE TEMP TABLE my_table (a TEXT, b DECIMAL(16, 2));
DESCRIBE my_table;
```

Outputs:

| column_name | datatype        |
|-------------|-----------------|
| a           | Utf8            |
| b           | Decimal64(16,2) |

Describe the output of a call to a table function:

```sql
DESCRIBE unnest([4,5,6]);
```

Outputs:

| column_name | datatype |
|-------------|----------|
| unnest      | Int32    |

## Describing files

`DESCRIBE` can only be used to describe local and remote files.

For example, we can describe a parquet file located in S3:

```sql
DESCRIBE 's3://glaredb-public/userdata0.parquet';
```

Which will output the following:

| column_name       | datatype      |
|-------------------|---------------|
| registration_dttm | Timestamp(ns) |
| id                | Int32         |
| first_name        | Utf8          |
| last_name         | Utf8          |
| email             | Utf8          |
| gender            | Utf8          |
| ip_address        | Utf8          |
| cc                | Utf8          |
| country           | Utf8          |
| birthdate         | Utf8          |
| salary            | Float64       |
| title             | Utf8          |
| comments          | Utf8          |


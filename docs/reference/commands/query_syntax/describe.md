---
title: DESCRIBE
---

# DESCRIBE

`DESCRIBE` statements provide information about the schema of queries, tables,
table functions, or files.

## Describing a Query

`DESCRIBE` followed by a query can be used to show the names and data types that
would be return by that query:

```sql
DESCRIBE SELECT 1 as a, 'hello' as b;
```

Shows the following query info:

| column_name | datatype |
|-------------|----------|
| a           | Int32    |
| b           | Utf8     |

## Describing Tables and Table Functions

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


<!-- TODO: Add in describe parquet/csv when that works again -->

---
title: DESCRIBE
---

# DESCRIBE

`DESCRIBE` statements provide information about the schema of queries, tables, 
table functions, or files.

## DESCRIBE query

The `DESCRIBE query` form shows the column names and data types that would be 
returned by the specified query.

```sql
DESCRIBE SELECT 1 as a, 'hello' as b;
```

Results:

```
a  Int32
b  Utf8
```

`DESCRIBE` can be used with queries that include more complex expressions:

```sql
DESCRIBE SELECT * FROM (VALUES (1, 2.0, 3.0::decimal(18,9))) AS v(a, b, c);
```

Results:

```
a  Int32
b  Decimal64(2,1)
c  Decimal64(18,9)
```

## DESCRIBE table/function/file

The `DESCRIBE table/function/file` form shows the column names and data types of a 
table, table function, or file.

Describe a table:

```sql
DESCRIBE my_table;
```

Describe a parquet file:

```sql
DESCRIBE 'path/to/file.parquet';
```

## Output format

The output of the `DESCRIBE` command is a table with two columns:
- Column names (left column)
- Data types (right column)

The data types correspond to GlareDB's internal type system, which includes types 
such as Int32, Int64, Float32, Float64, Decimal64, Utf8, Date32, etc.

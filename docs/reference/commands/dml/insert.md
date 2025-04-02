---
title: INSERT
---

# INSERT

The `INSERT` statement adds new rows to a table.

## Syntax

```sql
INSERT INTO table_name [(column1, column2, ...)] query
```

Where `query` can be either `VALUES` or a `SELECT` statement.

## Parameters

- `table_name`: The name of the table to insert data into.
- `column1, column2, ...`: Optional. A comma-separated list of column names. If specified, values will be inserted into these columns in the order provided.
- `query`: A query that returns the data to insert. This can be either a `VALUES` clause or a `SELECT` statement.

## Examples

Insert values into a table:

```sql
INSERT INTO cities VALUES ('New York', 'NY', 8804190);
```

Insert values into specific columns:

```sql
INSERT INTO cities (name, state_code) VALUES ('Chicago', 'IL');
```

Insert multiple rows at once:

```sql
INSERT INTO cities VALUES 
  ('Los Angeles', 'CA', 3898747),
  ('Houston', 'TX', 2304580);
```

Insert data from a query:

```sql
INSERT INTO large_cities
SELECT * FROM cities WHERE population > 1000000;
```

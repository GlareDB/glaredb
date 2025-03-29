---
title: CREATE VIEW
---

# CREATE VIEW

The `CREATE VIEW` statement creates a named view that represents a stored query. Views can be referenced in queries just like tables, but they don't store data themselves - they execute their underlying query when accessed.

> Note: Currently, only temporary views are supported using `CREATE TEMP VIEW`.

## Syntax

```sql
CREATE TEMP VIEW view_name [(column_name [, ...])]
AS query
```

Where:
- `TEMP`: Specifies that the view is temporary and will only exist for the current session
- `view_name`: Name to assign to the view
- `column_name`: Optional comma-separated list of names to assign to the view's columns
- `query`: A SELECT statement that defines the view

## Examples

### Basic View Creation

Create a simple view that selects a single value:

```sql
CREATE TEMP VIEW v1 AS SELECT 8 AS a;
```

Query the view:

```sql
SELECT * FROM v1;
```

Result:

| a |
|---|
| 8 |

### View with Complex Query

Create a view based on a more complex query:

```sql
CREATE TEMP VIEW v2 AS
  SELECT *
    FROM
      generate_series(1, 100) g1(a),
      generate_series(1, 100) g2(b)
    WHERE a = b + 1;
```

Query the view with a condition:

```sql
SELECT sum(a) FROM v2 WHERE b < 50;
```

Result:

| sum(a) |
|--------|
| 1274   |

### Column Aliasing

Create a view with custom column names:

```sql
CREATE TEMP VIEW v3(a, b) AS SELECT 3 as a1, 4 as b1, 5 as c1;
```

The view will have columns named `a`, `b`, and `c1`. The first two columns are renamed from `a1` and `b1`, while the third column keeps its original name.

## View Aliasing in Queries

Views can be aliased in queries, which allows renaming the view's columns:

```sql
SELECT * FROM v3 view_alias(x, y, z);
```

This query renames the columns of `v3` to `x`, `y`, and `z` for this specific query.

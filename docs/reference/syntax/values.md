---
title: VALUES
---

# VALUES

The `VALUES` clause allows you to provide literal row values directly in your SQL queries. It can be used to create inline tables without requiring a permanent table to be defined.

## Basic syntax

The basic syntax for `VALUES` is:

```sql
VALUES (expr1, expr2, ...), (expr1, expr2, ...), ...
```

Where each set of parentheses represents a single row, and each expression within the parentheses represents a column value.

Select two rows of data, first row containing `('cat', 4)` and second row containing `('dog', 5)`:

```sql
SELECT * FROM (VALUES ('cat', 4), ('dog', 5));
```

This produces:

| column1 | column2 |
|---------|---------|
| cat     | 4       |
| dog     | 5       |

## Column aliases

You can provide aliases for the columns produced by `VALUES` by appending an alias for the entire `VALUES` expression followed by a list of column names in parentheses:

```sql
SELECT * FROM (VALUES (1, 2.0, 3)) v(a, b, c);
```

This produces:

| a | b   | c |
|---|-----|---|
| 1 | 2.0 | 3 |

If you provide fewer column aliases than there are columns in your `VALUES` expression, the remaining columns will receive default names:

```sql
SELECT * FROM (VALUES (1, 2.0, 3)) v(a, b);
```

This produces:

| a | b   | column3 |
|---|-----|---------|
| 1 | 2.0 | 3       |

## Lateral references

`VALUES` can be used with lateral joins, allowing references to columns from previous tables in the `FROM` clause:

```sql
SELECT * 
FROM (VALUES (2), (3)) v1(a), 
     (VALUES (a + 1, a * 2)) v2(b, c);
```

This produces:

| a | b | c |
|---|---|---|
| 2 | 3 | 4 |
| 3 | 4 | 6 |

The `LATERAL` keyword may be specified explicitly:

```sql
SELECT * 
FROM (VALUES (2), (3)) v1(a), 
     LATERAL (VALUES (a + 1, a * 2)) v2(b, c);
```

## NULL handling and implicit casting

`VALUES` can handle NULL values and perform implicit casting to ensure all rows have consistent types:

```sql
SELECT * FROM (VALUES (4), (NULL));
```

This produces:

| column1 |
|---------|
| 4       |
| NULL    |

## Restrictions

- All rows in a `VALUES` expression must have the same number of columns
- Empty `VALUES` expressions (e.g., `VALUES ()`) are not allowed

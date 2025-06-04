---
title: FROM
order: 1
---

# From

The `FROM` clause specifies the sources for data for a query. A source can be a
table, table function, subquery, `VALUES`, or a combination of any of those
through joins.

## Tables

A single table may be referenced in the `FROM` clause, which will only produce
columns from that table.

Select from a table named `cities`:

```sql
SELECT * FROM cities;
```

Table references may also be qualified with a schema and possibly database name.

Select from a table named `cities` in a schema named `census`:

```sql
SELECT * FROM census.cities;
```

## Table functions

Table functions are functions that produce tabular data, and can be used in the
`FROM` clause.

Select from a series of integers using the table function `generate_series`:

```sql
SELECT * FROM generate_series(1, 10);
```

Table functions like `read_csv` and `read_parquet` can also be used to query
external data:

```sql
SELECT * FROM read_parquet('my_parquet_file.parquet');
```

## Subqueries

Subqueries are nested queries that produce one or more columns.

Select from a subquery that's selecting from a table named `cities`:

```sql
SELECT *
FROM (SELECT name, state FROM cities);
```

## Values

`VALUES` can be used to provide literal row values:

Produce two rows, with row one containing `('cat', 4)` and row two containing
`('dog', 5)`:

```sql
SELECT * FROM (VALUES ('cat', 4), ('dog', 5));
```

## Joins

Joins produce new tables by horizontally combining columns from multiple input
tables.

### Cross Joins

A cross join is the simplest type of join, and produces all possible pairs
between the left and right tables.

Cross join two tables:

```sql
SELECT *
FROM cities CROSS JOIN states;
```

The `CROSS JOIN` keyword can be omitted, and replaced with just a comma:

```sql
SELECT *
FROM cities, states;
```

### Conditional Joins

Conditional joins are joins that specify the predicate used when joining rows
between tables.

Join `states` onto `cities` to get information about the state's capital:

```sql
SELECT *
FROM states JOIN cities
ON states.abbr = cities.state_abbr
```

This will produce rows like:

| name  | abbr | capital | name   | population | state_abbr |
|-------|------|---------|--------|------------|------------|
| Texas | TX   | Austin  | Austin | 979882     | TX         |

A subset of the columns can be selected if not all columns are needed:

```sql
SELECT states.*, cities.population
FROM states JOIN cities
ON states.abbr = cities.state_abbr
```

Producing rows with redundant data removed:

| name  | abbr | capital | population |
|-------|------|---------|------------|
| Texas | TX   | Austin  | 979882     |

#### Inner/Outer joins

Inner joins are joins that will return rows _only if_ the join predicate matches
for that row. Inner joins can be specified using either `INNER JOIN` or just
`JOIN`.

Outer joins are joins that return rows that both match and don't match the join
predicate.

- `LEFT OUTER JOIN`: Return all matched rows, and all non-matched rows from the left
  table.
- `RIGHT OUTER JOIN`: Return all matched rows, and all non-matched rows from the
  right table.
- `FULL OUTER JOIN`: Return all matched rows, and all non-matched rows from the
  left and right table.

### Lateral Joins

A lateral join allows a subquery in the `FROM` clause to reference columns from
previous tables in the same `FROM` clause.

Lateral joins allow subqueries and table functions to depend on _each row_ of
the table before it.

A simple example:

```sql
SELECT *
FROM generate_series(1, 3) g(a), LATERAL (SELECT (a + 2)) s(b);
```

| a | b |
|---|---|
| 1 | 3 |
| 2 | 4 |
| 3 | 5 |

The `LATERAL` keyword may be omitted. The dependency on column `a` in the right
side of the join is automatically detected:

```sql
SELECT *
FROM generate_series(1, 3) g(a), (SELECT (a + 2)) s(b);
```

Many table functions can only depend on inputs defined on left side of the join.
For example, we can use columns `start` and `stop` defined in the `VALUES` as
arguments to `generate_series` to produce multiple integer series:

```sql
SELECT *
FROM (VALUES (1, 2), (8, 10)) v(start, stop), generate_series(start, stop);
```

This will output:

| start | stop | generate_series |
|-------|------|-----------------|
| 1     | 2    | 1               |
| 1     | 2    | 2               |
| 8     | 10   | 8               |
| 8     | 10   | 9               |
| 8     | 10   | 10              |


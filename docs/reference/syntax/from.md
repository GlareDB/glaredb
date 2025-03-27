# FROM

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

### Cross joins

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

### Conditional joins

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
for that row. Inner joins can be specied using either `INNER JOIN` or just
`JOIN`.

Outer joins are joins that return rows that both match and don't match the join
predicate.

- `LEFT OUTER JOIN`: Return all matched rows, and all non-matched rows from the left
  table.
- `RIGHT OUTER JOIN`: Return all matched rows, and all non-matched rows from the
  right table.
- `FULL OUTER JOIN`: Return all matched rows, and all non-matched rows from the
  left and right table.


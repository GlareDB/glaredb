---
title: ORDER BY
---

# ORDER BY

The `ORDER BY` clause specifies the order of rows in a query result set. Without
an `ORDER BY` clause, the order of rows in a result set is not guaranteed.

## Basic Syntax

A basic `ORDER BY` clause orders results by one or more expressions:

```sql
SELECT name, population
FROM cities
ORDER BY population;
```

## Sorting Direction

By default, results are sorted in ascending order. You can explicitly specify
the sort direction with `ASC` (ascending) or `DESC` (descending):

```sql
-- Ascending order (default)
SELECT name, population
FROM cities
ORDER BY population ASC;

-- Descending order
SELECT name, population
FROM cities
ORDER BY population DESC;
```

## Multiple Columns

You can order by multiple columns, with each subsequent column used as a
tiebreaker when the previous columns have equal values:

```sql
SELECT name, state_abbr, population
FROM cities
ORDER BY state_abbr, population DESC;
```

## Handling NULL Values

By default, NULL values are considered larger than any non-NULL value during
sorting:

- In `ASC` order, NULLs appear last
- In `DESC` order, NULLs appear first

You can override this behavior with `NULLS FIRST` or `NULLS LAST`:

```sql
-- Place NULL values first in ascending order
SELECT name, population
FROM cities
ORDER BY population ASC NULLS FIRST;

-- Place NULL values last in descending order
SELECT name, population
FROM cities
ORDER BY population DESC NULLS LAST;
```

## Ordering by Position

You can also order by column position (1-based indexing) in the select list:

```sql
SELECT name, population
FROM cities
ORDER BY 2; -- Orders by the population column
```

## Ordering by Expression

Any expression can be used in the `ORDER BY` clause:

```sql
SELECT name, population
FROM cities
ORDER BY population / 1000;
```

## Ordering by Alias

You can order by a column alias defined in the `SELECT` clause:

```sql
SELECT name, population / 1000 AS thousands
FROM cities
ORDER BY thousands;
```

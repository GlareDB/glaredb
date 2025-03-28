---
title: LIMIT
---

# LIMIT

The `LIMIT` clause restricts the number of rows returned by a query. Without an
`ORDER BY` clause, which rows are returned when using `LIMIT` is non-deterministic.

## Basic Syntax

A basic `LIMIT` clause specifies the maximum number of rows to return:

```sql
SELECT name, population
FROM cities
LIMIT 10;
```

## OFFSET Clause

The `OFFSET` clause can be used with `LIMIT` to skip a specified number of rows
before starting to return rows:

```sql
-- Skip 5 rows and return the next 10
SELECT name, population
FROM cities
LIMIT 10 OFFSET 5;
```

## Usage with ORDER BY

When using `LIMIT` with `ORDER BY`, the result will contain the first N rows
after sorting is applied:

```sql
-- Return the 3 cities with the highest population
SELECT name, population
FROM cities
ORDER BY population DESC
LIMIT 3;
```

## Usage with OFFSET and ORDER BY

Combining `LIMIT`, `OFFSET`, and `ORDER BY` allows for pagination of results:

```sql
-- Return the cities with the 4th to 6th highest populations
SELECT name, population
FROM cities
ORDER BY population DESC
LIMIT 3 OFFSET 3;
```

---
title: HAVING
---

# HAVING

The `HAVING` clause filters the results of a `GROUP BY` operation based on a condition. While the `WHERE` clause filters rows before they are grouped, the `HAVING` clause filters groups after they are formed.

A `HAVING` clause can only be used in conjunction with a `GROUP BY` clause or when the `SELECT` list contains at least one aggregate function.

## Basic Usage

Filter groups based on a condition on the grouping column:

```sql
SELECT state_abbr, SUM(population)
FROM cities
GROUP BY state_abbr
HAVING state_abbr = 'OH';
```

Filter groups based on an aggregate value:

```sql
SELECT state_abbr, SUM(population)
FROM cities
GROUP BY state_abbr
HAVING SUM(population) > 1000000;
```

## Expressions in HAVING

The `HAVING` clause can contain complex expressions involving aggregates:

```sql
SELECT state_abbr, SUM(population)
FROM cities
GROUP BY state_abbr
HAVING SUM(population) + 10000 > 1000000;
```

## Column References

Columns referenced in the `HAVING` clause must either:
1. Be part of the `GROUP BY` clause, or
2. Be used within an aggregate function

For example, this is valid because `state_abbr` is in the `GROUP BY`:

```sql
SELECT state_abbr, SUM(population)
FROM cities
GROUP BY state_abbr
HAVING state_abbr = 'OH';
```

And this is valid because `population` is used in an aggregate:

```sql
SELECT state_abbr, SUM(population)
FROM cities
GROUP BY state_abbr
HAVING SUM(population) > 1000000;
```

But this would be invalid because `city_name` is neither in the `GROUP BY` nor used in an aggregate:

```sql
SELECT state_abbr, SUM(population)
FROM cities
GROUP BY state_abbr
HAVING city_name = 'Columbus';  -- Error: city_name not in GROUP BY
```

## Column Aliases

Column aliases defined in the `SELECT` list cannot be referenced in the `HAVING` clause. You must repeat the expression instead:

```sql
-- This will NOT work
SELECT state_abbr, SUM(population) AS total_pop
FROM cities
GROUP BY state_abbr
HAVING total_pop > 1000000;  -- Error: total_pop not found

-- This WILL work
SELECT state_abbr, SUM(population) AS total_pop
FROM cities
GROUP BY state_abbr
HAVING SUM(population) > 1000000;
```

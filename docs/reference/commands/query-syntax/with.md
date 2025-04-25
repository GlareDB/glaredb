---
title: WITH
---

# WITH

The `WITH` clause allows you to define temporary result sets (Common Table
Expressions or CTEs) that exist only for the duration of a query. CTEs can
simplify complex queries by breaking them down into more manageable parts.

## Basic Syntax

The basic syntax for `WITH` is:

```sql
WITH cte_name AS (
    select_statement
)
SELECT * FROM cte_name;
```

Where `cte_name` is the name given to the CTE and `select_statement` is a valid
SELECT query that defines the data for the CTE.

Create a CTE named `numbers` that contains a sequence of integers:

```sql
WITH numbers AS (
    SELECT * FROM generate_series(1, 5)
)
SELECT * FROM numbers;
```

This produces:

| generate_series |
|-----------------|
| 1               |
| 2               |
| 3               |
| 4               |
| 5               |

## Multiple CTEs

You can define multiple CTEs in a single `WITH` clause by separating them with
commas:

```sql
WITH
    cte1 AS (select_statement1),
    cte2 AS (select_statement2)
SELECT * FROM cte1 JOIN cte2 ON ...;
```

Create two CTEs and join them together:

```sql
WITH
    even_numbers AS (
        SELECT * FROM generate_series(2, 10, 2)
    ),
    odd_numbers AS (
        SELECT * FROM generate_series(1, 9, 2)
    )
SELECT e.generate_series AS even, o.generate_series AS odd
FROM even_numbers e JOIN odd_numbers o
ON e.generate_series = o.generate_series + 1;
```

This produces:

| even | odd |
|------|-----|
| 2    | 1   |
| 4    | 3   |
| 6    | 5   |
| 8    | 7   |
| 10   | 9   |

## CTE References

CTEs defined earlier in the `WITH` clause can be referenced by CTEs defined
later:

```sql
WITH
    numbers AS (
        SELECT * FROM generate_series(1, 3)
    ),
    doubled AS (
        SELECT generate_series, generate_series * 2 AS doubled_value
        FROM numbers
    )
SELECT * FROM doubled;
```

This produces:

| generate_series | doubled_value |
|-----------------|---------------|
| 1               | 2             |
| 2               | 4             |
| 3               | 6             |

## Materialized CTEs

By default, a CTE is evaluated each time it is referenced in the query. You can
use the `MATERIALIZED` keyword to cache the results of a CTE, which can improve
performance when the CTE is referenced multiple times:

```sql
WITH cte_name AS MATERIALIZED (
    select_statement
)
SELECT * FROM cte_name;
```

Create a materialized CTE:

```sql
WITH numbers AS MATERIALIZED (
    SELECT * FROM generate_series(1, 1000000)
)
SELECT COUNT(*) FROM numbers WHERE generate_series % 2 = 0;
```

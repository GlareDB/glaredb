---
title: HAVING
---

# HAVING

The `HAVING` clause filters the results of a `GROUP BY` operation based on a
condition. While the `WHERE` clause filters rows before they are grouped, the
`HAVING` clause filters groups after they are formed.

Columns referenced in the `HAVING` clause must either be part of the `GROUP BY`
clause or be used within an aggregate function.

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

Any aggregate function can be used in the `HAVING` clause, not just ones that
appear in the `SELECT` clause. For example, we can limit our per-state
population totals just to states that we have more than 10 cities for:

```sql
SELECT state_abbr, SUM(population)
FROM cities
GROUP BY state_abbr
HAVING COUNT(*) > 10;
```

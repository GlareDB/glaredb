---
title: HAVING
---

# HAVING

The `HAVING` clause filters the results of a `GROUP BY` operation based on a condition. While the `WHERE` clause filters rows before they are grouped, the `HAVING` clause filters groups after they are formed.

```sql
SELECT state_abbr, SUM(population)
FROM cities
GROUP BY state_abbr
HAVING SUM(population) > 1000000;
```

Columns referenced in the `HAVING` clause must either be part of the `GROUP BY` clause or be used within an aggregate function.

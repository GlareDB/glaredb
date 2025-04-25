---
title: WHERE
---

# WHERE

The `WHERE` clause provides filters to apply to the output of executing the
`FROM` portion of a query.

Any expression that returns a boolean value can be used in the `WHERE` clause.

Simple filtering using a literal:

```sql
SELECT *
FROM cities
WHERE state_abbr = 'OH';
```

More complex expressions, including subqueries can be used as well:

```
SELECT *
FROM cities c1
WHERE population >= (SELECT avg(population) FROM cities c2 WHERE c1.state = c2.state)
```

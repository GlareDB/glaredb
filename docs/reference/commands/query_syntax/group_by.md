---
title: GROUP BY
---

# GROUP BY

A `GROUP BY` can be used to specify which columns to use for grouping values
during aggregate.

When an aggregate is executed with no `GROUP BY` clause, all rows are used to
produce the final aggregate value. When a `GROUP BY` is provided, aggregate
values are produced for each group.

For example, we can get min and max city population for each state:

```sql
SELECT state_abbr, min(population), max(population)
FROM cities
GROUP BY state_abbr;
```

A `GROUP BY` can be used even if there are no aggregates in the `SELECT`. This
produces a single row for each group (e.g. `DISTINCT`).

## Grouping Sets

`ROLLUP`, `CUBE`, or `GROUPING SETS` can be used to `GROUP BY` more than a
single dimension. A `GROUPING SET` clause can be used to specify which
dimmensions to group on.

For the following examples, we'll be working some city population data:

```sql
CREATE TEMP TABLE cities (name TEXT, population INT, state_abbr TEXT);
INSERT INTO cities VALUES
  ('Houston', 2314157, 'TX'),
  ('Austin', 979882, 'TX'),
  ('Dallas', 1302868, 'TX'),
  ('San Antonio', 1495295, 'TX'),
  ('Columbus', 913175, 'OH'),
  ('Cincinnati', 311097, 'OH'),
  ('Cleveland', 362656, 'OH');
```

If we wanted to find the average city population per state, as well as the
overall city population average, we can use grouping sets to specify those
dimensions.

We can use a `ROLLUP` for this which will produce the grouping sets
`(state_abbr, ())`, with empty paranthesis representing the "empty" group (how
we'll get the overall average):

```sql
SELECT state_abbr, avg(population)
FROM cities
GROUP BY ROLLUP (state_abbr);
```

Which will return these rows:

| state_abbr | avg                |
|------------|--------------------|
| NULL       | 1097018.5714285714 |
| TX         | 1523050.5          |
| OH         | 528976             |


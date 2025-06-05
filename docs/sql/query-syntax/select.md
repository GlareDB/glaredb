---
title: SELECT
order: 0
---

# SELECT

`SELECT` statements perform data retrieval from the database, and contain
expressions to select and transform the results.

## Select list

The select list is a list of expressions representing the output of a query.
Expressions can include constant values, column references, function calls, and
more.

For example, we can select the `name` from a `cities` table, and our output will
contain only a single column:

```sql
SELECT name FROM cities;
```

If we wanted to uppercase the name, we can call the `upper` function on the
`name` column:

```sql
SELECT upper(name) FROM cities;
```

Providing an alias for the uppercased name can be done with the `AS` keyword:

```sql
SELECT upper(name) AS upper_name FROM cities;
```

`upper_name` will be the column name used when returning the results for the
query. For brevity, the `AS` keyword can be omitted when aliasing an expression
in the select list.

When expressions aren't given an alias, a generated column name will be used.

### Lateral Alias References

When an expression is given alias in the select list, that alias can be used
inside expressions defined to the right of the aliased expression.

For example, this query defines three aliased expressions, with the second
expression referencing the first expression, and the last expression referencing
both of the previously aliased expressions:


```sql
SELECT 3 AS a,
       a + 5 AS b,
       a * b;
```

This query will output the following:

| a | b | ?column? |
|---|---|----------|
| 3 | 8 | 24       |

Column references take precedence over alias. For example, the following query
will have the second expression evaluate on the output of the `FROM` clause, and
not on the previously defined alias:

```sql
SELECT 4 AS a, a FROM generate_series(1, 3) g(a);
```

This query will output the following:

| a | a |
|---|---|
| 4 | 1 |
| 4 | 2 |
| 4 | 3 |

This rule applies to anything in the `FROM` clause, including tables, table
functions, and subqueries.

## Star expressions

`*` will be expanded to select all columns from the base tables in the query.

Select all columns from table `cities`:

```sql
SELECT * FROM cities;
```

When combined with a join, `*` will return columns from all tables involved in
the join.

Select all columns from `cities` and `states`:

```sql
SELECT *
FROM cities, states
WHERE cities.state_code = states.code;
```

`*` can be prefixed with a table name to only expand columns from that table.

Select all columns from `cities` and no columns from `counties`:

```sql
SELECT cities.*
FROM cities, states
WHERE cities.state_code = states.code;
```

### Excluding Columns

`EXCLUDE`/`EXCEPT` can be used to exclude certain columns from the output when
combined with `*`. Any number of columns can be excluded.

Exclude column `state_code` from the `cities` table:

```sql
SELECT * EXCLUDE (state_code) FROM cities;
```

`EXCEPT` can be used in place of `EXCLUDE`, and has the same meaning:

```sql
SELECT * EXCEPT (state_code) FROM cities;
```

### Replacing Columns

`REPLACE` can be used to replace columns in the output with a different
expression. Any number of of columns can be replaced.

Replace the column `name` in the output with a column where all `name` values
are upper cased:

```sql
SELECT * REPLACE (upper(name) AS name) FROM cities;
```

The replacement will be in the same position as the original column.

For example, if our `cities` table contains these values:

| name      | state_abbr |
|-----------|------------|
| Akron     | OH         |
| Cleveland | OH         |

Then our results using `REPLACE (upper(name) AS name)` will become:

| name      | state_abbr |
|-----------|------------|
| AKRON     | OH         |
| CLEVELAND | OH         |

### Dynamic Columns

`COLUMNS` can be used to select columns that match a regular expression.

Select all columns that end with `_name`:

```sql
SELECT COLUMNS('_name$') FROM persons;
```

This would select columns like `first_name` and `last_name`, and would exclude
columns like `age`.

`COLUMNS` can be used alongside other column selectors.

Select all columns that end with `_name`, and also select the `height` column:

```sql
SELECT COLUMNS('_name$'), height FROM persons;
```

## Aggregates

Aggregates produce a result that combines multiple rows into a single value.
When the `SELECT` list contains an aggregate function, then all expressions
being selected need to either be part of an aggregate function, or part of a
group (specified via `GROUP BY`).

Get the min and max `population` from a `cities` table:

```sql
SELECT min(population), max(population) FROM cities;
```

### DISTINCT Aggregates

Aggregate functions can be used with the `DISTINCT` keyword to operate only on
unique values rather than all values. When `DISTINCT` is specified, duplicate
values are eliminated before the aggregate function is applied.

For example:
```sql
-- Count the number of unique departments
SELECT count(DISTINCT department) FROM employees;

-- Calculate the sum of unique values
SELECT sum(DISTINCT salary) FROM employees;
```

The `DISTINCT` modifier can be used with any aggregate function.

## DISTINCT clause

The `DISTINCT` clause removes duplicate rows from the result set.

```sql
SELECT DISTINCT name, state_abbr FROM cities;
```

By default, `ALL` is used, which returns all rows including duplicates.

```sql
SELECT ALL name, state_abbr FROM cities;
```


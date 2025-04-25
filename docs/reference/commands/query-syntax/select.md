---
title: SELECT
order: 0
---

# SELECT

`SELECT` statements perform data retrieval from the database, and may contain
expressions to transform the results.

The `SELECT` clause contains a list of expressions to specify the results of a
query.

## Star Expressions

`*` will be expanded to select all columns from the base tables in the query.

Select all columns from table `cities`:

```sql
SELECT * FROM cities;
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
combined with `*`.

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
expression.

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

Aggregate functions can be used with the `DISTINCT` keyword to operate only on unique values rather than all values. When `DISTINCT` is specified, duplicate values are eliminated before the aggregate function is applied.

For example:
```sql
-- Count the number of unique departments
SELECT count(DISTINCT department) FROM employees;

-- Calculate the sum of unique values
SELECT sum(DISTINCT salary) FROM employees;
```

The `DISTINCT` modifier can be used with any aggregate function.

## DISTINCT Clause

The `DISTINCT` clause removes duplicate rows from the result set.

```sql
SELECT DISTINCT name, state_abbr FROM cities;
```

By default, `ALL` is used, which returns all rows including duplicates.

```sql
SELECT ALL name, state_abbr FROM cities;
```


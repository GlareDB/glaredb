---
title: Comparisons
---

# Comparisons

## Comparison operators

Comparison operators compare two values and return a boolean result (true or
false). They are commonly used in `WHERE` clauses and join conditions.

| Operator               | Description                                          |
|------------------------|------------------------------------------------------|
| `=`                    | Equal to                                             |
| `<>`, `!=`             | Not equal to                                         |
| `<`                    | Less than                                            |
| `<=`                   | Less than or equal to                                |
| `>`                    | Greater than                                         |
| `>=`                   | Greater than or equal to                             |
| `IS DISTINCT FROM`     | Not equal, treating NULL values as comparable values |
| `IS NOT DISTINCT FROM` | Equal, treating NULL values as comparable values     |

### Basic Comparisons

Check if a value is equal:

```sql
SELECT * FROM cities WHERE state_code = 'CA';
```

Check if a value is not equal:

```sql
SELECT * FROM products WHERE category <> 'Electronics';
```

Check if a value is less than 100:

```sql
SELECT * FROM orders WHERE amount < 100;
```

Check if a value is less than or equal to the given date:

```sql
SELECT * FROM employees WHERE hire_date <= '2020-01-01';
```

Check if the value is greater than 50:

```sql
SELECT * FROM products WHERE price > 50;
```

Check if the value is greater than or equal to 90:

```sql
SELECT * FROM students WHERE score >= 90;
```

### IS [NOT] DISTINCT FROM

These operators are similar to the `=` and `<>` operators, but they treat NULL
values as comparable values.

Regular comparison with NULL returns NULL (unknown):

```sql
SELECT 1 = NULL;   -- Result: NULL
SELECT NULL = NULL;   -- Result: NULL
```

`DISTINCT FROM` treats NULLs as comparable:

```sql
SELECT 1 IS DISTINCT FROM NULL;   -- Result: true
SELECT NULL IS DISTINCT FROM NULL;   -- Result: false
```

`NOT DISTINCT FROM` also treats NULLs as comparable:

```sql
SELECT 1 IS NOT DISTINCT FROM NULL;   -- Result: false
SELECT NULL IS NOT DISTINCT FROM NULL;   -- Result: true
```

## IS predicates

`IS` predicates check if an expression _is_ or _is not_ a given value, returning true or false.

| Predicate      | Description                                                                |
|----------------|----------------------------------------------------------------------------|
| `IS TRUE`      | Check if an expression is true. NULL if the input expression is NULL.      |
| `IS NOT TRUE`  | Check if an expression is not true. NULL if the input expression is NULL.  |
| `IS FALSE`     | Check if an expression is false. NULL if the input expression is NULL.     |
| `IS NOT FALSE` | Check if an expression is not false. NULL if the input expression is NULL. |
| `IS NULL`      | Returns true if the expression is NULL, false otherwise.                   |
| `IS NOT NULL`  | Returns true if the expression is not NULL, false otherwise.               |

### Examples

Is boolean:

```sql
SELECT true IS TRUE; -- Returns true
SELECT NULL IS TRUE; -- Returns NULL
```

Is NULL:

```sql
SELECT NULL IS NULL; -- Returns true
SELECT 4 IS NULL; -- Returns false
```

## BETWEEN predicates

`BETWEEN` predicates check if an expression is _between_ or _not between_ a
lower and upper bound.

| Predicate                                | Description                                                    |
|------------------------------------------|----------------------------------------------------------------|
| `expression BETWEEN lower AND upper`     | Check if `expression` is contained by `lower` and `upper`.     |
| `expression NOT BETWEEN lower AND upper` | Check if `expression` is not contained by `lower` and `upper`. |

### Examples

Check if number is in range:

```sql
SELECT 4 BETWEEN 2 AND 8; -- Returns true
SELECT 2 BETWEEN 2 AND 8; -- Returns true, expression is >= lower bound
SELECT 8 BETWEEN 2 AND 8; -- Returns true, expression is <= upper bound
SELECT 10 BETWEEN 2 AND 8; -- Returns false
```

Check if number is not in range:

```sql
SELECT 4 NOT BETWEEN 2 AND 8; -- Returns false
SELECT 2 NOT BETWEEN 2 AND 8; -- Returns false, expression is >= lower bound
SELECT 8 NOT BETWEEN 2 AND 8; -- Returns false, expression is <= upper bound
SELECT 10 NOT BETWEEN 2 AND 8; -- Returns true
```

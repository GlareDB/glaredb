---
title: Comparison
---

# Comparison Operators

Comparison operators compare two values and return a boolean result (true or false). They are commonly used in WHERE clauses and join conditions.

## Syntax

```sql
value_expression operator value_expression
```

## Operators

| Operator | Description |
|----------|-------------|
| `=` | Equal to |
| `<>` | Not equal to |
| `<` | Less than |
| `<=` | Less than or equal to |
| `>` | Greater than |
| `>=` | Greater than or equal to |
| `IS DISTINCT FROM` | Not equal, treating NULL values as comparable values |
| `IS NOT DISTINCT FROM` | Equal, treating NULL values as comparable values |

## Examples

### Equal (=) and Not Equal (<>)

```sql
SELECT * FROM cities WHERE state_code = 'CA';

SELECT * FROM products WHERE category <> 'Electronics';
```

### Less Than (<) and Less Than or Equal To (<=)

```sql
SELECT * FROM orders WHERE amount < 100;

SELECT * FROM employees WHERE hire_date <= '2020-01-01';
```

### Greater Than (>) and Greater Than or Equal To (>=)

```sql
SELECT * FROM products WHERE price > 50;

SELECT * FROM students WHERE score >= 90;
```

### IS DISTINCT FROM and IS NOT DISTINCT FROM

These operators are similar to the `=` and `<>` operators, but they treat NULL values as comparable values:

```sql
-- Regular comparison with NULL returns NULL (unknown)
SELECT 1 = NULL;   -- Result: NULL
SELECT NULL = NULL;   -- Result: NULL

-- DISTINCT FROM treats NULLs as comparable
SELECT 1 IS DISTINCT FROM NULL;   -- Result: true
SELECT NULL IS DISTINCT FROM NULL;   -- Result: false

-- NOT DISTINCT FROM also treats NULLs as comparable
SELECT 1 IS NOT DISTINCT FROM NULL;   -- Result: false
SELECT NULL IS NOT DISTINCT FROM NULL;   -- Result: true
```

Example with tables:

```sql
-- Find rows where city_name is different from stored_city, 
-- even if either value is NULL
SELECT * FROM addresses 
WHERE city_name IS DISTINCT FROM stored_city;
```

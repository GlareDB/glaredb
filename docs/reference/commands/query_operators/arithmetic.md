---
title: Arithmetic
---

# Arithmetic Operators

Arithmetic operators perform mathematical operations on numeric values in SQL expressions.

## Syntax

```sql
value_expression operator value_expression
```

## Operators

| Operator | Description |
|----------|-------------|
| `+` | Addition |
| `-` | Subtraction |
| `*` | Multiplication |
| `/` | Division |
| `%` | Modulo (remainder) |

## Examples

### Addition (+)

```sql
SELECT 5 + 3;
-- Result: 8

SELECT price + tax AS total_cost FROM orders;
```

### Subtraction (-)

```sql
SELECT 10 - 4;
-- Result: 6

SELECT current_balance - withdrawal AS new_balance FROM accounts;
```

### Multiplication (*)

```sql
SELECT 7 * 8;
-- Result: 56

SELECT quantity * unit_price AS line_total FROM items;
```

### Division (/)

```sql
SELECT 20 / 4;
-- Result: 5

SELECT total_amount / number_of_people AS amount_per_person FROM expenses;
```

### Modulo (%)

```sql
SELECT 17 % 5;
-- Result: 2

SELECT order_id % 10 AS routing_key FROM orders;
```

---
title: Logical
---

# Logical Operators

Logical operators combine boolean expressions and return a boolean result. They
are commonly used in WHERE clauses to combine multiple conditions.

## Syntax

```sql
-- AND/OR
boolean_expression operator boolean_expression

-- NOT
NOT boolean_expression
```

## Operators

| Operator | Description |
|----------|-------------|
| `AND` | Returns true if both expressions are true |
| `OR` | Returns true if either expression is true |
| `NOT` | Returns the opposite of the expression's value |

## Examples

### AND

```sql
-- Select customers who are both from California AND have made a purchase
SELECT * FROM customers
WHERE state = 'CA' AND purchases > 0;
```

### OR

```sql
-- Select products that are either electronics OR cost more than $100
SELECT * FROM products
WHERE category = 'Electronics' OR price > 100;
```

### NOT

```sql
-- Select cities that are not in Texas
SELECT * FROM cities
WHERE NOT (state_code = 'TX');

-- Alternative form using inequality
SELECT * FROM cities
WHERE state_code <> 'TX';
```

### Combining Multiple Operators

```sql
-- Select products that are in stock and either on sale or under $20
SELECT * FROM products
WHERE in_stock = true
  AND (on_sale = true OR price < 20);
```

Note: When combining operators, parentheses can be used to control the order of
evaluation.

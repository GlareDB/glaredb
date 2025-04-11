---
title: Subquery
---

# Subquery Operators

Subquery operators compare values with the results of a subquery. They are used to create complex queries that reference the results of other queries.

## Syntax

```sql
expression operator (subquery)
```

## Operators

| Operator | Description |
|----------|-------------|
| `IN` | Returns true if the expression equals any value in the subquery result |
| `NOT IN` | Returns true if the expression does not equal any value in the subquery result |
| `EXISTS` | Returns true if the subquery returns at least one row |
| `NOT EXISTS` | Returns true if the subquery returns no rows |
| `ANY` | Returns true if the comparison is true for any value returned by the subquery |
| `ALL` | Returns true if the comparison is true for all values returned by the subquery |

## Examples

### IN and NOT IN

```sql
-- Find customers who placed orders in the last month
SELECT * FROM customers 
WHERE customer_id IN (
  SELECT customer_id 
  FROM orders 
  WHERE order_date >= CURRENT_DATE - INTERVAL '1 month'
);

-- Find products that have not been ordered
SELECT * FROM products 
WHERE product_id NOT IN (
  SELECT product_id 
  FROM order_items
);
```

### EXISTS and NOT EXISTS

```sql
-- Find customers who have at least one order
SELECT * FROM customers c 
WHERE EXISTS (
  SELECT 1 
  FROM orders o 
  WHERE o.customer_id = c.customer_id
);

-- Find customers who have no orders
SELECT * FROM customers c 
WHERE NOT EXISTS (
  SELECT 1 
  FROM orders o 
  WHERE o.customer_id = c.customer_id
);
```

### ANY and ALL

```sql
-- Find products that cost more than any product in the 'Electronics' category
SELECT * FROM products 
WHERE price > ANY (
  SELECT price 
  FROM products 
  WHERE category = 'Electronics'
);

-- Find products that cost more than all products in the 'Books' category
SELECT * FROM products 
WHERE price > ALL (
  SELECT price 
  FROM products 
  WHERE category = 'Books'
);
```

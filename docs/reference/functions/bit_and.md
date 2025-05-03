# bit_and

The `bit_and` aggregate function performs a bitwise AND operation on all non-NULL input values.

## Syntax

```sql
bit_and(expr)
```

## Arguments

- `expr`: An integer expression.

## Return Type

Returns the same type as the input expression.

## Description

The `bit_and` function performs a bitwise AND operation on all non-NULL input values. If all input values are NULL, the function returns NULL. If the input set is empty, the function returns NULL.

## Examples

```sql
-- Calculate the bitwise AND of integer values
SELECT bit_and(v) FROM (VALUES (15), (7), (3)) AS t(v);
-- Result: 3

-- NULL values are ignored
SELECT bit_and(v) FROM (VALUES (15), (7), (NULL)) AS t(v);
-- Result: 7

-- All NULL values return NULL
SELECT bit_and(v) FROM (VALUES (NULL), (NULL)) AS t(v);
-- Result: NULL

-- Empty set returns NULL
SELECT bit_and(v) FROM (VALUES (15)) AS t(v) WHERE 1=0;
-- Result: NULL
```

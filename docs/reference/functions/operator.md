---
title: Operator Functions
---

# Operator Function Reference

Operator functions are functions used to implement SQL operators. These
functions can be called directly, but it's typically more ergonomic to use the
operator syntax. See the [Arithmetic](../sql/expression/arithmetic.md),
[Comparison](../sql/expression/comparison.md), and
[Logical](../sql/expression/logical.md) operator expressions.

Functions that use reserved symbols, like `>` for "greater than", can be called
directly by double-quoting the function name. For example, to call the `>`
binary function:

```sql
SELECT ">"(8, 2); -- Returns true
```

<!-- DOCSGEN_START operator_functions -->

## `!=`

Check if two values are not equal. Returns NULL if either argument is NULL.

**Example**: `a != b`

**Output**: `false`

## `%`

Returns the remainder after dividing the left value by the right value.

**Example**: `10 % 3`

**Output**: `1`

## `*`

Multiplies two numeric values.

**Example**: `5 * 3`

**Output**: `15`

## `+`

Adds two numeric values together.

**Example**: `5 + 3`

**Output**: `8`

## `-`

Subtracts the right value from the left value.

**Example**: `10 - 4`

**Output**: `6`

## `/`

Divides the left value by the right value.

**Example**: `15 / 3`

**Output**: `5`

## `<`

Check if the left value is less than the right. Returns NULL if either argument is NULL.

**Example**: `a < b`

**Output**: `false`

## `<=`

Check if the left value is less than or equal to the right. Returns NULL if either argument is NULL.

**Example**: `a <= b`

**Output**: `false`

## `<>`

Check if two values are not equal. Returns NULL if either argument is NULL.

**Example**: `a != b`

**Output**: `false`

## `=`

Check if two values are equal. Returns NULL if either argument is NULL.

**Example**: `a = b`

**Output**: `true`

## `>`

Check if the left value is greater than the right. Returns NULL if either argument is NULL.

**Example**: `a > b`

**Output**: `false`

## `>=`

Check if the left value is greater than or equal to the right. Returns NULL if either argument is NULL.

**Example**: `a >= b`

**Output**: `false`

## `add`

Adds two numeric values together.

**Example**: `5 + 3`

**Output**: `8`

## `and`

Boolean and all inputs.

**Example**: `and(true, false, true)`

**Output**: `false`

## `div`

Divides the left value by the right value.

**Example**: `15 / 3`

**Output**: `5`

## `is_distinct_from`

Check if two values are not equal, treating NULLs as normal data values.

**Example**: `'cat' IS DISTINCT FROM NULL`

**Output**: `true`

## `is_false`

Check if a value is false.

**Example**: `is_false(false)`

**Output**: `true`

## `is_not_distinct_from`

Check if two values are equal, treating NULLs as normal data values.

**Example**: `'cat' IS NOT DISTINCT FROM NULL`

**Output**: `false`

## `is_not_false`

Check if a value is not false.

**Example**: `is_not_false(false)`

**Output**: `false`

## `is_not_null`

Check if a value is not NULL.

**Example**: `is_not_null(NULL)`

**Output**: `false`

## `is_not_true`

Check if a value is not true.

**Example**: `is_not_true(false)`

**Output**: `true`

## `is_null`

Check if a value is NULL.

**Example**: `is_null(NULL)`

**Output**: `true`

## `is_true`

Check if a value is true.

**Example**: `is_true(false)`

**Output**: `false`

## `mul`

Multiplies two numeric values.

**Example**: `5 * 3`

**Output**: `15`

## `not`

Returns TRUE if the input is FALSE, and FALSE if the input is TRUE.

**Example**: `not(TRUE)`

**Output**: `FALSE`

## `or`

Boolean or all inputs.

**Example**: `or(true, false, true)`

**Output**: `true`

## `rem`

Returns the remainder after dividing the left value by the right value.

**Example**: `10 % 3`

**Output**: `1`

## `struct_extract`

Extracts a value from a struct.

## `sub`

Subtracts the right value from the left value.

**Example**: `10 - 4`

**Output**: `6`


<!-- DOCSGEN_END -->

---
title: Operator Functions
---

# Operator Function Reference

Functions used to implement SQL operators. These functions can be called
directly, but it's typically more ergonomic to use the operator syntax. 

Expression operators documentation:

- [Arithmetic](../sql/expressions/arithmetic.md)
- [Comparison](../sql/expressions/comparison.md)
- [Logical](../sql/expressions/logical.md)

Functions that use reserved symbols, like `>` for "greater than", can be called
directly by double-quoting the function name. For example, to call the `>`
binary function:

```sql
SELECT ">"(8, 2); -- Returns true
```

## Numeric Operator Functions

<!-- DOCSGEN_START numeric_operator_functions -->

### `%`

Returns the remainder after dividing the left value by the right value.

**Example**: `10 % 3`

**Output**: `1`

### `*`

Multiplies two numeric values.

**Example**: `5 * 3`

**Output**: `15`

### `+`

Adds two numeric values together.

**Example**: `5 + 3`

**Output**: `8`

### `-`

Subtracts the right value from the left value.

**Example**: `10 - 4`

**Output**: `6`

### `/`

Divides the left value by the right value.

**Example**: `15 / 3`

**Output**: `5`

### `add`

Alias of `+`.

Adds two numeric values together.

**Example**: `5 + 3`

**Output**: `8`

### `div`

Alias of `/`.

Divides the left value by the right value.

**Example**: `15 / 3`

**Output**: `5`

### `mul`

Alias of `*`.

Multiplies two numeric values.

**Example**: `5 * 3`

**Output**: `15`

### `rem`

Alias of `%`.

Returns the remainder after dividing the left value by the right value.

**Example**: `10 % 3`

**Output**: `1`

### `sub`

Alias of `-`.

Subtracts the right value from the left value.

**Example**: `10 - 4`

**Output**: `6`


<!-- DOCSGEN_END -->

## Comparison Operator Functions

<!-- DOCSGEN_START comparison_operator_functions -->

### `!=`

Check if two values are not equal. Returns NULL if either argument is NULL.

**Example**: `a != b`

**Output**: `false`

### `<`

Check if the left value is less than the right. Returns NULL if either argument is NULL.

**Example**: `a < b`

**Output**: `false`

### `<=`

Check if the left value is less than or equal to the right. Returns NULL if either argument is NULL.

**Example**: `a <= b`

**Output**: `false`

### `<>`

Alias of `!=`.

Check if two values are not equal. Returns NULL if either argument is NULL.

**Example**: `a != b`

**Output**: `false`

### `=`

Check if two values are equal. Returns NULL if either argument is NULL.

**Example**: `a = b`

**Output**: `true`

### `>`

Check if the left value is greater than the right. Returns NULL if either argument is NULL.

**Example**: `a > b`

**Output**: `false`

### `>=`

Check if the left value is greater than or equal to the right. Returns NULL if either argument is NULL.

**Example**: `a >= b`

**Output**: `false`

### `is_distinct_from`

Check if two values are not equal, treating NULLs as normal data values.

**Example**: `'cat' IS DISTINCT FROM NULL`

**Output**: `true`

### `is_false`

Check if a value is false.

**Example**: `is_false(false)`

**Output**: `true`

### `is_not_distinct_from`

Check if two values are equal, treating NULLs as normal data values.

**Example**: `'cat' IS NOT DISTINCT FROM NULL`

**Output**: `false`

### `is_not_false`

Check if a value is not false.

**Example**: `is_not_false(false)`

**Output**: `false`

### `is_not_null`

Check if a value is not NULL.

**Example**: `is_not_null(NULL)`

**Output**: `false`

### `is_not_true`

Check if a value is not true.

**Example**: `is_not_true(false)`

**Output**: `true`

### `is_null`

Check if a value is NULL.

**Example**: `is_null(NULL)`

**Output**: `true`

### `is_true`

Check if a value is true.

**Example**: `is_true(false)`

**Output**: `false`


<!-- DOCSGEN_END -->

## Logical Operator Functions

<!-- DOCSGEN_START logical_operator_functions -->

### `and`

Boolean and all inputs.

**Example**: `and(true, false, true)`

**Output**: `false`

### `not`

Returns TRUE if the input is FALSE, and FALSE if the input is TRUE.

**Example**: `not(TRUE)`

**Output**: `FALSE`

### `or`

Boolean or all inputs.

**Example**: `or(true, false, true)`

**Output**: `true`


<!-- DOCSGEN_END -->


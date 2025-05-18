---
title: General Functions
---

# General Function Reference

<!-- DOCSGEN_START general_functions -->

## `!=`

Check if two values are not equal. Returns NULL if either argument is NULL.

**Example**: `a != b`

**Output**: `false`

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

## `and`

Boolean and all inputs.

**Example**: `and(true, false, true)`

**Output**: `false`

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

## `not`

Returns TRUE if the input is FALSE, and FALSE if the input is TRUE.

**Example**: `not(TRUE)`

**Output**: `FALSE`

## `or`

Boolean or all inputs.

**Example**: `or(true, false, true)`

**Output**: `true`

## `struct_extract`

Extracts a value from a struct.


<!-- DOCSGEN_END -->

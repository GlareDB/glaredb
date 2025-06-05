---
title: Aggregate functions
order: 0
---

# General aggregate function reference

<!-- DOCSGEN_START general_purpose_aggregate_functions -->

## `avg`

Return the average value from the input column.

## `bit_and`

Returns the bitwise AND of all non-NULL input values.

## `bit_or`

Returns the bitwise OR of all non-NULL input values.

## `bool_and`

Returns true if all non-NULL inputs are true, otherwise false.

## `bool_or`

Returns true if any non-NULL input is true, otherwise false.

## `count`

Return the count of non-NULL inputs.

## `every`

**Alias of `bool_and`**

Returns true if all non-NULL inputs are true, otherwise false.

## `first`

Return the first non-NULL value.

## `max`

Return the maximum non-NULL value seen from input.

## `min`

Return the minimum non-NULL value seen from input.

## `string_agg`

Concatenate all non-NULL input string values using a delimiter.

## `sum`

Compute the sum of all non-NULL inputs.


<!-- DOCSGEN_END -->

## Grouping

The `GROUPING` function is a special function for determining which input
expressions are taking part in a group's aggregation.

See [GROUP BY](../query-syntax/group-by.md) for more details.

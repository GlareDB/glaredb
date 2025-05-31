---
title: Aggregate Functions
---

# Aggregate Function Reference

## General Purpose Aggregates

<!-- DOCSGEN_START general_purpose_aggregate_functions -->

### `approx_count_distinct`

Return an estimated number of distinct, non-NULL values in the input.

### `approx_quantile`

Compute the approximate quantile using a T-Digest sketch.

### `approx_unique`

Return an estimated number of distinct, non-NULL values in the input.

### `avg`

Return the average value from the input column.

### `bit_and`

Returns the bitwise AND of all non-NULL input values.

### `bit_or`

Returns the bitwise OR of all non-NULL input values.

### `bool_and`

Returns true if all non-NULL inputs are true, otherwise false.

### `bool_or`

Returns true if any non-NULL input is true, otherwise false.

### `count`

Return the count of non-NULL inputs.

### `every`

Returns true if all non-NULL inputs are true, otherwise false.

### `first`

Return the first non-NULL value.

### `max`

Return the maximum non-NULL value seen from input.

### `min`

Return the minimum non-NULL value seen from input.

### `string_agg`

Concatenate all non-NULL input string values using a delimiter.

### `sum`

Compute the sum of all non-NULL inputs.


<!-- DOCSGEN_END -->

## Statistical Aggregates

Aggregate functions typically used for statistics. `NULL` inputs are ignored.

<!-- DOCSGEN_START statistics_aggregate_functions -->

### `corr`

Return the (Pearson) population correlation coefficient.

### `covar_pop`

Compute population covariance.

### `covar_samp`

Compute sample covariance.

### `regr_avgx`

Compute the average of the independent variable ('x').

### `regr_avgy`

Compute the average of the dependent variable ('y').

### `regr_count`

Compute the count where both inputs are not NULL.

### `regr_r2`

Compute the square of the correlation coefficient.

### `regr_slope`

Compute the slope of the least-squares-fit linear equation.

### `stddev`

Compute the sample standard deviation.

### `stddev_pop`

Compute the population standard deviation.

### `stddev_samp`

Compute the sample standard deviation.

### `var_pop`

Compute the population variance.

### `var_samp`

Compute the sample variance.


<!-- DOCSGEN_END -->

## GROUPING

The `GROUPING` function is a special function for determining which input
expressions are taking part in a group's aggregation.

See [GROUP BY](../sql/query-syntax/group-by.md) for more details.

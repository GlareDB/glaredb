---
title: Approximate aggregates
order: 1
---

# Approximate aggregate function reference

Approximate aggregate functions are functions that produce approximate results,
and use fewer resources than the equivalent exact aggregation. For example,
`approx_count_distinct` can be used in place of `COUNT (DISTINCT ...)` to
significantly reduce memory usage when the number of distinct inputs is large.

<!-- DOCSGEN_START approximate_aggregate_functions -->

## `approx_count_distinct`

Return an estimated number of distinct, non-NULL values in the input. This is an
approximate version of `COUNT(DISTINCT ...)`.

Internally, this uses a HyperLogLog sketch and yields roughly a 1.6% relative
error.

## `approx_quantile`

Computes a value for the given quantile such that approximately `count(input) *
quantile` numbers are smaller than the returned value.

Internally uses a T-Digest data sketch.

## `approx_unique`

**Alias of `approx_count_distinct`**

Return an estimated number of distinct, non-NULL values in the input. This is an
approximate version of `COUNT(DISTINCT ...)`.

Internally, this uses a HyperLogLog sketch and yields roughly a 1.6% relative
error.


<!-- DOCSGEN_END -->

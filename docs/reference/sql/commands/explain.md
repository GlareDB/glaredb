---
title: EXPLAIN
---

# Explain

`EXPLAIN` statements provide information about how a query will be executed,
showing the unoptimized, optimized, and physical plans for the query.

## Syntax

```sql
EXPLAIN [VERBOSE] [(FORMAT format)] query
```

## Parameters

- `VERBOSE`: If additional details should be added to the explain.
- `format`: Format out for the explain.
- `query`: The query to explain.

## Explain output

`EXPLAIN` displays three forms of a query.

- **Unoptimized**: The logical plan with no optimizations.
- **Optimized**: The logical plan after optimizations have be applied, including
  join reordering and column pruning.
- **Physical**: The final plan to be executed.

By default, the output is displayed in a text format showing the "base" plan,
and any materializations in the plan.

## Examples

Simple EXPLAIN:

```sql
EXPLAIN SELECT * FROM cities;
```

EXPLAIN with VERBOSE option:

```sql
EXPLAIN VERBOSE SELECT * FROM cities;
```

EXPLAIN with JSON format:

```sql
EXPLAIN (FORMAT JSON) SELECT * FROM cities;
```


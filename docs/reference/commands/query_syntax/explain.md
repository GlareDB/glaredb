---
title: EXPLAIN
order: 0
---

# EXPLAIN

`EXPLAIN` statements provide information about how a query will be executed, showing the unoptimized, optimized, and physical plans that the database will use.

## Syntax

```sql
EXPLAIN [ VERBOSE ] [ ( option [ ... ] ) ] statement

where option can be:
    FORMAT { TEXT | JSON }
```

## Description

`EXPLAIN` displays the execution plan that the GlareDB planner generates for the supplied statement. The execution plan shows how the tables referenced by the statement will be scanned and how data will be processed.

The optional `VERBOSE` keyword causes the plan to include additional information, such as the output columns for each node in the plan tree.

The optional `FORMAT` option can be used to specify the output format of the plan. The default format is `TEXT`. The `JSON` format is also available for machine-readable output.

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

---
title: List functions
---

# List function reference

<!-- DOCSGEN_START list_functions -->

## `array_distance`

**Alias of `l2_distance`**

Compute the Euclidean distance between two lists. Both lists must be the same length and cannot contain NULLs.

**Example**: `l2_distance([1.0, 1.0], [2.0, 4.0])`

**Output**: `3.1622776601683795`

## `l2_distance`

Compute the Euclidean distance between two lists. Both lists must be the same length and cannot contain NULLs.

**Example**: `l2_distance([1.0, 1.0], [2.0, 4.0])`

**Output**: `3.1622776601683795`

## `list_extract`

Extract an item from the list. Uses 1-based indexing.

**Example**: `list_extract([4,5,6], 2)`

**Output**: `5`

## `unnest`

Converts a list into a table with one row per list element.


<!-- DOCSGEN_END -->

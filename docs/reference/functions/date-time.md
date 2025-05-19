---
title: Date/Time Functions
---

# Date and Time Function Reference

<!-- DOCSGEN_START date_time_functions -->

## `date_part`

Get a subfield.

**Example**: `date_part('day', DATE '2024-12-17')`

**Output**: `17.000`

## `date_trunc`

Truncates a timestamp to the specified precision.

**Example**: `date_trunc('day', TIMESTAMP '2023-03-15 13:45:30')`

**Output**: `2023-03-15 00:00:00`

## `epoch`

Converts a Unix timestamp in seconds to a timestamp.

**Example**: `epoch_s(1675209600)`

**Output**: `2023-02-01 00:00:00`

## `epoch_ms`

Converts a Unix timestamp in milliseconds to a timestamp.

**Example**: `epoch_ms(1675209600000)`

**Output**: `2023-02-01 00:00:00`

## `epoch_s`

Converts a Unix timestamp in seconds to a timestamp.

**Example**: `epoch_s(1675209600)`

**Output**: `2023-02-01 00:00:00`


<!-- DOCSGEN_END -->

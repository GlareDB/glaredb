---
title: SET and RESET
---

# SET and RESET

Set and reset configuration options on the session.

## Syntax

`SET` syntax:

```sql
SET config_option TO config_value
```

`RESET` syntax:

```sql
RESET config_option;
```

## Parameters

- `config_option`: The configuration option to change.
- `config_value`: The value to set the option to.

## Examples

Set the value of `partitions` (degree of plan parallelism) to a new value:

```sql
SET partitions TO 8;
```

`=` may be used in place of the `TO` keyword:

```sql
SET partitions TO 8;
```

Reset the value of `partitions` back to its default value.

```sql
RESET partitions;
```

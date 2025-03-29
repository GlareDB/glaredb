---
title: DROP
---

# DROP

The `DROP` statement removes database objects such as tables and schemas from the database.

## Syntax

```sql
DROP SCHEMA [ IF EXISTS ] schema_name

DROP TABLE [ IF EXISTS ] [ schema_name. ] table_name
```

Where:
- `IF EXISTS`: Prevents an error if the object does not exist
- `schema_name`: The name of the schema containing the object
- `table_name`: The name of the table to drop

## Examples

### Drop a Schema

Drop a schema if it exists:

```sql
DROP SCHEMA IF EXISTS my_schema;
```

### Drop a Table

Drop a table:

```sql
DROP TABLE my_table;
```

Drop a table in a specific schema:

```sql
DROP TABLE my_schema.my_table;
```

Drop a table only if it exists:

```sql
DROP TABLE IF EXISTS my_table;
```

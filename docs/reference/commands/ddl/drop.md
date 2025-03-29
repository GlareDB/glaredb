---
title: DROP
---

# DROP

The `DROP` statement removes database objects such as tables and schemas from the database.

## Syntax

```sql
DROP SCHEMA [ IF EXISTS ] schema_name [ CASCADE | RESTRICT ]

DROP TABLE [ IF EXISTS ] [ schema_name. ] table_name [ CASCADE | RESTRICT ]
```

Where:
- `IF EXISTS`: Prevents an error if the object does not exist
- `schema_name`: The name of the schema containing the object
- `table_name`: The name of the table to drop
- `CASCADE`: Automatically drop objects that depend on the dropped object
- `RESTRICT`: Refuse to drop objects if there are dependent objects (this is the default)

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

### Using CASCADE

Drop a table and all dependent objects:

```sql
DROP TABLE my_table CASCADE;
```

## Notes

- The `DROP SCHEMA` command removes a schema from the database. All objects contained within the schema (tables, views, etc.) are also dropped if CASCADE is specified.
- The `DROP TABLE` command removes a table from the database. Any views or other objects that depend on the table will prevent the drop unless CASCADE is specified.
- If CASCADE is not specified, the default behavior is RESTRICT, which will cause the drop operation to fail if there are any dependent objects.

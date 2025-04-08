# Catalogs

## Session

Default catalogs on session create:

- "system"
  - Contains built in functions.
  - Contains built in vies and tables (**unimplemented**)
  - Read only
- "temp"
  - Contains temp objects that last the lifetime of a session.

## Remote Execution Context

Catalogs on the "remote" side during distributed or hybrid execution.

**unimplemented**

- "system"

---

# Resolving Objects

---

# Statements

## `ALTER TABLE`

**unimplemented**

## `ALTER VIEW`

**unimplemented**

## `COPY`

**unimplemented**

## `CREATE SCHEMA`

Create a schema in the current catalog. **unimplemented**

```sql
CREATE SCHEMA my_schema;
```

Create a schema in a specified catalog.

```sql
CREATE SCHEMA temp.my_schema;
```

Create a schema if it doens't already exist.

```sql
CREATE SCHEMA IF NOT EXISTS temp.my_schema;
```

## `CREATE TABLE`

### Persistent Tables

**unimplemented**

Create a table in the current schema.

```sql
CREATE TABLE my_table (
    a INT,
    b TEXT,
)
```

### Temporary Tables

Create a temporary table.

```sql
CREATE TEMP TABLE my_table (
    a INT,
    b TEXT,
)
```

### `IF NOT EXISTS`

**unimplemented**

### `OR REPLACE`

**unimplemented**

### Column Constraints

`CHECK` and `NULL`/`NOT NULL`

**unimplemented**

### Foreign Keys

**unimplemented**

### Generated Columns

**unimplemented**

### `CREATE TABLE ... AS ...`

**unimplemented**

## `CREATE VIEW`

**unimplemented**

## `DELETE`

**unimplemented**

## `DROP SCHEMA`

Drop a schema.

```sql
DROP SCHEMA temp.my_schema
```

Drop a schema if it exists

```sql
DROP SCHEMA IF EXISTS temp.my_schema
```

## `DROP TABLE`

**unimplemented**

## `DROP VIEW`

**unimplemented**

## `INSERT`

**unimplemented**

## `SELECT`

Syntax

```sql
SELECT <select_list>
FROM <tables⟩
WHERE <condition>
GROUP BY <groups>
HAVING <group_filter<
    WINDOW <window_expression>
    QUALIFY <qualify_filter>
ORDER BY <order_expression>
LIMIT <n>
OFFSET <n>;
```



**unimplemented**

## `SET`/`RESET`

Set a variable local to the session.

```sql
SET batch_size TO 8096
```

`=` alternative to `TO`.

```sql
SET batch_size TO 8096
```

Reset a variable to its default.

```sql
RESET batch_size;
```

Reset all variables to their defaults.

```sql
RESET ALL;
```

**unimplemented**

## `UPDATE`

**unimplemented**

## `USE`

**unimplemented**

---

# Functions

---

# Variables

| Name                            |
|---------------------------------|
| debug_string_var                |
| application_name                |
| debug_error_on_nested_loop_join |
| partitions                      |
| batch_size                      |

---

# Data Types

| SQL data type               | Execution data type |
|-----------------------------|---------------------|
| `VARCHAR`, `TEXT`, `STRING` | Utf8                |
| `SMALLINT`, `INT2`          | Int16               |
| `INTEGER`, `INT`, `INT4`    | Int32               |
| `BIGINT`, `INT8`            | Int64               |
| `REAL`, `FLOAT`, `FLOAT4`   | Float32             |
| `DOUBLE`, `FLOAT8`          | Float64             |
| `BOOL`, `BOOLEAN`           | Bool                |

---

# Identifiers

Identifiers are case-insenstive unless quoted with `"`.



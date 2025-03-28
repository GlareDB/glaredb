---
title: Identifiers
---

# Identifiers

Identifiers are used when referencing database objects.

Identifier rules attempt to follow Postgres semantics where possible.

## Case-Sensitivity

Identifiers in a SQL query are case-insensitive.

All of the following queries behave exactly the same:

```sql
SELECT * FROM CITIES;
SELECT * FROM cities;
SELECT * FROM Cities;
```

Identifiers wrapped in a double quote (`"`) retain their original casing.

Both of these queries are referencing different tables:

```sql
SELECT * FROM "Cities";
SELECT * FROM cities;
```

Case-insitivity applies to all database objects, including function.

Both queries are equivalent:

```sql
SELECT abs(-1.0);
SELECT ABS(-1.0);
```

## Generated Column Names

When no explicit alias is provided for an expression in the `SELECT` list, an
column name will be generated using the following rules:

- If an expression is a column reference, the column name is used.
- If an expression is a function, the function name is used.
- If none of the above rules apply, the identifier `?column?` is used.


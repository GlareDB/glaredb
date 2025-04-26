---
title: Identifiers
order: 1
---

# Identifiers

Identifiers are used when referencing database objects.

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

Case-insensitivity applies to all database objects, including functions.

Both queries are equivalent:

```sql
SELECT abs(-1.0);
SELECT ABS(-1.0);
```

When querying columns, unquoted identifiers match column names ignoring case.
For example, if we have a parquet file with columns `EmployeeName` and
`Department`, all of the below queries behave the same:

```sql
SELECT EmployeeName, Department FROM './employees.parquet';
SELECT employeename, department FROM './employees.parquet';
SELECT "EmployeeName", "Department" FROM './employees.parquet';
```

Note that quoting a column name forces case sensitive matching. Using the same
parquet example, the below query will error:

```sql
-- Error: Missing column for reference: employeename
SELECT "employeename", "department" FROM './employees.parquet';
```

## Generated Column Names

When no explicit alias is provided for an expression in the `SELECT` list, an
column name will be generated using the following rules:

- If an expression is a column reference, the column name is used.
- If an expression is a function, the function name is used.
- If none of the above rules apply, the identifier `?column?` is used.


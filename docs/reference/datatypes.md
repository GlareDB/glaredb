---
title: Data Types
---

# Data Types

## Scalar Data Types

| SQL Alias                      | Internal Type        | Description                                     |
|--------------------------------|----------------------|-------------------------------------------------|
| `TINYINT`, `INT1`              | Int8                 | Signed 8-bit (1-byte) integer                   |
| `SMALLINT`, `INT2`             | Int16                | Signed 16-bit (2-byte) integer                  |
| `INT`, `INTEGER`, `INT4`       | Int32                | Signed 32-bit (4-byte) integer                  |
| `BIGINT`, `INT8`               | Int64                | Signed 64-bit (8-byte) integer                  |
|                                | UInt8                | Unsigned 8-bit (1-byte) integer                 |
|                                | UInt16               | Unsigned 16-bit (2-byte) integer                |
|                                | UInt32               | Unsigned 32-bit (4-byte) integer                |
|                                | UInt64               | Unsigned 64-bit (8-byte) integer                |
| `HALF`, `FLOAT2`               | Float16              | Half precision (2-byte) floating-point number   |
| `REAL`, `FLOAT`, `FLOAT4`      | Float32              | Single precision (4-byte) floating-point number |
| `DOUBLE`, `FLOAT8`             | Float64              | Double precision (8-byte) floating-point number |
| `DECIMAL(p,s)`, `NUMERIC(p,s)` | Decimal64/Decimal128 | Fixed precision decimal                         |
| `BOOL`, `BOOLEAN`              | Boolean              | Boolean (true/false)                            |
| `DATE`                         | Date32/Date64        | Calendar date                                   |
| `TIMESTAMP`                    | Timestamp            | A date with time                                |
| `INTERVAL`                     | Interval             | A time interval                                 |
| `VARCHAR`, `TEXT`, `STRING`    | Utf8                 | A variable length utf8 string                   |

### Decimals

Decimals are defined with a precision (width) and scale, and are used to
represent fixed precision values. The precision indicates the total number of
digits the value can hold, and scale indicates the number of digits to the right
of decimal point.

When specifying `DECIMAL` in a SQL query, the default precision is 18 and
default scale is 3. Precision and scale can be used with the syntax `DECIMAL(p,
s)`.

For example, casting column `a` to a decimal with a precision of 8 and scale of
2:

```sql
SELECT a::DECIMAL(8, 2);
```

Decimal literals in SQL queries are parsed into a decimal type with the exact
precision and scale needed to hold the value.

A literal value of '5.43' will be parsed as a `DECIMAL(3, 2)`. This can be
verified with a `DESCRIBE` statement:

```sql
DESCRIBE SELECT 5.43
```


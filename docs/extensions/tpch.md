---
title: TPC-H
---

# TPC-H Data Generation

The `tpch_gen` extension allows for generating TPC-H data directly in GlareDB.
This extension is built using [tpchgen-rs](https://github.com/clflushopt/tpchgen-rs).

Both the CLI and Python bindings are compiled with this extension by default.
The WASM bindings do not ship with this extension.

## Usage

### Generating Data

This extension registers the following table functions in the `tpch_gen` schema:

- `customer`
- `lineitem`
- `nation`
- `orders`
- `part`
- `partsupp`
- `region`
- `supplier`

Each table function accepts a single float argument indicating the scale factor
to use when generating data. For consistency, all functions accept this
argument, even if a scale factor isn't needed to generate the data (e.g.
`nation`).

To generate "lineitem" data using a scale factor of 1:

```sql
SELECT * FROM tpch_gen.lineitem(1);
```

> Note: A global text pool is generated lazily on the first call to any of the
> table functions. Additional calls will reuse this text pool leading to
> slightly faster execution.

### Creating TPC-H Tables

Creating all tables for the TPC-H schema use the data generation functions:

```sql
CREATE TEMP TABLE customer AS SELECT * FROM tpch_gen.customer(1);
CREATE TEMP TABLE lineitem AS SELECT * FROM tpch_gen.lineitem(1);
CREATE TEMP TABLE nation AS SELECT * FROM tpch_gen.nation(1);
CREATE TEMP TABLE orders AS SELECT * FROM tpch_gen.orders(1);
CREATE TEMP TABLE part AS SELECT * FROM tpch_gen.part(1);
CREATE TEMP TABLE partsupp AS SELECT * FROM tpch_gen.partsupp(1);
CREATE TEMP TABLE region AS SELECT * FROM tpch_gen.region(1);
CREATE TEMP TABLE supplier AS SELECT * FROM tpch_gen.supplier(1);
```


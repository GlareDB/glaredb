---
title: TPC-H Generation Functions
---

# TPC-H Generation Functions

The `tpch_gen` extension provides table functions for generating TPC-H benchmark data directly in GlareDB. These functions are available in the `tpch_gen` schema.

## Table Functions

### `tpch_gen.customer(scale_factor)`

Generates TPC-H customer data with the specified scale factor.

**Parameters:**
- `scale_factor` (FLOAT): Controls the size of the generated dataset.

**Returns:**
- Table with the following columns:
  - `c_custkey` (INT64): Customer key
  - `c_name` (UTF8): Customer name
  - `c_address` (UTF8): Customer address
  - `c_nationkey` (INT32): Foreign key to nation
  - `c_phone` (UTF8): Customer phone number
  - `c_acctbal` (DECIMAL): Customer account balance
  - `c_mktsegment` (UTF8): Market segment
  - `c_comment` (UTF8): Comment

### `tpch_gen.lineitem(scale_factor)`

Generates TPC-H lineitem data with the specified scale factor.

**Parameters:**
- `scale_factor` (FLOAT): Controls the size of the generated dataset.

**Returns:**
- Table with the following columns:
  - `l_orderkey` (INT64): Order key
  - `l_partkey` (INT64): Part key
  - `l_suppkey` (INT64): Supplier key
  - `l_linenumber` (INT32): Line number within order
  - `l_quantity` (INT64): Quantity ordered
  - `l_extendedprice` (DECIMAL): Extended price
  - `l_discount` (DECIMAL): Discount percentage
  - `l_tax` (DECIMAL): Tax percentage
  - `l_returnflag` (UTF8): Return flag
  - `l_linestatus` (UTF8): Line status
  - `l_shipdate` (DATE32): Ship date
  - `l_commitdate` (DATE32): Commit date
  - `l_receiptdate` (DATE32): Receipt date
  - `l_shipinstruct` (UTF8): Shipping instructions
  - `l_shipmode` (UTF8): Shipping mode
  - `l_comment` (UTF8): Comment

### `tpch_gen.nation(scale_factor)`

Generates TPC-H nation data with the specified scale factor.

**Parameters:**
- `scale_factor` (FLOAT): Controls the size of the generated dataset (has no effect for this function as nation data is fixed).

**Returns:**
- Table with the following columns:
  - `n_nationkey` (INT32): Nation key
  - `n_name` (UTF8): Nation name
  - `n_regionkey` (INT32): Foreign key to region
  - `n_comment` (UTF8): Comment

### `tpch_gen.orders(scale_factor)`

Generates TPC-H orders data with the specified scale factor.

**Parameters:**
- `scale_factor` (FLOAT): Controls the size of the generated dataset.

**Returns:**
- Table with the following columns:
  - `o_orderkey` (INT64): Order key
  - `o_custkey` (INT64): Customer key
  - `o_orderstatus` (UTF8): Order status
  - `o_totalprice` (DECIMAL): Total price
  - `o_orderdate` (DATE32): Order date
  - `o_orderpriority` (UTF8): Order priority
  - `o_clerk` (UTF8): Clerk
  - `o_shippriority` (INT32): Shipping priority
  - `o_comment` (UTF8): Comment

### `tpch_gen.part(scale_factor)`

Generates TPC-H part data with the specified scale factor.

**Parameters:**
- `scale_factor` (FLOAT): Controls the size of the generated dataset.

**Returns:**
- Table with the following columns:
  - `p_partkey` (INT64): Part key
  - `p_name` (UTF8): Part name
  - `p_mfgr` (UTF8): Manufacturer
  - `p_brand` (UTF8): Brand
  - `p_type` (UTF8): Type
  - `p_size` (INT32): Size
  - `p_container` (UTF8): Container
  - `p_retailprice` (DECIMAL): Retail price
  - `p_comment` (UTF8): Comment

### `tpch_gen.partsupp(scale_factor)`

Generates TPC-H part supplier data with the specified scale factor.

**Parameters:**
- `scale_factor` (FLOAT): Controls the size of the generated dataset.

**Returns:**
- Table with the following columns:
  - `ps_partkey` (INT64): Part key
  - `ps_suppkey` (INT64): Supplier key
  - `ps_availqty` (INT32): Available quantity
  - `ps_supplycost` (DECIMAL): Supply cost
  - `ps_comment` (UTF8): Comment

### `tpch_gen.region(scale_factor)`

Generates TPC-H region data with the specified scale factor.

**Parameters:**
- `scale_factor` (FLOAT): Controls the size of the generated dataset (has no effect for this function as region data is fixed).

**Returns:**
- Table with the following columns:
  - `r_regionkey` (INT32): Region key
  - `r_name` (UTF8): Region name
  - `r_comment` (UTF8): Comment

### `tpch_gen.supplier(scale_factor)`

Generates TPC-H supplier data with the specified scale factor.

**Parameters:**
- `scale_factor` (FLOAT): Controls the size of the generated dataset.

**Returns:**
- Table with the following columns:
  - `s_suppkey` (INT64): Supplier key
  - `s_name` (UTF8): Supplier name
  - `s_address` (UTF8): Supplier address
  - `s_nationkey` (INT32): Foreign key to nation
  - `s_phone` (UTF8): Supplier phone number
  - `s_acctbal` (DECIMAL): Supplier account balance
  - `s_comment` (UTF8): Comment

## Examples

Generate lineitem data with a scale factor of 1:

```sql
SELECT * FROM tpch_gen.lineitem(1);
```

Create a temporary table with nation data:

```sql
CREATE TEMP TABLE nation AS SELECT * FROM tpch_gen.nation(1);
```

Count the number of orders with a scale factor of 0.1:

```sql
SELECT COUNT(*) FROM tpch_gen.orders(0.1);
```

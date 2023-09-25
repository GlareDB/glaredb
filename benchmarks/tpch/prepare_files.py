import sys

import polars as pl

scale_fac = int(sys.argv[1])

h_nation = """n_nationkey
n_name
n_regionkey
n_comment""".split(
    "\n"
)

h_region = """r_regionkey
r_name
r_comment""".split(
    "\n"
)

h_part = """p_partkey
p_name
p_mfgr
p_brand
p_type
p_size
p_container
p_retailprice
p_comment""".split(
    "\n"
)

h_supplier = """s_suppkey
s_name
s_address
s_nationkey
s_phone
s_acctbal
s_comment""".split(
    "\n"
)

h_partsupp = """ps_partkey
ps_suppkey
ps_availqty
ps_supplycost
ps_comment""".split(
    "\n"
)

h_customer = """c_custkey
c_name
c_address
c_nationkey
c_phone
c_acctbal
c_mktsegment
c_comment""".split(
    "\n"
)

h_orders = """o_orderkey
o_custkey
o_orderstatus
o_totalprice
o_orderdate
o_orderpriority
o_clerk
o_shippriority
o_comment""".split(
    "\n"
)

h_lineitem = """l_orderkey
l_partkey
l_suppkey
l_linenumber
l_quantity
l_extendedprice
l_discount
l_tax
l_returnflag
l_linestatus
l_shipdate
l_commitdate
l_receiptdate
l_shipinstruct
l_shipmode
comments""".split(
    "\n"
)

for name in [
    "nation",
    "region",
    "part",
    "supplier",
    "partsupp",
    "customer",
    "orders",
    "lineitem",
]:
    print("process table:", name)
    df = pl.read_csv(
        f"tables_scale/{scale_fac}/{name}.tbl",
        has_header=False,
        separator="|",
        try_parse_dates=True,
        new_columns=eval(f"h_{name}"),
    )
    print(df.shape)
    df = df.with_columns([pl.col(pl.Date).cast(pl.Datetime)])
    df.write_parquet(
        f"tables_scale/{scale_fac}/{name}.parquet",
        statistics=True,
        row_group_size=1024 * 512,
    )

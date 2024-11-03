import duckdb
import datafusion
import rayexec
import pathlib
import pandas as pd
import time
import os
import matplotlib.pyplot as plt

plt.style.use("tableau-colorblind10")

# TPC-H scale factor.
sf = 10


def generate_data():
    if os.path.isdir(f"./benchmarks/data/tpch-{sf}"):
        return
    con = duckdb.connect()
    con.sql("PRAGMA disable_progress_bar;SET preserve_insertion_order=false")
    con.sql(f"CALL dbgen(sf={sf})")
    pathlib.Path(f"./benchmarks/data/tpch-{sf}").mkdir(parents=True, exist_ok=True)
    for tbl in [
        "nation",
        "region",
        "customer",
        "supplier",
        "lineitem",
        "orders",
        "partsupp",
        "part",
    ]:
        con.sql(
            f"COPY (SELECT * FROM {tbl}) TO './benchmarks/data/tpch-{sf}/{tbl}.parquet'"
        )
    con.close()


generate_data()

queries = {
    1: """
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= date '1998-09-02'
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;
    """,
    2: """
SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM
      part,
      supplier,
      partsupp,
      nation,
      region
WHERE
    p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = 15
    AND p_type LIKE '%BRASS'
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND ps_supplycost = (
        SELECT
            min(ps_supplycost)
        FROM
            partsupp,
            supplier,
            nation,
            region
        WHERE
            p_partkey = ps_partkey
            AND s_suppkey = ps_suppkey
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_name = 'EUROPE')
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
LIMIT 100;
    """,
    3: """
SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('1995-03-15' AS date)
    AND l_shipdate > CAST('1995-03-15' AS date)
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10;
    """,
    4: """
    SELECT
        o_orderpriority,
        count(*) AS order_count
    FROM
        orders
    WHERE
        o_orderdate >= CAST('1993-07-01' AS date)
        AND o_orderdate < CAST('1993-10-01' AS date)
        AND EXISTS (
            SELECT
                *
            FROM
                lineitem
            WHERE
                l_orderkey = o_orderkey
                AND l_commitdate < l_receiptdate)
    GROUP BY
        o_orderpriority
    ORDER BY
        o_orderpriority;
        """,
    5: """
    SELECT
        n_name,
        sum(l_extendedprice * (1 - l_discount)) AS revenue
    FROM
      customer,
      orders,
      lineitem,
      supplier,
      nation,
      region
    WHERE
        c_custkey = o_custkey
        AND l_orderkey = o_orderkey
        AND l_suppkey = s_suppkey
        AND c_nationkey = s_nationkey
        AND s_nationkey = n_nationkey
        AND n_regionkey = r_regionkey
        AND r_name = 'ASIA'
        AND o_orderdate >= CAST('1994-01-01' AS date)
        AND o_orderdate < CAST('1995-01-01' AS date)
    GROUP BY
        n_name
    ORDER BY
        revenue DESC;
        """,
    6: """
    SELECT
        sum(l_extendedprice * l_discount) AS revenue
    FROM
        lineitem
    WHERE
        l_shipdate >= CAST('1994-01-01' AS date)
        AND l_shipdate < CAST('1995-01-01' AS date)
        AND l_discount BETWEEN 0.05
        AND 0.07
        AND l_quantity < 24;
        """,
    7: """
    SELECT
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) AS revenue
    FROM (
        SELECT
            n1.n_name AS supp_nation,
            n2.n_name AS cust_nation,
            extract(year FROM l_shipdate) AS l_year,
            l_extendedprice * (1 - l_discount) AS volume
        FROM
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2
        WHERE
            s_suppkey = l_suppkey
            AND o_orderkey = l_orderkey
            AND c_custkey = o_custkey
            AND s_nationkey = n1.n_nationkey
            AND c_nationkey = n2.n_nationkey
            AND ((n1.n_name = 'FRANCE'
                    AND n2.n_name = 'GERMANY')
                OR (n1.n_name = 'GERMANY'
                    AND n2.n_name = 'FRANCE'))
            AND l_shipdate BETWEEN CAST('1995-01-01' AS date)
            AND CAST('1996-12-31' AS date)) AS shipping
    GROUP BY
        supp_nation,
        cust_nation,
        l_year
    ORDER BY
        supp_nation,
        cust_nation,
        l_year;
        """,
    8: """
    SELECT
        o_year,
        sum(
            CASE WHEN nation = 'BRAZIL' THEN
                volume
            ELSE
                0
            END) / sum(volume) AS mkt_share
    FROM (
        SELECT
            extract(year FROM o_orderdate) AS o_year,
            l_extendedprice * (1 - l_discount) AS volume,
            n2.n_name AS nation
        FROM
           part,
           supplier,
           lineitem,
           orders,
           customer,
           nation n1,
           nation n2,
           region
        WHERE
            p_partkey = l_partkey
            AND s_suppkey = l_suppkey
            AND l_orderkey = o_orderkey
            AND o_custkey = c_custkey
            AND c_nationkey = n1.n_nationkey
            AND n1.n_regionkey = r_regionkey
            AND r_name = 'AMERICA'
            AND s_nationkey = n2.n_nationkey
            AND o_orderdate BETWEEN CAST('1995-01-01' AS date)
            AND CAST('1996-12-31' AS date)
            AND p_type = 'ECONOMY ANODIZED STEEL') AS all_nations
    GROUP BY
        o_year
    ORDER BY
        o_year;
        """,
    9: """
SELECT
    nation,
    o_year,
    sum(amount) AS sum_profit
FROM (
    SELECT
        n_name AS nation,
        extract(year FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
    FROM
        part,
        supplier,
        lineitem,
        partsupp,
        orders,
        nation
    WHERE
        s_suppkey = l_suppkey
        AND ps_suppkey = l_suppkey
        AND ps_partkey = l_partkey
        AND p_partkey = l_partkey
        AND o_orderkey = l_orderkey
        AND s_nationkey = n_nationkey
        AND p_name LIKE '%green%') AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC;
    """,
    10: """
SELECT
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    customer,
    orders,
    lineitem,
    nation
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= CAST('1993-10-01' AS date)
    AND o_orderdate < CAST('1994-01-01' AS date)
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC
LIMIT 20;
    """,
    11: """
SELECT
    ps_partkey,
    sum(ps_supplycost * ps_availqty) AS value
FROM
    partsupp,
    supplier,
    nation
WHERE
    ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
    AND n_name = 'GERMANY'
GROUP BY
    ps_partkey
HAVING
    sum(ps_supplycost * ps_availqty) > (
        SELECT
            sum(ps_supplycost * ps_availqty) * 0.0001000000
        FROM
            partsupp,
            supplier,
            nation
        WHERE
            ps_suppkey = s_suppkey
            AND s_nationkey = n_nationkey
            AND n_name = 'GERMANY')
ORDER BY
    value DESC;
    """,
    12: """
SELECT
    l_shipmode,
    sum(
        CASE WHEN o_orderpriority = '1-URGENT'
            OR o_orderpriority = '2-HIGH' THEN
            1
        ELSE
            0
        END) AS high_line_count,
    sum(
        CASE WHEN o_orderpriority <> '1-URGENT'
            AND o_orderpriority <> '2-HIGH' THEN
            1
        ELSE
            0
        END) AS low_line_count
FROM
    lineitem,
    orders
WHERE
    o_orderkey = l_orderkey
    AND l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= CAST('1994-01-01' AS date)
    AND l_receiptdate < CAST('1995-01-01' AS date)
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode;
    """,
    14: """
SELECT
    100.00 * sum(
        CASE WHEN p_type LIKE 'PROMO%' THEN
            l_extendedprice * (1 - l_discount)
        ELSE
            0
        END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
    part,
    lineitem
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= date '1995-09-01'
    AND l_shipdate < CAST('1995-10-01' AS date);
    """,
    15: """
WITH revenue0 AS (
    SELECT
        l_suppkey AS supplier_no,
        sum(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM
        lineitem
    WHERE
        l_shipdate >= CAST('1996-01-01' AS date)
      AND l_shipdate < CAST('1996-04-01' AS date)
    GROUP BY
        supplier_no
)
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM
    supplier,
    revenue0
WHERE
    s_suppkey = supplier_no
    AND total_revenue = (
        SELECT
            max(total_revenue)
        FROM  revenue0)
ORDER BY
    s_suppkey;
    """,
    17: """
SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly
FROM
    lineitem,
    part
WHERE
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
        SELECT
            0.2 * avg(l_quantity)
        FROM
            lineitem
        WHERE
            l_partkey = p_partkey);
    """,
    18: """
SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
FROM
    customer,
    orders,
    lineitem
WHERE
    o_orderkey IN (
        SELECT
            l_orderkey
        FROM
            lineitem
        GROUP BY
            l_orderkey
        HAVING
            sum(l_quantity) > 300)
    AND c_custkey = o_custkey
    AND o_orderkey = l_orderkey
GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
ORDER BY
    o_totalprice DESC,
    o_orderdate
LIMIT 100;
    """,
    19: """
SELECT
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    lineitem,
    part
WHERE (p_partkey = l_partkey
    AND p_brand = 'Brand#12'
    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    AND l_quantity >= 1
    AND l_quantity <= 1 + 10
    AND p_size BETWEEN 1 AND 5
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON')
    OR (p_partkey = l_partkey
        AND p_brand = 'Brand#23'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= 10
        AND l_quantity <= 10 + 10
        AND p_size BETWEEN 1 AND 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON')
    OR (p_partkey = l_partkey
        AND p_brand = 'Brand#34'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= 20
        AND l_quantity <= 20 + 10
        AND p_size BETWEEN 1 AND 15
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON');
    """,
    20: """
SELECT
    s_name,
    s_address
FROM
    supplier,
    nation
WHERE
    s_suppkey IN (
        SELECT
            ps_suppkey
        FROM
            partsupp
        WHERE
            ps_partkey IN (
                SELECT
                    p_partkey
                FROM
                    part
                WHERE
                    p_name LIKE 'forest%')
                AND ps_availqty > (
                    SELECT
                        0.5 * sum(l_quantity)
                    FROM
                        lineitem
                    WHERE
                        l_partkey = ps_partkey
                        AND l_suppkey = ps_suppkey
                        AND l_shipdate >= CAST('1994-01-01' AS date)
                        AND l_shipdate < CAST('1995-01-01' AS date)))
            AND s_nationkey = n_nationkey
            AND n_name = 'CANADA'
        ORDER BY
            s_name;
    """,
    21: """
SELECT
    s_name,
    count(*) AS numwait
FROM
    supplier,
    lineitem AS l1,
    orders,
    nation
WHERE
    s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT
            *
        FROM
            lineitem AS l2
        WHERE
            l2.l_orderkey = l1.l_orderkey
            AND l2.l_suppkey <> l1.l_suppkey)
    AND NOT EXISTS (
        SELECT
            *
        FROM
            lineitem AS l3
        WHERE
            l3.l_orderkey = l1.l_orderkey
            AND l3.l_suppkey <> l1.l_suppkey
            AND l3.l_receiptdate > l3.l_commitdate)
    AND s_nationkey = n_nationkey
    AND n_name = 'SAUDI ARABIA'
GROUP BY
    s_name
ORDER BY
    numwait DESC,
    s_name
LIMIT 100;
    """,
}


def setup_rayexec(conn):
    for tbl in [
        "nation",
        "region",
        "customer",
        "supplier",
        "lineitem",
        "orders",
        "partsupp",
        "part",
    ]:
        conn.query(
            f"CREATE TEMP VIEW {tbl} AS SELECT * FROM './benchmarks/data/tpch-{sf}/{tbl}.parquet'"
        )


def setup_duckdb(conn):
    for tbl in [
        "nation",
        "region",
        "customer",
        "supplier",
        "lineitem",
        "orders",
        "partsupp",
        "part",
    ]:
        conn.query(
            f"CREATE TEMP VIEW {tbl} AS SELECT * FROM './benchmarks/data/tpch-{sf}/{tbl}.parquet'"
        )


def setup_datafusion(ctx):
    for tbl in [
        "nation",
        "region",
        "customer",
        "supplier",
        "lineitem",
        "orders",
        "partsupp",
        "part",
    ]:
        ctx.register_parquet(tbl, f"./benchmarks/data/tpch-{sf}/{tbl}.parquet")


def execute_rayexec(conn, dump_profile=False):
    df = pd.DataFrame(columns=["dur", "query"])
    for query_id, query in sorted(queries.items()):
        print("Query " + str(query_id))
        start = time.time()
        try:
            collect_profile_data = dump_profile
            table = conn.query(query, collect_profile_data)
            table.show()
            stop = time.time()
            duration = stop - start
            if dump_profile:
                table.dump_profile()
        except Exception as err:
            print(err)
        print(duration)
        row = {"dur": duration, "query": query_id}
        df = pd.concat(
            [
                df if not df.empty else None,
                pd.DataFrame(row, index=[query_id]),
            ],
            axis=0,
            ignore_index=True,
        )
    return df


def execute_datafusion(ctx):
    df = pd.DataFrame(columns=["dur", "query"])
    for query_id, query in sorted(queries.items()):
        print("Query " + str(query_id))
        start = time.time()
        try:
            print(ctx.sql(query))
            stop = time.time()
            duration = stop - start
        except Exception as er:
            print(er)
            duration = 0
        print(duration)
        row = {"dur": duration, "query": query_id}
        df = pd.concat(
            [
                df if not df.empty else None,
                pd.DataFrame(row, index=[query_id]),
            ],
            axis=0,
            ignore_index=True,
        )
    return df


def execute_duckdb(conn):
    df = pd.DataFrame(columns=["dur", "query"])
    for query_id, query in sorted(queries.items()):
        print("Query " + str(query_id))
        start = time.time()
        try:
            print(conn.sql(query))
            stop = time.time()
            duration = stop - start
        except Exception as er:
            print(er)
            duration = 0
        print(duration)
        row = {"dur": duration, "query": query_id}
        df = pd.concat(
            [
                df if not df.empty else None,
                pd.DataFrame(row, index=[query_id]),
            ],
            axis=0,
            ignore_index=True,
        )
    return df


rayexec_conn = rayexec.connect()
setup_rayexec(rayexec_conn)

datafusion_ctx = datafusion.SessionContext()
setup_datafusion(datafusion_ctx)

duckdb_conn = duckdb.connect()
setup_duckdb(duckdb_conn)

rayexec_times = execute_rayexec(rayexec_conn, False)
rayexec_conn.close()

datafusion_times = execute_datafusion(datafusion_ctx)

duckdb_times = execute_duckdb(duckdb_conn)
duckdb_conn.close()

print("RAYEXEC")
print(rayexec_times)
print("DATAFUSION")
print(datafusion_times)
print("DUCKDB")
print(duckdb_times)

rayexec_times.insert(0, "system", "rayexec (dev)")
datafusion_times.insert(0, "system", "datafusion 41.0.0")
duckdb_times.insert(0, "system", "duckdb 1.1.1")


# Combine the dataframes into a single dataframe
all_times = pd.concat([rayexec_times, datafusion_times, duckdb_times], axis=0)

# Pivot the datafram
pivot_df = all_times.pivot(index="query", columns="system", values="dur")

# Plot the pivoted DataFrame
ax = pivot_df.plot(
    kind="bar", rot=0, ylabel="Duration in Seconds (Lower is Better)", figsize=(18, 5)
)

from psutil import *

vCPU = str(cpu_count()) + " vCPU"
mem = round(virtual_memory().total / (1024 * 1024 * 1024), 0)
runtime = (
    "TPC-H SF="
    + str(sf)
    + " (Parquet) "
    + " Macbook Air M2 "
    + vCPU
    + " "
    + str(mem)
    + "GB"
)


# Customize the plot
plt.title(runtime)
plt.tight_layout()

# Save the figure
plt.savefig("times.png", dpi=300)
